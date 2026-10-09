"""The MT5 bridge: the EXECUTION layer of Financial Ian (MODE A).

Takes a :class:`~mintel.ian.signal.Signal` (already in the spot pair's
direction) and, only if every check passes, places it through the shared
executor (``make_executor(mode, broker, 990911, "IAN")``) with the
protective stop attached to the order itself:

1. tradability - the symbol exists, its contract is complete and trading is
   allowed (and, LIVE, the account allows trading);
2. quote freshness - the spot quote is no older than ``max_quote_age_s``;
3. spread - no wider than ``max_spread_pips`` and ``max_spread_typical_mult``
   x the symbol's typical spread;
4. spot confirmation - over the last ``confirm_window_s`` the spot price has
   not moved against the signal by more than ``max_against_pips``, nor already
   run more than ``max_chase_fraction`` of the stop distance with it;
5. the stop - on the losing side and outside the broker's minimum distance;
6. size - ``risk_money`` at the stop, never above ``max_lots``; the smallest
   lot only if it risks no more than ``max_risk_money``. Never a size increase
   after a loss: the size is a fixed amount of money at the stop.

IDEMPOTENCY. The checks have no side effects. Then the signal id is CLAIMED
in the persisted dedupe store (an atomic insert in data/ian.sqlite) and only a
successful claim is sent. A second submit of the same id - a duplicate, a
retry after a timeout, the same signal after a restart - finds the claim and
never sends: if the first send's outcome was unknown the broker's positions
are searched for the order (it carries the id in its comment) and it is
adopted if found; otherwise nothing is sent and the report says so. An
outcome is UNKNOWN when the send raised (the connection failed mid-send) AND
when the broker's answer is "no connection" (10031) or "timed out" (10012):
the MT5 adapter reports a lost reply that way, without raising, and the
terminal may already have taken the order. So is a fill the executor then
reports as "closed at once" (no stop confirmed) unless the broker's own
closing deal shows the close: the position may still be open, and if the
broker's positions show it with no stop it is protected at once (its stop
placed, or it is closed). Only a real refusal, or a close the broker
confirms, is REJECTED; when that order did fill, the report carries its
ticket, entry price and volume so the engine books the round trip.

LATENCY is recorded at every hop: data received, signal, order request, MT5
receipt (when the adapter reports it), broker acceptance and fill. A hop that
cannot be measured is reported as not measured, never estimated.

CFDs ON A GENERIC INSTRUMENT (Binance's crypto books traded through the
broker's BTCUSD / ETHUSD CFDs, ``BridgeConfig.cfd``): a CFD's "pip" is one
point, so the FX pip limits are replaced by:

* the market is open at the broker: trading enabled for the symbol and a
  fresh quote (a closed market stops quoting);
* COSTS: the live spread plus the commission must not eat more than
  ``max_cost_share`` of the target move (``target_r`` x the stop distance);
  the spread is otherwise held only against its own typical size;
* spot confirmation: not moved against the signal by more than
  ``max_against_frac`` of the stop distance;
* size: ``risk_money`` at the stop from the CFD's own contract (tick value,
  volume step, minimum and maximum); the broker's minimum only when it risks
  no more than ``min_lot_risk_mult`` (1.5) x ``risk_money`` - otherwise the
  trade is skipped with a note; never more than ``max_lots``;
* margin: the order is skipped when it would take more than
  ``max_margin_share`` of the free margin (when the broker can say).

Legal behaviour only: every order is one genuine market order with intent to
hold the position; nothing is ever placed to be cancelled.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol, Sequence

from ..broker.base import OrderRequest, OrderResult, Side, Tick
from ..clock import to_utc, utcnow
from . import MAGIC, TAG
from .journal import ADOPTED, FILLED, REJECTED, UNKNOWN, Journal

class SignalExecutor(Protocol):
    """What the engine needs from an execution venue. MODE A is :class:`Mt5Bridge` (the spot pair in
    MetaTrader 5); MODE B - the future itself through a futures broker - would be another class with the
    same ``submit``, so nothing in the intelligence layer is tied to MT5."""

    def submit(self, sig, tick, now: dt.datetime, spot_history: Sequence = (), spec=None) -> "ExecutionReport": ...


FILLED_S = "FILLED"
REFUSED = "REFUSED"          # a check failed; nothing was sent (the same id may be submitted again later)
DUPLICATE = "DUPLICATE"      # this id was already sent once; nothing was sent now

# broker answers that are not a refusal: no reply came back, so the order may be at the broker
# (10031 "no connection with the trade server" - what the MT5 adapter returns when order_send() gives
# None; 10012 "request timed out")
OUTCOME_UNKNOWN_RETCODES = frozenset({10031, 10012})


@dataclass(frozen=True)
class CfdRules:
    """Costs and safety for a CFD traded on a generic instrument's signal (see the module notes)."""
    max_cost_share: float = 0.3          # spread + commission vs the target move
    target_r: float = 1.0                # the target move = target_r x the stop distance
    commission_per_lot: float = 0.0      # round turn, account currency, per 1.0 lot (0: the broker's own figure if known)
    max_against_frac: float = 0.15       # spot confirmation, as a share of the stop distance
    min_lot_risk_mult: float = 1.5       # the broker's minimum lot only if it risks no more than this x risk_money
    max_margin_share: float = 0.5        # of the free margin
    broker_label: str = ""               # "IC Markets": named in the notes


@dataclass
class BridgeConfig:
    max_quote_age_s: float = 5.0
    max_spread_pips: float = 2.5
    max_spread_typical_mult: float = 3.0
    confirm_window_s: float = 15.0
    max_against_pips: float = 1.0
    max_chase_fraction: float = 0.5
    risk_money: float = 10.0
    max_risk_money: float = 20.0
    max_lots: float = 0.5
    paper_slippage_points: float = 1.0
    cfd: dict = field(default_factory=dict)      # root (BTCUSDT) -> CfdRules: that instrument trades a CFD


@dataclass
class ExecutionReport:
    signal_id: str
    status: str
    ok: bool
    message: str
    ticket: int = 0
    price: float = 0.0
    volume: float = 0.0
    stop: float = 0.0
    mode: str = ""
    risk_money: float = 0.0
    latency: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"signal_id": self.signal_id, "status": self.status, "ok": self.ok, "message": self.message,
                "ticket": self.ticket, "price": self.price, "volume": self.volume, "stop": self.stop, "mode": self.mode,
                "risk_money": round(self.risk_money, 2), "latency": dict(self.latency)}


class TimedBroker:
    """A transparent wrapper that timestamps every order the executor sends: the moment the request
    leaves us and the moment the broker's answer comes back. Everything else passes straight through."""

    def __init__(self, broker, wall: Callable[[], dt.datetime] = utcnow):
        self._broker = broker
        self._wall = wall
        self.last_request: Optional[dt.datetime] = None
        self.last_answer: Optional[dt.datetime] = None
        self.last_result: Optional[OrderResult] = None

    def send(self, req: OrderRequest) -> OrderResult:
        self.last_request = self._wall()
        self.last_answer = None
        self.last_result = None
        res = self._broker.send(req)
        self.last_answer = self._wall()
        self.last_result = res
        return res

    def __getattr__(self, name):
        return getattr(self._broker, name)


def comment_for(signal_id: str) -> str:
    return f"{TAG} {signal_id[:10]}"


def _iso(t: Optional[dt.datetime]) -> Optional[str]:
    return t.isoformat() if t is not None else None


def _ms(a: Optional[dt.datetime], b: Optional[dt.datetime]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round((b - a).total_seconds() * 1000.0, 1)


class Mt5Bridge:
    def __init__(self, executor, journal: Journal, broker=None, cfg: Optional[BridgeConfig] = None,
                 spec_fn: Optional[Callable[[str], object]] = None, timed: Optional[TimedBroker] = None,
                 magic: int = MAGIC):
        self.executor = executor
        self.journal = journal
        self.broker = broker
        self.cfg = cfg or BridgeConfig()
        self.spec_fn = spec_fn or (lambda s: broker.spec(s) if broker is not None else None)
        self.timed = timed
        self.magic = int(magic)

    @property
    def mode(self) -> str:
        return getattr(self.executor, "mode", "OFF") if self.executor is not None else "OFF"

    # -------------------------------------------------------------- checks --
    def rules_for(self, sig) -> Optional[CfdRules]:
        r = (self.cfg.cfd or {}).get(str(getattr(sig, "root", "") or ""))
        return r if isinstance(r, CfdRules) else None

    def check(self, sig, tick: Optional[Tick], spec, now: dt.datetime,
              spot_history: Sequence[Tick] = ()) -> tuple[bool, str]:
        """All the pre-trade checks. No side effects."""
        c = self.cfg
        rules = self.rules_for(sig)
        if rules is not None:
            return self._check_cfd(sig, tick, spec, now, spot_history, rules)
        if self.executor is None:
            return False, "mode is OFF: nothing is ever sent"
        if spec is None:
            return False, f"{sig.spot_symbol}: no contract specification from the broker"
        if not spec.is_tradable():
            return False, f"{sig.spot_symbol}: not tradable ({', '.join(spec.missing_fields()) or 'trading disabled'})"
        if self.mode == "LIVE" and self.broker is not None:
            try:
                acc = self.broker.account()
                if not acc.trade_allowed:
                    return False, "the account does not allow trading right now"
            except Exception as exc:
                return False, f"cannot read the account ({exc})"
        if tick is None or tick.bid <= 0 or tick.ask <= 0 or tick.ask < tick.bid:
            return False, f"{sig.spot_symbol}: no valid quote"
        age = (to_utc(now) - to_utc(tick.time)).total_seconds()
        if age > c.max_quote_age_s:
            return False, f"{sig.spot_symbol}: the quote is {age:.1f} s old (over {c.max_quote_age_s:g} s)"
        pip = float(spec.pip_size)
        spread_pips = (tick.ask - tick.bid) / pip if pip else 0.0
        typical = float(getattr(spec, "typical_spread_points", 0.0) or 0.0) * float(spec.point) / pip if pip else 0.0
        if spread_pips > c.max_spread_pips:
            return False, f"{sig.spot_symbol}: spread {spread_pips:.1f} pips, over {c.max_spread_pips:g}"
        if typical > 0 and spread_pips > c.max_spread_typical_mult * typical:
            return False, (f"{sig.spot_symbol}: spread {spread_pips:.1f} pips is {spread_pips / typical:.1f}x its "
                           f"typical {typical:.1f}")
        side = sig.side
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        if (touch - sig.intended_stop) * side.sign <= 0:
            return False, f"{sig.spot_symbol}: the stop {sig.intended_stop} is not on the losing side of {touch}"
        try:
            min_d = spec.min_stop_distance_price(tick.ask - tick.bid)
        except Exception:
            min_d = 0.0
        if abs(touch - sig.intended_stop) < min_d - 1e-12:
            return False, f"{sig.spot_symbol}: the stop is inside the broker's minimum distance"
        # spot confirmation (RETAIL quotes): not moving against the signal, not already run away
        hist = [t for t in spot_history if (to_utc(now) - to_utc(t.time)).total_seconds() <= c.confirm_window_s]
        if hist:
            moved = (tick.mid - hist[0].mid) * side.sign
            if moved < -c.max_against_pips * pip - 1e-12:
                return False, (f"{sig.spot_symbol}: the spot market does not confirm - it moved {moved / pip:+.1f} pips "
                               f"against the signal in the last {c.confirm_window_s:g} s")
            if moved > c.max_chase_fraction * sig.stop_distance + 1e-12:
                return False, (f"{sig.spot_symbol}: the spot has already run {moved / pip:+.1f} pips the signal's way; "
                               "not chasing it")
        return True, ""

    def _check_cfd(self, sig, tick: Optional[Tick], spec, now: dt.datetime, spot_history: Sequence[Tick],
                   rules: CfdRules) -> tuple[bool, str]:
        """The checks for a CFD on a generic instrument (see the module notes). No side effects."""
        c = self.cfg
        sym = sig.spot_symbol
        who = f"{rules.broker_label} {sym}".strip()
        if self.executor is None:
            return False, "mode is OFF: nothing is ever sent"
        if spec is None:
            return False, f"{who}: the broker has no such CFD (no contract specification)"
        if not spec.is_tradable():
            missing = spec.missing_fields()
            return False, (f"{who}: not tradable - " + (f"its contract is incomplete ({', '.join(missing)})" if missing
                           else "trading is disabled at the broker (the market is closed or the symbol is close-only)"))
        if self.mode == "LIVE" and self.broker is not None:
            try:
                acc = self.broker.account()
                if not acc.trade_allowed:
                    return False, "the account does not allow trading right now"
            except Exception as exc:
                return False, f"cannot read the account ({exc})"
        if tick is None or tick.bid <= 0 or tick.ask <= 0 or tick.ask < tick.bid:
            return False, f"{who}: no valid quote (the market may be closed at the broker)"
        age = (to_utc(now) - to_utc(tick.time)).total_seconds()
        if age > c.max_quote_age_s:
            return False, (f"{who}: the last quote is {age:.1f} s old (over {c.max_quote_age_s:g} s) - the market is "
                           "closed or not quoting at the broker")
        spread = float(tick.ask) - float(tick.bid)
        typical = float(getattr(spec, "typical_spread_points", 0.0) or 0.0) * float(spec.point)
        if typical > 0 and spread > c.max_spread_typical_mult * typical + 1e-12:
            return False, f"{who}: spread {spread:.2f} is {spread / typical:.1f}x its typical {typical:.2f}"
        side = sig.side
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        if (touch - sig.intended_stop) * side.sign <= 0:
            return False, f"{who}: the stop {sig.intended_stop} is not on the losing side of {touch}"
        try:
            min_d = spec.min_stop_distance_price(spread)
        except Exception:
            min_d = 0.0
        if abs(touch - sig.intended_stop) < min_d - 1e-12:
            return False, f"{who}: the stop is inside the broker's minimum distance"
        # costs: the spread and the commission against the move the trade is aiming for
        target = rules.target_r * float(sig.stop_distance)
        if target <= 0:
            return False, f"{who}: no stop distance to measure the costs against"
        comm, comm_src = self.commission_price(sym, spec, rules)
        share = (spread + comm) / target
        if share > rules.max_cost_share + 1e-12:
            what = f"spread {spread:.2f}" + (f" + commission {comm:.2f} ({comm_src})" if comm > 0 else "")
            return False, (f"{who}: costs too high - {what} would eat {share:.0%} of the {target:.2f} target move "
                           f"({rules.target_r:g} x the stop distance); the most allowed is {rules.max_cost_share:.0%}")
        hist = [t for t in spot_history if (to_utc(now) - to_utc(t.time)).total_seconds() <= c.confirm_window_s]
        if hist:
            moved = (tick.mid - hist[0].mid) * side.sign
            if moved < -rules.max_against_frac * sig.stop_distance - 1e-12:
                return False, (f"{who}: the CFD does not confirm - it moved {moved:+.2f} against the signal in the last "
                               f"{c.confirm_window_s:g} s")
            if moved > c.max_chase_fraction * sig.stop_distance + 1e-12:
                return False, f"{who}: the CFD has already run {moved:+.2f} the signal's way; not chasing it"
        return True, ""

    def commission_price(self, symbol: str, spec, rules: CfdRules) -> tuple[float, str]:
        """The round-turn commission of one lot as a price distance (what it costs in the same units as the
        spread), and where the figure came from. The configured figure wins; else the broker's own, when it can
        say; else none."""
        money, src = float(rules.commission_per_lot or 0.0), "configured"
        if money <= 0:
            fn = getattr(self.broker, "commission_per_side", None)
            if callable(fn):
                try:
                    money, src = 2.0 * abs(float(fn(symbol, 1.0, spec) or 0.0)), "the broker's rate"
                except Exception:
                    money = 0.0
        per_price = float(spec.tick_value) / float(spec.tick_size) if float(spec.tick_size or 0) > 0 else 0.0
        if money <= 0 or per_price <= 0:
            return 0.0, ""
        return money / per_price, src

    def size(self, spec, stop_distance: float, rules: Optional[CfdRules] = None) -> tuple[float, float, str]:
        """(volume, money at the stop, refusal reason)."""
        c = self.cfg
        per_lot = spec.money_per_lot(stop_distance)
        if per_lot <= 0:
            return 0.0, 0.0, "cannot value the stop"
        vol = spec.normalise_volume(min(c.risk_money / per_lot, c.max_lots))
        vmin = float(getattr(spec, "volume_min", 0.01) or 0.01)
        if rules is not None:
            # a CFD: risk_money at the stop; the broker's minimum only up to min_lot_risk_mult x risk_money
            cap = min(c.max_risk_money, rules.min_lot_risk_mult * c.risk_money)
            if vol < vmin:
                if vmin > c.max_lots + 1e-12:
                    return 0.0, 0.0, (f"the broker's smallest {spec.name} size ({vmin:g} lots) is over the most this bot "
                                      f"trades ({c.max_lots:g} lots): skipped")
                if per_lot * vmin <= cap + 1e-9:
                    vol = vmin
                else:
                    return 0.0, 0.0, (f"the broker's smallest {spec.name} size ({vmin:g} lots) would risk "
                                      f"{per_lot * vmin:.2f} at the stop, more than {rules.min_lot_risk_mult:g} x the "
                                      f"{c.risk_money:g} this bot risks: skipped")
            return vol, per_lot * vol, ""
        if vol < vmin:
            if per_lot * vmin <= c.max_risk_money:
                vol = vmin
            else:
                return 0.0, 0.0, f"the smallest size would risk {per_lot * vmin:.2f}, over {c.max_risk_money:g}"
        return vol, per_lot * vol, ""

    def _margin_block(self, sig, vol: float, tick: Optional[Tick], rules: CfdRules) -> str:
        """A CFD order that would take more than ``max_margin_share`` of the free margin is skipped (only when the
        broker can say what it needs and what is free)."""
        fn = getattr(self.broker, "calc_margin", None)
        if not callable(fn) or tick is None or self.broker is None:
            return ""
        try:
            price = float(tick.ask if sig.side is Side.BUY else tick.bid)
            need = fn(sig.spot_symbol, sig.side, float(vol), price)
            free = float(getattr(self.broker.account(), "margin_free", 0.0) or 0.0)
        except Exception:
            return ""
        if need is None or free <= 0:
            return ""
        if float(need) > rules.max_margin_share * free + 1e-9:
            return (f"{rules.broker_label} {sig.spot_symbol}: {vol:g} lots would need {float(need):.2f} of margin, more "
                    f"than {rules.max_margin_share:.0%} of the {free:.2f} free: skipped").strip()
        return ""

    # -------------------------------------------------------------- submit --
    def submit(self, sig, tick: Optional[Tick], now: dt.datetime, spot_history: Sequence[Tick] = (),
               spec=None) -> ExecutionReport:
        lat = dict(sig.latency or {})
        spec = spec if spec is not None else self._spec(sig.spot_symbol)
        existing = self.journal.order(sig.signal_id)
        if existing is not None:
            return self._repeat(sig, existing, now, lat, tick, spec)
        ok, why = self.check(sig, tick, spec, now, spot_history)
        if not ok:
            return ExecutionReport(sig.signal_id, REFUSED, False, why, mode=self.mode, latency=lat)
        rules = self.rules_for(sig)
        vol, risk, why = self.size(spec, sig.stop_distance, rules)
        if vol <= 0:
            return ExecutionReport(sig.signal_id, REFUSED, False, why, mode=self.mode, latency=lat)
        if rules is not None:
            why = self._margin_block(sig, vol, tick, rules)
            if why:
                return ExecutionReport(sig.signal_id, REFUSED, False, why, mode=self.mode, latency=lat)
        claimed, row = self.journal.claim(sig.signal_id, sig.spot_symbol, sig.direction, vol, sig.intended_stop,
                                          self.mode, now)
        if not claimed:
            return self._repeat(sig, row or {}, now, lat, tick, spec)
        lat["order_request"] = now.isoformat()
        if self.timed is not None:
            self.timed.last_result = None                # the answer read below is this send's, never an older one
        try:
            fill = self.executor.open(sig.spot_symbol, sig.side, vol, sig.intended_stop, 0.0, tick, spec, now,
                                      comment=comment_for(sig.signal_id))
        except Exception as exc:
            self.journal.order_update(sig.signal_id, UNKNOWN, now, message=f"send raised: {exc}")
            self._timing(lat, now, filled=False)
            return self._unknown(sig, f"the order may or may not have reached the broker ({exc}); it will never be "
                                      "sent again - the broker's positions are checked for it", 0, tick, spec, now, lat)
        self._timing(lat, now, filled=bool(fill.ok))
        if fill.ok:
            self.journal.order_update(sig.signal_id, FILLED, now, ticket=int(fill.ticket), price=float(fill.price),
                                      volume=float(fill.volume or vol), message=fill.message)
            return ExecutionReport(sig.signal_id, FILLED_S, True, fill.message, int(fill.ticket), float(fill.price),
                                   float(fill.volume or vol), float(sig.intended_stop), self.mode,
                                   spec.money_per_lot(abs(float(fill.price) - sig.intended_stop)) * float(fill.volume or vol),
                                   lat)
        msg = str(fill.message or "")
        if self._outcome_unknown(msg):
            what = msg if msg.startswith("send failed") else f"no reply from the broker ({msg})"
            self.journal.order_update(sig.signal_id, UNKNOWN, now, message=msg)
            return self._unknown(sig, f"{what}: the order may have reached the broker; it will never be sent again - "
                                      "the broker's positions are checked for it", 0, tick, spec, now, lat)
        # the broker FILLED the order and the executor then reported it "closed at once" (its stop was not
        # confirmed). It never checks that close went through: a positions read-back that came back empty (the
        # MT5 adapter's answer when the read fails) can leave the position open with its stop. Refused only
        # when the broker's own closing deal shows it closed; otherwise UNKNOWN, so the pair stays blocked and
        # the sweep adopts the position (or closes it and books it if it has no stop)
        filled = self._filled_ticket()
        if (filled or msg.startswith("closed at once")) and not (filled and self._closed_at_broker(filled)):
            # the fill's ticket, price and volume are kept: the engine books it from them once the broker's
            # closing deal shows
            res = self.timed.last_result if (filled and self.timed is not None) else None
            got = ({"ticket": filled, "price": float(getattr(res, "price", 0.0) or 0.0),
                    "volume": float(getattr(res, "filled_volume", 0.0) or vol)} if filled else {})
            self.journal.order_update(sig.signal_id, UNKNOWN, now, message=msg, **got)
            what = f"the broker filled the order (ticket {filled})" if filled else "the broker filled the order"
            return self._unknown(sig, f"{what}, then: {msg}; the broker shows no close for it, so it may still be "
                                      "open - it will never be sent again and the broker's positions are checked for it",
                                 filled, tick, spec, now, lat)
        self.journal.order_update(sig.signal_id, REJECTED, now, message=msg, **({"ticket": filled} if filled else {}))
        res = self.timed.last_result if (filled and self.timed is not None) else None
        return ExecutionReport(sig.signal_id, REJECTED, False, msg, filled,
                               float(getattr(res, "price", 0.0) or 0.0) if filled else 0.0,
                               float(getattr(res, "filled_volume", 0.0) or vol) if filled else 0.0,
                               mode=self.mode, latency=lat)

    def _unknown(self, sig, msg: str, ticket: int, tick: Optional[Tick], spec, now: dt.datetime,
                 lat: dict) -> ExecutionReport:
        """The send is on record as UNKNOWN. The broker's positions are looked at once more, now: a position
        of this order shown with no stop is protected at once - its stop placed, or it is closed (then the
        report is REJECTED and carries its ticket, entry price and volume so the engine books it) - instead
        of being left bare until the engine's next sweep."""
        found = self.find_at_broker(sig.signal_id)
        if found is not None and not float(getattr(found, "sl", 0.0) or 0.0):
            _, done = self._protect(sig, found, tick, spec, now)
            if done.startswith("closed"):
                return ExecutionReport(sig.signal_id, REJECTED, False, done, int(found.ticket),
                                       float(found.entry_price), float(found.volume), mode=self.mode, latency=lat)
            msg = f"{msg}; {done}"
        return ExecutionReport(sig.signal_id, UNKNOWN, False, msg, ticket, mode=self.mode, latency=lat)

    def _filled_ticket(self) -> int:
        """The ticket of this send when the broker's own answer was a fill (0 when not known)."""
        res = self.timed.last_result if self.timed is not None else None
        try:
            return int(res.ticket or 0) if res is not None and res.ok else 0
        except (TypeError, ValueError):
            return 0

    def _closed_at_broker(self, ticket: int) -> bool:
        """The broker's deal history holds the closing deal for this position."""
        fn = getattr(self.executor, "closed_deal", None) or getattr(self.broker, "closed_deal", None)
        try:
            return bool(fn(int(ticket))) if fn is not None else False
        except Exception:
            return False

    def _outcome_unknown(self, msg: str) -> bool:
        """A failed open whose outcome is not known: the send raised, or the broker's answer was 'no connection'
        / 'timed out' (the retcode from this send's answer, or from the executor's 'rejected (<retcode>)')."""
        if msg.startswith("send failed"):
            return True
        codes = set()
        m = re.match(r"\s*rejected \((-?\d+)\)", msg)
        if m:
            codes.add(int(m.group(1)))
        res = self.timed.last_result if self.timed is not None else None
        if res is not None:
            try:
                codes.add(int(res.retcode))
            except (TypeError, ValueError):
                pass
        return bool(codes & OUTCOME_UNKNOWN_RETCODES)

    def _spec(self, symbol: str):
        try:
            return self.spec_fn(symbol)
        except Exception:
            return None

    def _timing(self, lat: dict, now: dt.datetime, filled: bool) -> None:
        """Hops after the order request. PAPER has no broker: those hops are marked as not applicable."""
        if self.mode != "LIVE" or self.timed is None:
            lat["mt5_receipt"] = None
            lat["broker_accept"] = None
            lat["fill"] = now.isoformat() if filled else None
            lat["note"] = "PAPER: simulated fill, no broker hops"
            return
        t = self.timed
        raw = dict(getattr(t.last_result, "raw", {}) or {}) if t.last_result is not None else {}
        rec = raw.get("received_utc") or raw.get("time_received")
        lat["order_request_wall"] = _iso(t.last_request)
        lat["mt5_receipt"] = str(rec) if rec else None
        lat["broker_accept"] = _iso(t.last_answer) if filled else None
        lat["fill"] = _iso(t.last_answer) if filled else None
        lat["ms_request_to_accept"] = _ms(t.last_request, t.last_answer)
        if not rec:
            lat["note"] = "MT5 receipt not reported by this adapter (not measured)"

    def _repeat(self, sig, row: dict, now: dt.datetime, lat: dict, tick: Optional[Tick] = None,
                spec=None) -> ExecutionReport:
        """The id was claimed before. Never send again."""
        state = str(row.get("state") or "")
        if state in (UNKNOWN, "SENDING"):
            found = self.find_at_broker(sig.signal_id)
            if found is not None:
                stop, why = float(getattr(found, "sl", 0.0) or 0.0), ""
                if not stop:
                    stop, why = self._protect(sig, found, tick, spec, now)
                    if not stop:
                        return ExecutionReport(sig.signal_id, REJECTED if why.startswith("closed") else UNKNOWN, False,
                                               why, int(found.ticket), float(found.entry_price), float(found.volume),
                                               mode=self.mode, latency=lat)
                self.journal.order_update(sig.signal_id, ADOPTED, now, ticket=int(found.ticket),
                                          price=float(found.entry_price), volume=float(found.volume),
                                          message="found at the broker after an unknown send" + (f"; {why}" if why else ""))
                try:
                    risk = spec.money_per_lot(abs(float(found.entry_price) - stop)) * float(found.volume)
                except Exception:
                    risk = 0.0
                return ExecutionReport(sig.signal_id, FILLED_S, True,
                                       "the earlier send did reach the broker: found and adopted, nothing sent again"
                                       + (f"; {why}" if why else ""),
                                       int(found.ticket), float(found.entry_price), float(found.volume), stop,
                                       self.mode, risk, latency=lat)
            return ExecutionReport(sig.signal_id, DUPLICATE, False,
                                   "already sent once with an unknown outcome and not at the broker: nothing sent again",
                                   mode=self.mode, latency=lat)
        if state in (FILLED, ADOPTED):
            return ExecutionReport(sig.signal_id, DUPLICATE, False, f"already filled as ticket {row.get('ticket')}: nothing sent again",
                                   int(row.get("ticket") or 0), float(row.get("price") or 0.0),
                                   float(row.get("volume") or 0.0), float(row.get("stop") or 0.0), self.mode, latency=lat)
        return ExecutionReport(sig.signal_id, DUPLICATE, False,
                               f"already sent once ({state.lower() or 'unknown'}): nothing sent again", mode=self.mode,
                               latency=lat)

    def _protect(self, sig, found, tick: Optional[Tick], spec, now: dt.datetime) -> tuple[float, str]:
        """A position found after an unknown send holds no stop at the broker. Place the signal's stop now;
        if the broker refuses it, or the price is already through it, close the position at once.
        Returns (the stop now held, or 0.0; what was done, in plain English)."""
        want = float(sig.intended_stop)
        side = found.side
        through = tick is not None and ((float(tick.bid) <= want) if side is Side.BUY else (float(tick.ask) >= want))
        if through:
            why = f"the price is already through its stop {want}"
        else:
            try:
                ok = bool(self.executor.modify_stop(int(found.ticket), want))
            except Exception:
                ok = False
            if ok:
                return want, f"it had no stop at the broker, so its stop {want} was placed at once"
            why = f"the broker refused the stop {want} ({getattr(self.executor, 'last_error', '') or 'no reason given'})"
        px = float(found.entry_price)
        t = tick if tick is not None else Tick(found.symbol, now, px, px)
        try:
            fill = self.executor.close(int(found.ticket), t, spec, comment=f"{TAG} no-stop")
        except Exception as exc:
            fill = None
            err = str(exc)
        else:
            err = str(getattr(fill, "message", "") or "")
        if fill is not None and fill.ok:
            msg = f"closed at once: ticket {found.ticket} was found at the broker with no stop and {why}"
            self.journal.order_update(sig.signal_id, REJECTED, now, ticket=int(found.ticket), message=msg)
            return 0.0, msg
        # still open with no stop: the order stays UNKNOWN, so the next submit and the engine's sweep (which
        # closes any position of ours without a stop, and runs on the pass after an UNKNOWN) both try again
        return 0.0, (f"ticket {found.ticket} was found at the broker with no stop and {why}; the close was refused "
                     f"({err}); it will be tried again")

    def find_at_broker(self, signal_id: str):
        """A position of ours whose comment carries this signal id (LIVE only)."""
        if self.mode != "LIVE":
            return None
        want = comment_for(signal_id)
        try:
            for p in self.executor.positions():
                if str(getattr(p, "comment", "") or "").startswith(want) and int(getattr(p, "magic", 0) or 0) == self.magic:
                    return p
        except Exception:
            return None
        return None
