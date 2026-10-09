"""Position sizing and circuit breakers.

Sizing philosophy
-----------------
Aggression means *acting decisively when the evidence is strong*, not ignoring
risk.  So risk scales between two operator-owned numbers:

    base_risk_pct  <= risk <= max_risk_pct

and the position of the scale is set by **calibrated** confidence, not by a raw
score.  A 97/100 opportunity in a score band with no trading history sizes near
base risk; the same score in a band with 60 observed trades and positive
expectancy is allowed to reach the maximum.

Explicitly prohibited, and enforced by tests:

* martingale of any kind - no doubling after a loss, no averaging into a loser,
  no recovery grids, no "increase size because the last trade lost";
* exceeding ``max_risk_pct`` for any reason whatsoever;
* uncapped Kelly.  Fractional Kelly is available, needs a large sample, and is
  still clamped to the operator's ceiling.

Top-opportunity size (version 5, 9 Oct)
---------------------------------------
A trade in a segment that has EARNED it (``risk.top_segments``: an approach in
one kind of market, judged on the bot's own closed trades) may risk up to
``risk.top_risk_money`` instead of the normal ``max_risk_money`` cap, and on FX
and metals carries at most ``risk.top_max_lots``. The raw score plays no part:
on the record the highest scores earned the least. ``max_risk_pct`` is still
the ceiling, and every breaker, the correlated- and total-exposure caps and
the margin checks apply exactly as to any other trade. The segment's own
record is checked before every such trade; when it stops earning, the trade
is sized like any other.

Circuit breakers
----------------
The breakers exist as much to contain *software failure* as market loss: a
runaway loop that sends thirty orders a minute, a spread explosion, a rejection
storm.  A breaker stops new entries; it never stops managing existing risk.
"""
from __future__ import annotations

import datetime as dt
import logging
import math
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from ..broker.base import AccountInfo, Position, Side
from ..clock import to_utc, utcnow
from ..config import Config
from ..contracts import SymbolSpec, classify_fx, infer_group
from .evidence import MarketState

log = logging.getLogger("mintel.risk")

TIER_FRACTION = {"NO_TRADE": 0.0, "NORMAL": 0.0, "STRONG": 0.45,
                 "EXCEPTIONAL": 1.0}


@dataclass
class SizingResult:
    volume: float
    risk_pct: float
    risk_money: float
    stop_distance: float
    stop_pips: float
    money_per_lot_at_stop: float
    reasons: tuple[str, ...] = ()
    rejected: str = ""

    @property
    def ok(self) -> bool:
        return self.volume > 0 and not self.rejected


@dataclass
class BreakerState:
    name: str
    tripped: bool
    reason: str = ""
    until: Optional[dt.datetime] = None
    auto_resume: bool = True


@dataclass
class RiskSnapshot:
    equity: float
    balance: float
    open_risk_pct: float
    open_positions: int
    daily_pnl: float
    daily_pnl_pct: float
    peak_equity: float
    drawdown_pct: float
    margin_level: float
    breakers: tuple[BreakerState, ...] = ()

    @property
    def entries_allowed(self) -> bool:
        return not any(b.tripped for b in self.breakers)

    def blocking_reasons(self) -> tuple[str, ...]:
        return tuple(f"{b.name}: {b.reason}" for b in self.breakers if b.tripped)


# --------------------------------------------------------------- correlation --

CORRELATION_GROUPS: dict[str, tuple[str, ...]] = {
    "USD_LONG": ("USDJPY", "USDCHF", "USDCAD"),
    "USD_SHORT": ("EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"),
    "METALS": ("XAUUSD", "XAGUSD"),
    "EQUITY": ("GER40", "US500", "US30", "NAS100", "UK100", "JP225", "DE40"),
}


def exposure_key(symbol: str, side: Side) -> tuple[str, ...]:
    """Buckets a trade belongs to for correlated-exposure accounting.

    Long EURUSD and short USDJPY are the same dollar bet wearing two names; if
    they were counted separately the account could hold three times the intended
    dollar exposure while every individual position looked small.
    """
    up = symbol.upper()
    base, quote, _ = classify_fx(symbol)
    keys: list[str] = []
    if base and quote:
        long_ccy = base if side is Side.BUY else quote
        short_ccy = quote if side is Side.BUY else base
        keys.append(f"CCY_LONG:{long_ccy}")
        keys.append(f"CCY_SHORT:{short_ccy}")
    for group, members in CORRELATION_GROUPS.items():
        if any(up.startswith(m) for m in members):
            keys.append(f"{group}:{side.value}")
    if not keys:
        keys.append(f"SYMBOL:{up}:{side.value}")
    return tuple(keys)


class RiskManager:
    def __init__(self, cfg: Config, journal=None):
        self.cfg = cfg
        self.journal = journal
        self.peak_equity = 0.0
        self._day: Optional[dt.date] = None
        self._day_start_equity = 0.0
        # callable() -> today's realised P&L of the bot's trades from the
        # BROKER's records.  When set, the daily-loss breaker no longer
        # depends on an equity snapshot taken at start-up - which every
        # restart quietly reset, so on the first live day an 11% loss never
        # tripped a 3% limit.
        self.realised_today: Optional[Callable[[], float]] = None
        self._rejects: list[dt.datetime] = []
        self._orders: list[dt.datetime] = []
        self._manual_halt = ""
        self._open_risk: dict[int, tuple[str, Side, float]] = {}
        self._top_cache: dict = {}            # tactic -> (checked at, verdict, why)

    # -------------------------------------------------------------- bookkeeping
    def register_open_risk(self, ticket: int, symbol: str, side: Side,
                           risk_money: float) -> None:
        self._open_risk[ticket] = (symbol, side, risk_money)

    def forget(self, ticket: int) -> None:
        self._open_risk.pop(ticket, None)

    def note_reject(self, when: Optional[dt.datetime] = None) -> None:
        self._rejects.append(to_utc(when or utcnow()))

    def note_order(self, when: Optional[dt.datetime] = None) -> None:
        self._orders.append(to_utc(when or utcnow()))

    def halt(self, reason: str) -> None:
        self._manual_halt = reason

    def resume(self) -> None:
        self._manual_halt = ""

    def _roll_day(self, account: AccountInfo, now: dt.datetime) -> None:
        today = to_utc(now).date()
        if self._day != today:
            self._day = today
            self._day_start_equity = account.equity
        self.peak_equity = max(self.peak_equity, account.equity)

    # ----------------------------------------------------------------- breakers
    def snapshot(self, account: AccountInfo, positions: Sequence[Position],
                 now: Optional[dt.datetime] = None) -> RiskSnapshot:
        now = to_utc(now or utcnow())
        self._roll_day(account, now)
        r = self.cfg.risk
        equity = max(account.equity, 1e-9)

        open_risk_money = 0.0
        live_tickets = {p.ticket for p in positions}
        for ticket, (_, _, money) in list(self._open_risk.items()):
            if ticket in live_tickets:
                open_risk_money += money
            else:
                self._open_risk.pop(ticket, None)
        open_risk_pct = open_risk_money / equity * 100.0

        daily_pnl = account.equity - (self._day_start_equity or account.equity)
        daily_pct = daily_pnl / max(self._day_start_equity, 1e-9) * 100.0
        if self.realised_today is not None:
            try:
                realised = float(self.realised_today())
            except Exception:
                realised = None
            if realised is not None:
                floating = sum(float(getattr(p, "profit", 0.0) or 0.0) for p in positions)
                daily_pnl = realised + floating
                base = account.equity - daily_pnl
                daily_pct = daily_pnl / max(base, 1e-9) * 100.0
        dd = 0.0
        if self.peak_equity > 0:
            dd = max(0.0, (self.peak_equity - account.equity)
                     / self.peak_equity * 100.0)

        cutoff = now - dt.timedelta(minutes=10)
        self._rejects = [t for t in self._rejects if t >= cutoff]
        hour_cutoff = now - dt.timedelta(hours=1)
        self._orders = [t for t in self._orders if t >= hour_cutoff]

        breakers: list[BreakerState] = []

        def trip(name: str, cond: bool, reason: str, auto: bool = True) -> None:
            if cond:
                breakers.append(BreakerState(name, True, reason, None, auto))

        trip("MANUAL_HALT", bool(self._manual_halt), self._manual_halt, False)
        trip("DAILY_LOSS", daily_pct <= -abs(r.max_daily_loss_pct),
             f"down {abs(daily_pct):.2f}% today (limit {r.max_daily_loss_pct:.2f}%)",
             False)
        trip("MAX_DRAWDOWN", dd >= r.max_drawdown_pct,
             f"{dd:.2f}% below the equity peak (limit {r.max_drawdown_pct:.2f}%)",
             False)
        trip("TOTAL_EXPOSURE", open_risk_pct >= r.max_total_risk_pct,
             f"{open_risk_pct:.2f}% of equity already at risk "
             f"(limit {r.max_total_risk_pct:.2f}%)")
        trip("POSITION_COUNT", len(positions) >= r.max_open_positions,
             f"{len(positions)} positions open (limit {r.max_open_positions})")
        trip("MARGIN_LEVEL",
             account.margin > 0 and account.margin_level < r.min_margin_level_pct,
             f"margin level {account.margin_level:.0f}% "
             f"(minimum {r.min_margin_level_pct:.0f}%)")
        trip("REJECT_STORM", len(self._rejects) >= r.max_rejects_per_10min,
             f"{len(self._rejects)} rejected orders in 10 minutes - "
             f"something is wrong with the requests, not the market")
        trip("ORDER_RATE", len(self._orders) >= r.max_orders_per_hour,
             f"{len(self._orders)} orders in the last hour - "
             f"rate limit reached")
        trip("TRADE_PERMISSION", not account.trade_allowed,
             "the terminal or account has trading disabled", False)

        return RiskSnapshot(
            equity=account.equity, balance=account.balance,
            open_risk_pct=round(open_risk_pct, 3),
            open_positions=len(positions), daily_pnl=round(daily_pnl, 2),
            daily_pnl_pct=round(daily_pct, 3), peak_equity=self.peak_equity,
            drawdown_pct=round(dd, 3), margin_level=account.margin_level,
            breakers=tuple(breakers))

    # ------------------------------------------------------- top opportunity
    @staticmethod
    def market_kind(spec: Optional[SymbolSpec], symbol: str = "") -> str:
        """The kind of market (contracts.infer_group, as the scanner uses it)."""
        name = getattr(spec, "name", "") or symbol
        try:
            g = infer_group(name, getattr(spec, "base_currency", "") or "",
                            getattr(spec, "profit_currency", "") or "",
                            getattr(spec, "path", "") or "")
        except Exception:
            g = ""
        if (not g or g == "UNKNOWN") and spec is not None:
            g = str(getattr(spec, "asset_group", "") or g or "UNKNOWN")
        return str(g or "UNKNOWN").upper()

    def _top_segment(self, tactic: str, kind: str) -> Optional[tuple[str, str]]:
        r = self.cfg.risk
        base = str(tactic or "").upper()
        if base.endswith("_2X"):
            base = base[:-3]
        for seg in getattr(r, "top_segments", ()) or ():
            try:
                t, g = str(seg[0]).upper(), str(seg[1]).upper()
            except (TypeError, IndexError):
                continue
            if t == base and g == kind:
                return t, g
        return None

    def segment_record(self, tactic: str, kind: str) -> dict:
        """The segment's own closed trades in the journal, in the conditions
        and markets it can still trade: the newest ``top_lookback_trades``."""
        r = self.cfg.risk
        scan = getattr(self.cfg, "scan", None)
        off_regimes = {str(rg).upper() for t, rg in (getattr(scan, "disabled_tactic_regimes", ()) or ())
                       if str(t).upper() == tactic}
        excluded = {str(x).upper() for x in (getattr(getattr(self.cfg, "universe", None), "excluded_symbols", ()) or ())}
        rows = []
        for row in self.journal.segment_rows(tactic, limit=2000):
            sym = str(row.get("symbol") or "").upper()
            if sym in excluded or str(row.get("regime") or "").upper() in off_regimes:
                continue
            if self.market_kind(None, sym) != kind:
                continue
            rows.append(row)
            if len(rows) >= max(1, int(getattr(r, "top_lookback_trades", 40) or 40)):
                break
        money = [float(x.get("pnl_money") or 0.0) for x in rows]
        rs = [max(-2.0, min(6.0, float(x.get("r") or 0.0))) for x in rows]
        days: dict[str, float] = {}
        for x, m in zip(rows, money):
            d = str(x.get("closed_utc") or "")[:10]
            days[d] = days.get(d, 0.0) + m
        return {"trades": len(rows), "days": len(days),
                "up_days": sum(1 for v in days.values() if v > 0),
                "down_days": sum(1 for v in days.values() if v < 0),
                "net": round(sum(money), 2),
                "per_trade": round(sum(money) / len(money), 2) if money else 0.0,
                "mean_r": round(sum(rs) / len(rs), 3) if rs else 0.0}

    def top_opportunity(self, state: MarketState, kind: str,
                        now: Optional[dt.datetime] = None) -> tuple[bool, str]:
        """Is this a highest-opportunity trade? (yes/no, why in plain English).

        Yes only when the setup's approach and kind of market are listed in
        ``risk.top_segments`` AND that segment's own recent record still shows
        it earning: at least ``top_min_trades`` closed trades over at least
        ``top_min_days`` days, positive after costs per trade and in R, and
        more winning days than losing ones. The score is not looked at."""
        r = self.cfg.risk
        if not getattr(r, "top_size_enabled", False) or float(getattr(r, "top_risk_money", 0.0) or 0.0) <= 0:
            return False, ""
        seg = self._top_segment(state.tactic, kind)
        if seg is None:
            return False, ""
        tactic, g = seg
        label = f"{tactic.replace('_', ' ').lower()} on {g.replace('_', ' ').lower()}"
        if str(getattr(self.cfg, "aggression", "NORMAL")).upper() == "CONSERVATIVE":
            return False, f"{label} is a top segment, but the top size is off while the aggression setting is CONSERVATIVE"
        if self.journal is None or not hasattr(self.journal, "segment_rows"):
            return False, f"{label} is a top segment, but there is no record to check it against"
        now = to_utc(now or utcnow())
        cached = self._top_cache.get(seg)
        if cached is not None and (now - cached[0]).total_seconds() < 60.0:
            return cached[1], cached[2]
        try:
            rec = self.segment_record(tactic, g)
        except Exception as exc:                          # no record, no extra size
            return False, f"{label}: its record could not be read ({exc})"
        need_n = int(getattr(r, "top_min_trades", 20) or 20)
        need_d = int(getattr(r, "top_min_days", 4) or 4)
        facts = (f"{rec['trades']} trades over {rec['days']} days, {rec['net']:+.2f} after costs "
                 f"({rec['per_trade']:+.2f} a trade, {rec['mean_r']:+.2f} R), "
                 f"{rec['up_days']} days up and {rec['down_days']} down")
        if rec["trades"] < need_n or rec["days"] < need_d:
            verdict, why = False, (f"{label} is a top segment, but its record is too short for extra size: {facts} "
                                   f"(needs {need_n} trades over {need_d} days)")
        elif rec["per_trade"] <= 0 or rec["mean_r"] <= 0 or rec["up_days"] <= rec["down_days"]:
            verdict, why = False, (f"{label} is a top segment, but it is not earning now: {facts}; "
                                   f"sized like any other trade")
        else:
            verdict, why = True, f"top opportunity: {label} has earned it - {facts}"
        self._top_cache[seg] = (now, verdict, why)
        return verdict, why

    # ------------------------------------------------------------------ sizing
    def risk_pct_for(self, state: MarketState, calibrated_confidence: float,
                     kelly_stats: Optional[tuple[int, float, float]] = None
                     ) -> tuple[float, tuple[str, ...]]:
        """Choose the risk percentage for this specific opportunity.

        ``calibrated_confidence`` in [0, 1] comes from the calibrator, which
        blends the raw score with what that score band has actually delivered.
        ``kelly_stats`` is (samples, win_rate, payoff_ratio) when available.
        """
        r = self.cfg.risk
        reasons: list[str] = []
        base = r.base_risk_pct
        if state.tier == "NO_TRADE":
            return 0.0, ("opportunity does not clear the NORMAL tier",)
        # How much of the band ABOVE base risk this tier unlocks.  A NORMAL
        # opportunity trades at base risk - it is still a trade, just not one
        # worth leaning on.
        tier_headroom = TIER_FRACTION.get(state.tier, 0.0)

        # Scale within [base, max] by calibrated confidence, tier headroom and
        # the operator's aggression preference.  Aggression moves us along the
        # curve; it can never move the ceiling.
        conf = max(0.0, min(1.0, calibrated_confidence))
        aggression = self.cfg.aggression_scale()
        # A conservative setting should genuinely trade smaller, so below
        # NORMAL the *base* is also pulled back toward the configured minimum.
        # Above NORMAL only the climb toward the ceiling accelerates - the
        # ceiling itself is untouchable.
        if aggression < 1.0:
            base = r.min_risk_pct + (base - r.min_risk_pct) * aggression
        span = max(0.0, r.max_risk_pct - base)
        raw = base + span * min(1.0, conf * tier_headroom * aggression)
        reasons.append(f"tier {state.tier}, calibrated confidence {conf:.2f}, "
                       f"aggression {self.cfg.aggression}")

        if r.use_fractional_kelly and kelly_stats:
            n, win_rate, payoff = kelly_stats
            if n >= r.kelly_min_samples and payoff > 0:
                # f* = W - (1-W)/R, then a conservative fraction of it.
                f_star = win_rate - (1.0 - win_rate) / payoff
                if f_star > 0:
                    kelly_pct = f_star * r.kelly_fraction * 100.0
                    raw = min(raw, max(base, kelly_pct))
                    reasons.append(
                        f"fractional Kelly ({r.kelly_fraction:g} of f*={f_star:.3f} "
                        f"on {n} samples) caps risk at {kelly_pct:.2f}%")
                else:
                    raw = base
                    reasons.append("Kelly is negative on this sample - "
                                   "holding at base risk")
            else:
                reasons.append(f"only {n} comparable trades - too few for Kelly, "
                               f"staying on the calibrated curve")

        final = max(r.min_risk_pct, min(r.max_risk_pct, raw))
        if final >= r.max_risk_pct - 1e-9:
            reasons.append(f"clamped to your maximum of {r.max_risk_pct:.2f}%")
        return round(final, 4), tuple(reasons)

    def size(self, state: MarketState, spec: SymbolSpec, account: AccountInfo,
             positions: Sequence[Position], risk_pct: float,
             snapshot: Optional[RiskSnapshot] = None,
             margin_per_lot: Optional[float] = None, *,
             top: bool = False) -> SizingResult:
        """Convert a risk percentage into a broker-legal volume.

        Everything that turns price into money is applied: tick value, tick
        size, contract size (all inside ``spec.money_per_lot``), the broker's
        volume grid and bounds, free margin, and the correlated-exposure cap.

        ``top`` (a highest-opportunity trade, see :meth:`top_opportunity`):
        the trade aims at ``top_risk_money`` - still under ``max_risk_pct`` of
        the account - in place of the normal money cap, and outside indices
        carries at most ``top_max_lots``. Nothing else changes.
        """
        r = self.cfg.risk
        reasons: list[str] = []
        stop_distance = abs(state.entry - state.stop)
        pip = max(spec.pip_size, 1e-12)
        if stop_distance <= 0:
            return SizingResult(0, 0, 0, 0, 0, 0, rejected="stop distance is zero")

        money_cap = float(getattr(r, "max_risk_money", 0.0) or 0.0)
        top_money = float(getattr(r, "top_risk_money", 0.0) or 0.0)
        top = bool(top) and top_money > 0
        if top:
            # The top size: as much as the ceiling allows, up to top_risk_money.
            # The raw score and tier play no part (they did not earn it).
            risk_pct = r.max_risk_pct
            money_cap = top_money
            reasons.append(f"top opportunity: up to {top_money:.2f} {account.currency} "
                           f"(the normal cap is {float(getattr(r, 'max_risk_money', 0.0) or 0.0):.2f})")
        risk_pct = min(risk_pct, r.max_risk_pct)
        risk_money = account.equity * risk_pct / 100.0
        if money_cap > 0 and risk_money > money_cap:
            risk_money = money_cap
            risk_pct = money_cap / max(account.equity, 1e-9) * 100.0
            reasons.append(f"capped at {money_cap:.2f} {account.currency} a trade")

        # Correlated exposure: the cap applies to the BUCKET, not the symbol.
        if snapshot is not None:
            keys = set(exposure_key(state.symbol, state.side))
            bucket_money = 0.0
            for ticket, (sym, side, money) in self._open_risk.items():
                if keys & set(exposure_key(sym, side)):
                    bucket_money += money
            cap = account.equity * r.max_correlated_risk_pct / 100.0
            room = cap - bucket_money
            if room <= 0:
                return SizingResult(
                    0, 0, 0, stop_distance, stop_distance / pip, 0,
                    rejected=f"correlated exposure cap reached "
                             f"({bucket_money / max(account.equity,1e-9) * 100:.2f}% "
                             f"already at risk in the same bet)")
            if risk_money > room:
                risk_money = room
                reasons.append("reduced to respect the correlated exposure cap")

            total_cap = account.equity * r.max_total_risk_pct / 100.0
            total_room = total_cap - (snapshot.open_risk_pct / 100.0
                                      * account.equity)
            if total_room <= 0:
                return SizingResult(0, 0, 0, stop_distance, stop_distance / pip,
                                    0, rejected="total exposure cap reached")
            if risk_money > total_room:
                risk_money = total_room
                reasons.append("reduced to respect the total exposure cap")

        money_per_lot = spec.money_per_lot(stop_distance)
        if money_per_lot <= 0:
            return SizingResult(0, 0, 0, stop_distance, stop_distance / pip, 0,
                                rejected="cannot value this instrument's stop")

        raw_volume = risk_money / money_per_lot
        volume = spec.normalise_volume(raw_volume)
        if volume <= 0:
            return SizingResult(
                0, 0, risk_money, stop_distance, stop_distance / pip,
                money_per_lot,
                rejected=f"risk of {risk_money:.2f} {account.currency} is "
                         f"smaller than the minimum tradable size "
                         f"({spec.volume_min:g} lots costs "
                         f"{money_per_lot * spec.volume_min:.2f})")

        # Never let rounding push actual risk above the ceiling.
        actual_risk_money = money_per_lot * volume
        ceiling = account.equity * r.max_risk_pct / 100.0
        if money_cap > 0:
            ceiling = min(ceiling, money_cap)
        while actual_risk_money > ceiling + 1e-9 and volume > 0:
            volume = spec.normalise_volume(volume - spec.volume_step)
            actual_risk_money = money_per_lot * volume
            reasons.append("stepped down one lot increment to stay under your "
                           "maximum risk")
        if volume <= 0:
            return SizingResult(
                0, 0, risk_money, stop_distance, stop_distance / pip,
                money_per_lot,
                rejected="the smallest tradable size would exceed your "
                         "maximum risk per trade")
        lot_cap = float(getattr(r, "top_max_lots", 0.0) or 0.0)
        if top and lot_cap > 0 and volume > lot_cap + 1e-12 \
                and self.market_kind(spec, state.symbol) != "INDEX":
            capped = spec.normalise_volume(lot_cap)          # floored onto the broker's grid
            if capped <= 0:
                return SizingResult(
                    0, 0, risk_money, stop_distance, stop_distance / pip, money_per_lot,
                    rejected=f"the top-opportunity limit of {lot_cap:g} lots is below the "
                             f"broker's minimum of {spec.volume_min:g} lots")
            volume = capped
            actual_risk_money = money_per_lot * volume
            reasons.append(f"held to {capped:g} lots, the most a top-opportunity "
                           f"trade carries outside indices")

        # Margin feasibility, with a deliberate buffer so a fill does not put
        # the account straight onto a margin breaker.  ``margin_per_lot`` comes
        # from the broker (MT5 ``order_calc_margin``), which is the only source
        # that knows the cross-rates and margin rules involved; the fallback is
        # deliberately conservative because over-estimating margin only makes
        # the position smaller.
        if margin_per_lot is None:
            if spec.margin_initial > 0:
                margin_per_lot = spec.margin_initial
            else:
                margin_per_lot = (spec.contract_size * state.entry
                                  / max(account.leverage or 30, 1))
                reasons.append("margin estimated conservatively - the broker "
                               "did not supply a figure")
        if margin_per_lot > 0:
            # Two separate constraints, both applied:
            #   (a) the trade must fit inside free margin with a buffer;
            #   (b) the margin LEVEL after the fill must still clear the
            #       configured minimum, so stacking several correctly-sized
            #       trades cannot walk the account down to a margin call.
            cap_free = account.margin_free * 0.8
            level_min = max(r.min_margin_level_pct, 100.0) / 100.0
            cap_level = max(0.0, account.equity / level_min - account.margin)
            cap = min(cap_free, cap_level)
            if margin_per_lot * volume > cap:
                volume = spec.normalise_volume(cap / margin_per_lot)
                reasons.append(
                    "reduced so the margin level stays above your minimum of "
                    f"{r.min_margin_level_pct:.0f}%")
                if volume <= 0:
                    return SizingResult(
                        0, 0, risk_money, stop_distance, stop_distance / pip,
                        money_per_lot,
                        rejected=f"taking this trade would push the margin "
                                 f"level below {r.min_margin_level_pct:.0f}%")
                actual_risk_money = money_per_lot * volume

        return SizingResult(
            volume=volume,
            risk_pct=round(actual_risk_money / max(account.equity, 1e-9) * 100, 4),
            risk_money=round(actual_risk_money, 2),
            stop_distance=stop_distance, stop_pips=round(stop_distance / pip, 2),
            money_per_lot_at_stop=round(money_per_lot, 4),
            reasons=tuple(reasons))
