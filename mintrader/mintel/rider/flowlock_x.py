"""FLOWLOCK X - the Rapid Momentum Rider's stop manager.

RULE 1, NEVER WIDEN: a long's stop only ever rises, a short's only ever
falls. It is enforced in ONE place, :func:`never_widen`, which every
candidate stop passes through - in :meth:`FlowLockX.update` and again in
:meth:`FlowLockX.commit` (the engine commits only once the broker has taken
the stop). A stop is also only ever proposed on the correct side of the
market with a gap (``min_gap``): a stop that would sit at or through the
price is not sent - the trade is closed instead, because the level it
wanted to protect is already gone.

No fixed take-profit. Exits are the stop, THESIS DEAD, momentum dying, or a
protective level being reached.

States (the label says which mechanism holds the stop):

    INITIAL         the structural stop from entry; the thesis is tested hard
    PROVING         some favourable progress: the loss is cut, never strangled
    COSTS_COVERED   true NET breakeven: entry + commission + slippage buffer
                    (the spread is already inside it: a long is bought at the
                    ask and its stop fills on the bid)
    RUNNER          strong velocity, fresh extremes, high efficiency: MORE room
    PROFIT_RATCHET  an MFE-based floor from a giveback curve (a parameter, so
                    research can compare curves): early winner more room,
                    mature winner protect more, decelerating protect hard
    SWING_TRAIL     the last confirmed micro swing
    ATR_CHANDELIER  the best price less k ATR
    MOMENTUM_DECAY  MOMENTUM_DECAY_SCORE 0..1 tightens the trail as it rises
    EXHAUSTION      stretched far from the averages and slowing: protect hard
    THESIS_DEAD     exit now

HYBRID STOP: the candidates are structure (swing), ATR / chandelier, the
MFE floor, PSAR (optional - off by default, kept for research), the fast
EMA and the decay-tightened chandelier. Each state picks the right one; all
of them pass NEVER WIDEN.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Optional

INITIAL = "INITIAL"
PROVING = "PROVING"
COSTS_COVERED = "COSTS_COVERED"
RUNNER = "RUNNER"
PROFIT_RATCHET = "PROFIT_RATCHET"
SWING_TRAIL = "SWING_TRAIL"
ATR_CHANDELIER = "ATR_CHANDELIER"
MOMENTUM_DECAY = "MOMENTUM_DECAY"
EXHAUSTION = "EXHAUSTION"
THESIS_DEAD = "THESIS_DEAD"
STATES = (INITIAL, PROVING, COSTS_COVERED, RUNNER, PROFIT_RATCHET, SWING_TRAIL, ATR_CHANDELIER,
          MOMENTUM_DECAY, EXHAUSTION, THESIS_DEAD)

PLAIN = {
    INITIAL: "starting stop in place",
    PROVING: "making progress - loss cut",
    COSTS_COVERED: "costs covered - can no longer lose",
    RUNNER: "running strongly - given room",
    PROFIT_RATCHET: "profit locked in",
    SWING_TRAIL: "stop following the last swing",
    ATR_CHANDELIER: "stop following the price",
    MOMENTUM_DECAY: "momentum fading - stop tightened",
    EXHAUSTION: "stretched and tiring - protecting hard",
    THESIS_DEAD: "the move failed - out",
}


def never_widen(side: int, current: float, candidate: Optional[float]) -> float:
    """THE one place RULE 1 lives. A long (side +1) keeps the higher of the
    two, a short (side -1) the lower. A missing candidate changes nothing."""
    if candidate is None or candidate != candidate:
        return current
    if side > 0:
        return candidate if candidate > current else current
    return candidate if candidate < current else current


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


@dataclass
class GivebackCurve:
    """How much of the MFE the profit floor may give back."""
    name: str = "balanced"
    early: float = 0.55          # while the winner is young (MFE <= early_r R)
    mature: float = 0.30         # once it is mature (MFE >= mature_r R)
    decelerating: float = 0.18   # as momentum decays (blended in by the decay score)
    early_r: float = 1.0
    mature_r: float = 3.0

    def giveback(self, mfe_r: float, decay: float) -> float:
        if mfe_r <= self.early_r:
            g = self.early
        elif mfe_r >= self.mature_r:
            g = self.mature
        else:
            w = (mfe_r - self.early_r) / (self.mature_r - self.early_r)
            g = self.early + (self.mature - self.early) * w
        tight = min(g, self.decelerating)
        return g + (tight - g) * _clip(decay)


CURVES = {
    "balanced": GivebackCurve("balanced"),
    "loose": GivebackCurve("loose", 0.70, 0.45, 0.30),
    "tight": GivebackCurve("tight", 0.45, 0.22, 0.12),
    "flat_half": GivebackCurve("flat_half", 0.50, 0.50, 0.50),
    "steep": GivebackCurve("steep", 0.65, 0.15, 0.10, 0.8, 2.0),
}


@dataclass
class FlowLockParams:
    proving_r: float = 0.4               # MFE (in R) that starts PROVING
    proving_keep_risk: float = 0.6       # PROVING cuts the loss to this fraction of R ...
    proving_room_atr: float = 1.0        # ... but never closer than this to the price
    costs_r: float = 0.8                 # MFE (R) needed for COSTS_COVERED ...
    costs_mult: float = 2.0              # ... and at least this many times the costs
    ratchet_r: float = 1.2               # MFE (R) at which the profit floor starts
    curve: str = "balanced"
    swing_buffer_atr: float = 0.15
    chandelier_atr: float = 2.0
    runner_chandelier_atr: float = 2.8   # a stronger move deserves MORE room
    decay_min_chandelier_atr: float = 0.7
    runner_min_velocity: float = 1.5     # z (ATR-normalised velocity) in the trade's direction
    runner_min_efficiency: float = 0.45
    runner_extreme_age_s: float = 20.0
    runner_max_pullback_atr: float = 1.0 # a runner's pullbacks stay shallow
    decay_on: float = 0.5
    decay_exit: float = 0.85
    extreme_stale_seconds: float = 60.0
    extreme_grace_seconds: float = 5.0
    exhaustion_extension_atr: float = 4.5
    exhaustion_min_decay: float = 0.35
    exhaustion_keep: float = 0.75        # EXHAUSTION keeps at least this share of the MFE
    exhaustion_chandelier_atr: float = 0.6
    use_psar: bool = False
    psar_step: float = 0.02
    psar_max: float = 0.2
    psar_bar_seconds: float = 10.0
    ema_buffer_atr: float = 0.2
    min_gap_spreads: float = 1.0         # never closer to the price than this many spreads (and the broker's level)
    thesis_seconds: float = 180.0        # the failure tests apply while costs are not yet covered, this long
    failed_breakout_atr: float = 0.35
    opposing_velocity: float = 2.0       # z against the trade
    adverse_r: float = 0.7
    no_progress_seconds: float = 150.0
    no_progress_mfe_r: float = 0.3

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "FlowLockParams":
        p = cls()
        if not d:
            return p
        names = {f.name: f for f in fields(cls)}
        for k, v in d.items():
            if k not in names:
                continue
            cur = getattr(p, k)
            try:
                if isinstance(cur, bool):
                    if isinstance(v, bool):
                        setattr(p, k, v)
                elif isinstance(cur, str):
                    if str(v) in CURVES:
                        setattr(p, k, str(v))
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    setattr(p, k, float(v))
            except Exception:
                pass
        return p


@dataclass
class FlowInputs:
    """What the market looks like now. Only now/bid/ask are required; the
    rest default to "no information" so FlowLock X is usable on a bare
    price path (the property test) as well as with the full feature set."""
    now: float
    bid: float
    ask: float
    atr: float = 0.0
    swing_low: Optional[float] = None    # last confirmed micro swing low (protects longs)
    swing_high: Optional[float] = None   # last confirmed micro swing high (protects shorts)
    velocity: float = 0.0                # signed, ATR-normalised (z)
    accel: float = 0.0                   # signed, ATR per minute
    efficiency: float = 0.5
    efficiency_dir: int = 0
    adx_change: float = 0.0
    ema_fast: Optional[float] = None
    ema_slope: float = 0.0               # signed, ATR per bar
    extension_atr: float = 0.0           # signed (price - slow average) / ATR
    opposing_candles: float = 0.0        # 0..1
    cross_confirm: Optional[bool] = None
    tick_rate_ratio: float = 1.0
    failed_breakout: bool = False        # price back through the level it broke
    min_stop_distance: float = 0.0       # the broker's stops level, price


@dataclass
class FlowState:
    side: int
    entry: float
    initial_stop: float
    stop: float
    risk: float                          # R: |entry - initial stop|
    cost_price: float                    # commission (round trip) + slippage buffer, price
    opened_at: float
    trigger_level: float = 0.0
    state: str = INITIAL
    best: float = 0.0
    worst: float = 0.0
    best_at: float = 0.0
    costs_covered: bool = False
    visited: list = field(default_factory=list)
    peak_velocity: float = 0.5
    entry_efficiency: float = 0.5
    decay: float = 0.0
    stop_moves: int = 0
    last_update: float = 0.0
    # stop moves sent to the broker (RiderCore.stop_move_due): when the last
    # one was sent, and when the last ACCEPTED one was sent
    last_attempt_at: float = 0.0
    last_modify_at: float = 0.0
    # PSAR on psar_bar_seconds bars of the closing price
    psar: Optional[float] = None
    psar_ep: float = 0.0
    psar_af: float = 0.0
    psar_bar_start: float = 0.0
    psar_bar_hi: float = 0.0
    psar_bar_lo: float = 0.0

    def __post_init__(self):
        if not self.best:
            self.best = self.entry
        if not self.worst:
            self.worst = self.entry
        if not self.best_at:
            self.best_at = self.opened_at
        if not self.visited:
            self.visited = [self.state]

    @property
    def mfe(self) -> float:
        return max(0.0, (self.best - self.entry) * self.side)

    @property
    def mae(self) -> float:
        return max(0.0, (self.entry - self.worst) * self.side)

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in (
            "side", "entry", "initial_stop", "stop", "risk", "cost_price", "opened_at", "trigger_level", "state",
            "best", "worst", "best_at", "costs_covered", "visited", "peak_velocity", "entry_efficiency", "decay",
            "stop_moves", "last_update", "last_attempt_at", "last_modify_at", "psar", "psar_ep", "psar_af",
            "psar_bar_start", "psar_bar_hi", "psar_bar_lo")}

    @classmethod
    def from_dict(cls, d: dict) -> "FlowState":
        names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in names})


@dataclass
class FlowDecision:
    state: str
    new_stop: Optional[float] = None     # already through NEVER WIDEN and on the right side, or None
    exit: bool = False
    exit_reason: str = ""
    why: str = ""
    decay: float = 0.0
    candidates: dict = field(default_factory=dict)


class FlowLockX:
    def __init__(self, params: Optional[FlowLockParams] = None):
        self.p = params or FlowLockParams()
        self.curve = CURVES.get(self.p.curve, CURVES["balanced"])

    # ------------------------------------------------------------- start --
    def start(self, side: int, entry: float, stop: float, cost_price: float, now: float,
              trigger_level: float = 0.0, entry_efficiency: float = 0.5, entry_velocity: float = 0.5) -> FlowState:
        if side not in (1, -1):
            raise ValueError("side must be +1 or -1")
        if (entry - stop) * side <= 0:
            raise ValueError("the stop must be on the losing side of the entry")
        st = FlowState(side, float(entry), float(stop), float(stop), abs(entry - stop), max(0.0, float(cost_price)),
                       float(now), float(trigger_level or 0.0))
        st.entry_efficiency = max(0.2, float(entry_efficiency))
        st.peak_velocity = max(0.5, abs(float(entry_velocity)))
        st.last_update = float(now)
        return st

    # -------------------------------------------------------------- core --
    def decay_score(self, st: FlowState, x: FlowInputs) -> float:
        """MOMENTUM_DECAY_SCORE 0..1 from: falling velocity, deceleration,
        time since a new extreme, opposing candles and wicks, efficiency and
        ADX deteriorating, the EMA flattening, cross-market confirmation
        vanishing and tick activity slowing."""
        s = st.side
        p = self.p
        vel_dir = x.velocity * s
        vel_fade = _clip(1.0 - vel_dir / max(st.peak_velocity, 0.5))
        decel = _clip(-x.accel * s / 2.0)
        stale = _clip((x.now - st.best_at - p.extreme_grace_seconds) / max(p.extreme_stale_seconds, 1.0))
        opposing = _clip(x.opposing_candles)
        if x.efficiency_dir == -s and x.efficiency > 0.3:
            eff_drop = 1.0
        else:
            eff_drop = _clip((st.entry_efficiency - x.efficiency) / max(st.entry_efficiency, 0.2))
        adx_fall = _clip(-x.adx_change / 3.0)
        ema_flat = _clip(1.0 - x.ema_slope * s / 0.15) if x.ema_fast is not None else 0.5
        cross_lost = 1.0 if x.cross_confirm is False else 0.0
        tick_slow = _clip(1.0 - x.tick_rate_ratio)
        score = _clip(0.22 * vel_fade + 0.12 * decel + 0.18 * stale + 0.08 * opposing + 0.12 * eff_drop
                      + 0.06 * adx_fall + 0.08 * ema_flat + 0.06 * cross_lost + 0.08 * tick_slow)
        return max(score, 0.9) if x.failed_breakout else score

    def _psar_step(self, st: FlowState, x: FlowInputs, mark: float) -> None:
        p = self.p
        if st.psar_bar_start == 0.0:
            st.psar_bar_start, st.psar_bar_hi, st.psar_bar_lo = x.now, mark, mark
            return
        st.psar_bar_hi = max(st.psar_bar_hi, mark)
        st.psar_bar_lo = min(st.psar_bar_lo, mark)
        if x.now - st.psar_bar_start < p.psar_bar_seconds:
            return
        hi, lo = st.psar_bar_hi, st.psar_bar_lo
        s = st.side
        if st.psar is None:
            st.psar = lo if s > 0 else hi
            st.psar_ep = hi if s > 0 else lo
            st.psar_af = p.psar_step
        else:
            st.psar = st.psar + st.psar_af * (st.psar_ep - st.psar)
            if s > 0:
                st.psar = min(st.psar, lo)
                if hi > st.psar_ep:
                    st.psar_ep, st.psar_af = hi, min(p.psar_max, st.psar_af + p.psar_step)
            else:
                st.psar = max(st.psar, hi)
                if lo < st.psar_ep:
                    st.psar_ep, st.psar_af = lo, min(p.psar_max, st.psar_af + p.psar_step)
        st.psar_bar_start, st.psar_bar_hi, st.psar_bar_lo = x.now, mark, mark

    def _visit(self, st: FlowState, state: str) -> None:
        st.state = state
        if not st.visited or st.visited[-1] != state:
            st.visited.append(state)

    def update(self, st: FlowState, x: FlowInputs) -> FlowDecision:
        """One look at the market. Mutates the tracking fields of ``st``
        (best/worst, state, decay) but NEVER its stop: the caller commits a
        proposed stop with :meth:`commit` once the broker has accepted it."""
        p = self.p
        s = st.side
        mark = x.bid if s > 0 else x.ask                 # the price the position would close at
        st.last_update = x.now
        # stop touched: the broker's stop takes it (LIVE); PAPER fills at the stop or the gapped price,
        # less the slippage (RiderEngine._paper_stop_fill, the same as the replay's CostModel.stop_fill)
        if (s > 0 and x.bid <= st.stop) or (s < 0 and x.ask >= st.stop):
            return FlowDecision(st.state, None, True, "STOP", "the stop was reached", st.decay)
        if (mark - st.best) * s > 0:
            st.best, st.best_at = mark, x.now
        if (mark - st.worst) * s < 0:
            st.worst = mark
        vel_dir = x.velocity * s
        if vel_dir > st.peak_velocity:
            st.peak_velocity = vel_dir
        if p.use_psar:
            self._psar_step(st, x, mark)
        R = max(st.risk, 1e-12)
        atr = x.atr if x.atr > 0 else R
        mfe = st.mfe
        mfe_r = mfe / R
        fav = (mark - st.entry) * s
        decay = self.decay_score(st, x)
        st.decay = decay
        age = x.now - st.opened_at
        # ---------------------------------------------------- THESIS DEAD --
        if not st.costs_covered and age <= p.thesis_seconds:
            dead = ""
            if st.trigger_level and (st.trigger_level - mark) * s > p.failed_breakout_atr * atr and age > 3.0:
                dead = "the breakout failed - price fell back through the level it broke"
            elif -vel_dir >= p.opposing_velocity and fav < 0:
                dead = "price turned hard against the trade"
            elif -fav >= p.adverse_r * R and vel_dir < 0:
                dead = "price went against the trade and kept going"
            elif age >= p.no_progress_seconds and mfe_r < p.no_progress_mfe_r and fav < 0:
                dead = "it never got going"
            if dead:
                self._visit(st, THESIS_DEAD)
                return FlowDecision(THESIS_DEAD, None, True, "THESIS_DEAD", dead, decay)
        # ------------------------------------------------------- phases --
        cands: dict[str, float] = {}
        gap = max(x.min_stop_distance, p.min_gap_spreads * max(x.ask - x.bid, 0.0), 1e-12)
        if not st.costs_covered and mfe_r >= p.costs_r and mfe >= p.costs_mult * st.cost_price:
            st.costs_covered = True
        if st.costs_covered and decay >= p.decay_exit:
            self._visit(st, MOMENTUM_DECAY)
            return FlowDecision(MOMENTUM_DECAY, None, True, "MOMENTUM_DIED",
                                "the momentum died - banked the move", decay)
        if not st.costs_covered:
            if mfe_r >= p.proving_r:
                keep = st.entry - s * p.proving_keep_risk * R
                room = mark - s * p.proving_room_atr * atr
                cands["proving"] = min(keep, room) if s > 0 else max(keep, room)
                label = PROVING
            else:
                label = INITIAL
            chosen = cands.get("proving")
        else:
            costs = st.entry + s * st.cost_price
            cands["costs"] = costs
            ext = x.extension_atr * s
            g_curve = self.curve.giveback(mfe_r, decay)
            if mfe_r >= p.ratchet_r:
                cands["mfe_floor"] = st.best - s * g_curve * mfe
            if x.swing_low is not None and s > 0:
                cands["swing"] = x.swing_low - p.swing_buffer_atr * atr
            if x.swing_high is not None and s < 0:
                cands["swing"] = x.swing_high + p.swing_buffer_atr * atr
            if p.use_psar and st.psar is not None:
                cands["psar"] = st.psar
            runner = (vel_dir >= p.runner_min_velocity and x.efficiency >= p.runner_min_efficiency
                      and x.efficiency_dir in (0, s) and x.now - st.best_at <= p.runner_extreme_age_s
                      and (st.best - mark) * s <= p.runner_max_pullback_atr * atr
                      and x.cross_confirm is not False)
            if ext >= p.exhaustion_extension_atr and decay >= p.exhaustion_min_decay:
                label = EXHAUSTION
                cands["exhaustion_floor"] = st.best - s * (1.0 - p.exhaustion_keep) * mfe
                cands["exhaustion_chandelier"] = st.best - s * p.exhaustion_chandelier_atr * atr
                pick = ("exhaustion_floor", "exhaustion_chandelier", "costs")
                chosen = self._tightest(s, cands, pick)
            elif decay >= p.decay_on:
                label = MOMENTUM_DECAY
                w = _clip((decay - p.decay_on) / max(1.0 - p.decay_on, 1e-9))
                k = p.chandelier_atr + (p.decay_min_chandelier_atr - p.chandelier_atr) * w
                cands["decay_chandelier"] = st.best - s * k * atr
                if x.ema_fast is not None:
                    cands["ema"] = x.ema_fast - s * p.ema_buffer_atr * atr
                pick = ("decay_chandelier", "mfe_floor", "ema", "swing", "costs")
                chosen = self._tightest(s, cands, pick)
            elif runner:
                label = RUNNER
                cands["runner_chandelier"] = st.best - s * p.runner_chandelier_atr * atr
                loose = st.best - s * self.curve.early * mfe
                cands["runner_floor"] = loose
                trail = min(cands["runner_chandelier"], loose) if s > 0 else max(cands["runner_chandelier"], loose)
                chosen = max(trail, costs) if s > 0 else min(trail, costs)
            else:
                cands["chandelier"] = st.best - s * p.chandelier_atr * atr
                names = {"costs": COSTS_COVERED, "mfe_floor": PROFIT_RATCHET, "swing": SWING_TRAIL,
                         "chandelier": ATR_CHANDELIER, "psar": ATR_CHANDELIER}
                best_name, chosen = "costs", costs
                for nm in ("costs", "chandelier", "swing", "psar", "mfe_floor"):
                    c = cands.get(nm)
                    if c is not None and (c - chosen) * s > 0:
                        best_name, chosen = nm, c
                label = names[best_name]
        self._visit(st, label)
        if chosen is None:
            return FlowDecision(label, None, False, "", PLAIN[label], decay, cands)
        proposed = never_widen(s, st.stop, chosen)
        if proposed == st.stop:
            return FlowDecision(label, None, False, "", PLAIN[label], decay, cands)
        # a stop must sit on the right side of the market with a gap
        if (s > 0 and proposed >= x.bid - gap) or (s < 0 and proposed <= x.ask + gap):
            if label in (PROVING,):
                limit = x.bid - gap if s > 0 else x.ask + gap
                proposed = never_widen(s, st.stop, limit)
                if proposed == st.stop or (s > 0 and proposed >= x.bid) or (s < 0 and proposed <= x.ask):
                    return FlowDecision(label, None, False, "", PLAIN[label], decay, cands)
            else:
                return FlowDecision(label, None, True, "PROTECT_LEVEL",
                                    f"price came back to the protected level ({PLAIN[label]})", decay, cands)
        return FlowDecision(label, proposed, False, "", PLAIN[label], decay, cands)

    @staticmethod
    def _tightest(s: int, cands: dict, names: tuple) -> Optional[float]:
        out = None
        for nm in names:
            c = cands.get(nm)
            if c is None:
                continue
            if out is None or (c - out) * s > 0:
                out = c
        return out

    def commit(self, st: FlowState, stop: Optional[float]) -> bool:
        """Record a stop the broker has accepted. NEVER WIDEN again, here too."""
        new = never_widen(st.side, st.stop, stop)
        if new != st.stop:
            st.stop = new
            st.stop_moves += 1
            return True
        return False
