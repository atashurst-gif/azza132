"""States, moves and sizes (spec sections 6, 9, 12 and 21; Builder D).

Key states: PRACTICE, LIVE_TRIAL, LIVE(step), BANNED_REAL (practice
continues), PINNED (Aaron), PLACED (a bot or key Aaron made live, before its
own judgement) and PAUSED (a pause until a time, kept beside the state).

THE RATCHET. Moves UP (G1 trial, G2 trial to base, G3 a step, the demo
ladder's steps and its daily-stop levels, Formula 1's judgement, a floor) are
computed only by :func:`nightly` (22:20 UK) into tomorrow's plan, announced
that evening, and take effect only through :func:`release` at 06:30 UK on the
next UK trading day, unless Aaron vetoes the item with one command. During
the day :func:`tick` can only remove risk (D1, D2, D4, D5, S1, S2, K1 and the
brakes). Nothing here reads today's profit or loss or the progress against
£100 a day for anything that adds risk: the account figures reach this module
only through the brakes (which only restrict) and the headroom (which only
lowers money).

Never grow right after losses: a step needs the key at a new high of its real
record since it went live. Every real-money size decision and every demotion
of something live uses the real-only chain (P+_real, LCB80_real).

Section 21 (Aaron, 12 Oct: no limits on the demo): while the demo test passes
(config.json DEMO and the broker reporting a demo account, fresh) the ladder
continues past S4 (S5 2%, S6 2.5%, S7 3%, S8 4%, S9 5%) with half-Kelly, at
most 3 steps an ISO week, the stop-safety bootstrap run against the
effective daily stop; trials start at base (at most 4 at once, £150 a week of
trial losses); and the daily stop itself may be lifted 3 -> 4 -> 5 -> 6 ->
8 -> 10% one level a night. On a real-money account every one of these falls
back to the v1 values at once (the entries are written with v1 sizes and no
demo block). Every trade still has its broker stop; nothing here doubles
after a loss, averages into a loser, runs a grid or adds exposure because the
last trade lost.

Formula 1 is pinned until its 20-live-trade judgement (Aaron, 10 Oct): the
coach computes D1-D5, S1 and S2 for it in shadow only (shown under "Needs
you"), never changes its route, size or filters, and at the nightly after its
20th live trade applies the plan's rule (keep: governed at £13 with steps by
evidence; fail: back to practice), released at 06:30 UK.

This module imports nothing from the trading code except the pure helpers
the spec allows (engine.risk.base_risk through policy.py, verdicts' rule),
and never imports report.py or the page.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import math
import random
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from . import (BASE_STEP, BOT_BANDBREAKER, BOT_CROWD, BOT_IAN, BOT_LABEL, BOT_RUNNER, BOT_TNB, COOLOFF_DAYS,
               CONDITION_UNCLEAR, CUSUM_H, CUSUM_K, D3_STEP_DOWN_P, D3_TO_BASE_P, D3_TO_PRACTICE_MIN_NEFF,
               D3_TO_PRACTICE_P, D4_PRACTICE_SIGMA, D4_STEP_DOWN_SIGMA, D5_TRIAL_LOSS_GBP, DEMO_CORRELATED_MULT,
               DEMO_DAILY_STOP_LADDER, DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT, DEMO_DRAWDOWN_BREAKER_PCT,
               DEMO_KELLY_FRACTION, DEMO_MAX_DAILY_STOP_PCT, DEMO_MAX_STEP_PCT, DEMO_MAX_STEPS_WEEK, DEMO_MAX_TRIALS,
               DEMO_STEP_NAMES, DEMO_STOP_RAISE_LOOKBACK_DAYS, DEMO_STOP_RAISE_MAX_SHARE, DEMO_STOP_RAISE_MIN_LIVE_KEYS,
               DEMO_STOP_RAISE_MIN_P_PLUS, DEMO_TOTAL_MAX_PCT, DEMO_TOTAL_MULT, F1_KEY, G2_MIN_DAYS, G2_MIN_GAP_R,
               G2_MIN_P_PLUS_REAL, G2_MIN_TRADES, G3_MIN_DAYS_SINCE_CHANGE, G3_MIN_P_PLUS_REAL, G3_MIN_TRADES, GAP_R,
               JUDGEMENT_KEEP, JUDGEMENT_MIN_DAYS, JUDGEMENT_PRACTICE, KELLY_FRACTION, MAX_NEW_TRIALS_NIGHT,
               MAX_STEPS_NIGHT, MAX_STEPS_WEEK, MAX_TRIALS_OPEN, MODE_GUARD, MODE_OFF, MODE_PILOT, MODE_WATCH,
               PLACED_JUDGE_AFTER, ROUTE_BASELINE, ROUTE_PIN, ROUTE_PRACTICE, ROUTE_REAL, ROUTE_SKIP,
               STATE_BANNED_REAL, STATE_LIVE, STATE_LIVE_TRIAL, STATE_PAUSED, STATE_PINNED, STATE_PLACED,
               STATE_PRACTICE, STEP_DOWN_HOLD_DAYS, STEP_ORDER, STEPS_PCT, DEMO_STEPS_PCT, STOP_SAFETY_DAYS,
               STOP_SAFETY_MAX_SHARE, TRIAL_MAX_GBP, TRIAL_STEP, TZ_UK, UTC, WILD, Block, DemoLimits, Entry, Envelope,
               Floor, Key, Pin, as_utc, is_uk_trading_day, next_release_utc, parse_utc, uk_day)
from . import epochs as E
from . import estimate as ES
from . import policy as policy_mod
from . import records as R
from . import rules as RU
from .brakes import BrakeState
from .ledger import Segment
from .words import gbp, key_words, pct, r as rwords, say, short, step_words

UP, DOWN = "up", "down"
JUDGEMENT, RISK_LIMIT = "judgement", "risk limit"
GOVERNED_STATES = (STATE_LIVE_TRIAL, STATE_LIVE)
BOT_KEYS = {b: f"{b}|*|*|*" for b in (BOT_RUNNER, BOT_IAN, BOT_BANDBREAKER, BOT_CROWD)}
F1_JUMP_ITEM = "f1-jump"
TOP_V1_STEP = "S4"


# =====================================================================
# Small helpers
# =====================================================================
def _fin(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(float(x))


def add_trading_days(day: dt.date, n: int) -> dt.date:
    """The UK trading day ``n`` trading days after ``day``."""
    d, left = day, int(n)
    while left > 0:
        d += dt.timedelta(days=1)
        if is_uk_trading_day(d):
            left -= 1
    return d


def next_uk_midnight(now: dt.datetime) -> dt.datetime:
    d = uk_day(now) + dt.timedelta(days=1)
    return dt.datetime(d.year, d.month, d.day, tzinfo=TZ_UK).astimezone(UTC)


def matches(key: str, trade_key: str) -> bool:
    """Does the (possibly wildcard) segment key cover a trade's L3 key?"""
    return E.matches(key, trade_key)


def iso_week(d: dt.date) -> tuple[int, int]:
    return d.isocalendar()[:2]


def day_record(trades: Sequence) -> dict:
    """net (after costs), UK days traded, up and down days, in pounds and in R."""
    by_day: dict[dt.date, float] = {}
    for t in trades:
        by_day[t.uk_day] = by_day.get(t.uk_day, 0.0) + t.net_gbp
    net = round(sum(t.net_gbp for t in trades), 2)
    return {"trades": len(trades), "net": net, "days": len(by_day),
            "up": sum(1 for v in by_day.values() if v > 0.005), "down": sum(1 for v in by_day.values() if v < -0.005)}


# =====================================================================
# Sizes
# =====================================================================
@dataclass(frozen=True)
class Sizes:
    """The step ladder and its money (section 6.1, section 21)."""
    base_pct: float                 # Trend & Breakout's base percent (engine.risk.base_risk)
    base_money: float               # and its money cap (£13 today)
    max_risk_pct: float             # config.json's ceiling (read only)
    env: Envelope

    @classmethod
    def of(cls, cfg: Any, env: Envelope) -> "Sizes":
        from ..engine.risk import base_risk                 # the one reader of the base size
        bp, bm = base_risk(cfg.risk)
        return cls(float(bp), float(bm), float(cfg.risk.max_risk_pct), env)

    def v1_cap_pct(self) -> float:
        return min(self.max_risk_pct, float(self.env.max_step_pct))

    def demo_cap_pct(self) -> float:
        return min(float(self.env.demo_max_step_pct), DEMO_MAX_STEP_PCT)

    def ladder(self, demo: bool) -> tuple[str, ...]:
        """The steps above the trial a key may climb (base .. S4, then S5 .. S9 on the demo), each never
        above the ceiling in force."""
        out = []
        cap = self.demo_cap_pct() if demo else self.v1_cap_pct()
        for name, p in STEPS_PCT + (DEMO_STEPS_PCT if demo else ()):
            if name == TRIAL_STEP:
                continue
            p = self.base_pct if p is None else p
            if p <= cap + 1e-9:
                out.append(name)
        return tuple(out)

    def step_pct(self, step: str) -> float:
        for name, p in STEPS_PCT + DEMO_STEPS_PCT:
            if name == step:
                return self.base_pct if p is None else float(p)
        raise ValueError(f"unknown step {step!r}")

    def size(self, step: str, equity: float, demo: bool, state: str = STATE_LIVE) -> tuple[float, float]:
        """(risk_pct, money_gbp) of a step at ``equity``: the step's percent capped at the ceiling in force
        (config.json's 1.5% unless the demo test passes), the trial at 0.35% and never above £8 (at base on the
        demo), base at its money cap."""
        eq = float(equity)
        if state == STATE_LIVE_TRIAL or step == TRIAL_STEP:
            if demo:
                return self.size(BASE_STEP, eq, demo)
            p = min(float(self.env.trial_risk_pct), self.v1_cap_pct())
            money = min(eq * p / 100.0, float(self.env.trial_max_gbp), TRIAL_MAX_GBP)
            return round(p, 4), _floor2(money)
        cap = self.demo_cap_pct() if demo else self.v1_cap_pct()
        if step in DEMO_STEP_NAMES and not demo:
            step = self.highest(False)
        p = min(self.step_pct(step), cap)
        money = eq * p / 100.0
        if step == BASE_STEP and self.base_money > 0:
            money = min(money, self.base_money)
        return round(p, 4), _floor2(money)

    def v1_money(self, step: str, equity: float, state: str = STATE_LIVE) -> float:
        """Section 21's shadow: what the same trade would have risked under the v1 (real-money) rules."""
        return self.size(step, equity, False, state)[1]

    def highest(self, demo: bool) -> str:
        lad = self.ladder(demo)
        return lad[-1] if lad else BASE_STEP

    def next_step(self, step: Optional[str], demo: bool) -> Optional[str]:
        lad = self.ladder(demo)
        if step not in lad:
            return None
        i = lad.index(step)
        return lad[i + 1] if i + 1 < len(lad) else None

    def prev_step(self, step: Optional[str], demo: bool = True) -> Optional[str]:
        """One step down. On a real-money account (``demo`` False) a demo step is sized as the v1 top step,
        so one step down from it is the v1 step below that: risk really comes off."""
        order = [n for n in STEP_ORDER if n != TRIAL_STEP]
        if not demo and step in DEMO_STEP_NAMES:
            step = self.highest(False)
        if step not in order:
            return None
        i = order.index(step)
        return order[i - 1] if i > 0 else None


def _floor2(x: float) -> float:
    return math.floor(float(x) * 100.0 + 1e-9) / 100.0


# =====================================================================
# The world one decision sees
# =====================================================================
@dataclass
class World:
    """Everything a decision reads (all read only). ``mode``: the coach's effective mode (the envelope's,
    the WATCH day, the self-check and its own GUARD). ``demo``: section 21's demo test (both config.json and
    the broker say demo, fresh). ``equity``: the page's equity, used only for sizes and the brakes."""
    now: dt.datetime
    cfg: Any
    env: Envelope
    mode: str
    trades: list
    data_ok: bool = True
    unreadable_bots: tuple = ()
    fit: Any = None                       # estimate.Fit (the nightly refit, keys updated after closes)
    brakes: BrakeState = field(default_factory=BrakeState)
    demo: bool = False
    equity: Optional[float] = None
    ian_mode: str = "PAPER"
    judged: dict = field(default_factory=dict)          # key -> Judgement (released)
    stop_pct: Optional[float] = None                    # the effective daily stop (demo ladder), else config's
    data_dir: Optional[str] = None
    haircuts: dict = field(default_factory=dict)

    def __post_init__(self):
        self.now = as_utc(self.now)
        self.sizes = Sizes.of(self.cfg, self.env)
        if self.stop_pct is None:
            self.stop_pct = float(self.cfg.risk.max_daily_loss_pct)
        ctx = policy_mod.CheckContext.from_config(self.cfg, self.env, "")
        self.runner_live, self.runner_skip, self.runner_markets = ctx.runner_live, ctx.runner_skip, ctx.runner_markets
        self.pins = policy_mod.active_pins(self.env, self.judged)
        self.today = uk_day(self.now)

    # -- Aaron's static settings
    def static_live(self, bot: str) -> bool:
        if bot == BOT_IAN:
            return str(self.ian_mode).upper() == "LIVE"
        block = getattr(self.cfg, bot, None)
        if block is None or not getattr(block, "enabled", True):
            return False
        return str(getattr(block, "mode", "") or "").strip().upper() == "LIVE"

    def tnb_static_real(self, key: str) -> bool:
        """Is this Trend & Breakout key real on Aaron's own settings (tnb.live_groups, tnb.live_tactics)?"""
        bot, approach, kind, _ = (key.split("|") + [WILD] * 4)[:4]
        if bot != BOT_TNB:
            return False
        tnb = getattr(self.cfg, "tnb", None)
        groups = tuple(getattr(tnb, "live_groups", ()) or ())
        tactics = tuple(str(t).upper() for t in (getattr(tnb, "live_tactics", ()) or ()))
        mode = R._tnb_mode(self.cfg)
        if mode != "PAPER":
            return True                                 # Trend & Breakout fully LIVE on Aaron's settings
        if kind == WILD or kind not in groups:
            return False
        return not tactics or approach in tactics

    def pinned(self, key: str) -> Optional[Pin]:
        try:
            return policy_mod.pinned(Key.parse(key), self.pins)
        except ValueError:
            return None

    def forbidden(self, key: str) -> str:
        try:
            k = Key.parse(key)
        except ValueError as exc:
            return str(exc)
        return policy_mod.real_forbidden(k, self.env, self.pins, runner_live=self.runner_live,
                                         runner_skip=self.runner_skip, runner_markets=self.runner_markets)

    def est(self, key: str):
        """The key's estimate from the latest fit (a wildcard key fitted on its own when needed)."""
        if self.fit is None:
            return None
        e = self.fit.keys.get(key)
        if e is not None or WILD not in key.split("|")[1:]:
            return e
        cache = getattr(self, "_wild", None)
        if cache is None:
            cache = self._wild = {}
        if key not in cache:
            mine = [dataclasses.replace(t, condition=WILD) for t in self.trades
                    if t.bot == key.split("|")[0] and matches(key, t.key)]
            if not mine:
                cache[key] = None
            else:
                f = ES.fit(mine, self.now, taus=self.fit.taus, reestimate_tau=False, only_keys=[key],
                           pins=self.pins, haircuts=self.haircuts)
                cache[key] = f.keys.get(key)
        return cache[key]

    def key_trades(self, key: str, *, real: Optional[bool] = None, since: Optional[dt.datetime] = None) -> list:
        bot = key.split("|")[0]
        out = []
        for t in self.trades:
            if t.bot != bot or not matches(key, t.key) or t.closed_utc > self.now:
                continue
            if real is not None and t.real != real:
                continue
            if since is not None and t.opened_utc < since:
                continue
            out.append(t)
        return out


# =====================================================================
# Moves
# =====================================================================
@dataclass
class Move:
    """One change to a key (or to the account's demo stop), with its sentence and numbers."""
    key: str
    direction: str                    # up | down | note
    rule: str
    to_state: str
    to_step: Optional[str] = None
    from_state: str = ""
    from_step: Optional[str] = None
    kind: str = JUDGEMENT             # judgement | risk limit
    words: str = ""
    numbers: dict = field(default_factory=dict)
    item: str = ""                    # the plan item id (moves up)
    pause_until: str = ""             # D2 and the brakes: a pause, the state unchanged
    cooloff: bool = False
    hold: bool = False                # a step down: no step up for 5 UK trading days

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Move":
        names = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in names})

    @property
    def to_text(self) -> str:
        return f"{self.to_state}:{self.to_step}" if self.to_step else self.to_state

    @property
    def from_text(self) -> str:
        return f"{self.from_state}:{self.from_step}" if self.from_step else self.from_state


def state_text(seg: Segment) -> str:
    return f"{seg.state}:{seg.step}" if seg.step else seg.state


def apply(segs: dict, moves: Iterable[Move], now: dt.datetime) -> list[Move]:
    """Apply moves to the segments (in memory); returns the ones that changed something."""
    now = as_utc(now)
    today = uk_day(now)
    done = []
    for m in moves:
        if not m.to_state and not m.pause_until:
            continue
        if m.key.startswith("@"):
            seg = segs.get(m.key) or Segment(m.key, registered_utc=now.isoformat())
            if seg.state == m.to_state and (seg.step or None) == (m.to_step or None):
                continue
            m.from_state, m.from_step = seg.state, seg.step
            seg.state, seg.step, seg.since_utc, seg.rule = m.to_state, m.to_step, now.isoformat(), m.rule
            segs[m.key] = seg
            done.append(m)
            continue
        seg = segs.get(m.key)
        if seg is None:
            seg = Segment(m.key, registered_utc=now.isoformat())
            segs[m.key] = seg
        if m.pause_until:
            if seg.paused_until and parse_utc(seg.paused_until) and parse_utc(seg.paused_until) >= parse_utc(m.pause_until):
                continue
            m.from_state, m.from_step = seg.state, seg.step
            seg.paused_until = m.pause_until
            done.append(m)
            continue
        if seg.state == m.to_state and (seg.step or None) == (m.to_step or None) and (m.rule or "") == (seg.rule or ""):
            continue
        m.from_state, m.from_step = seg.state, seg.step
        went_live = m.to_state in GOVERNED_STATES and seg.state not in GOVERNED_STATES
        if went_live and not m.numbers.get("live_since_utc"):
            seg.live_since_utc = now.isoformat()
        elif m.numbers.get("live_since_utc"):
            seg.live_since_utc = str(m.numbers["live_since_utc"])
        seg.state, seg.step, seg.rule = m.to_state, m.to_step, m.rule
        seg.since_utc = str(m.numbers.get("since_utc") or now.isoformat())
        if m.cooloff:
            seg.cooloff_until = add_trading_days(today, COOLOFF_DAYS).isoformat()
        if m.hold:
            seg.hold_until = add_trading_days(today, STEP_DOWN_HOLD_DAYS).isoformat()
        if m.rule == "G2_FAIL":
            seg.g2_failures += 1
        if m.to_state == STATE_BANNED_REAL and m.numbers.get("epoch"):
            seg.banned_epoch = str(m.numbers["epoch"])
        if m.to_state not in GOVERNED_STATES:
            seg.trial_pnl_gbp, seg.cusum, seg.peak_r, seg.real_n = 0.0, 0.0, 0.0, 0
        done.append(m)
    return done


# =====================================================================
# Registration: which keys the coach knows, and their starting state
# =====================================================================
def sync(segs: dict, w: World) -> list[Move]:
    """New keys are registered (their forward-only evidence starts now) and Aaron's pins, placed bots and
    static live keys get their states. Nothing here grants anything."""
    now = w.now.isoformat()
    moves: list[Move] = []
    epoch_now = {}
    for p in w.env.pins:
        active = any(p.key == q.key for q in w.pins)
        seg = segs.get(p.key)
        if active:
            if seg is None:
                segs[p.key] = Segment(p.key, registered_utc=now, state=STATE_PINNED, since_utc=now, rule="PIN")
            elif seg.state != STATE_PINNED:
                moves.append(Move(p.key, "note", "PIN", STATE_PINNED,
                                  words=say("PINNED", what=key_words(p.key)), kind=RISK_LIMIT))
    for bot, key in BOT_KEYS.items():
        if bot not in w.env.govern:
            continue
        live = w.static_live(bot)
        seg = segs.get(key)
        want = STATE_PLACED if live else STATE_PRACTICE
        if seg is None:
            segs[key] = Segment(key, registered_utc=now, state=want, since_utc=now)
        elif seg.state in (STATE_PLACED, STATE_PRACTICE) and seg.state != want and not seg.rule:
            moves.append(Move(key, "note", "PLACED" if live else "PRACTICE", want,
                              words=say("PLACED", what=BOT_LABEL.get(bot, bot), n=PLACED_JUDGE_AFTER) if live else
                              say("PRACTICE_KEY", what=BOT_LABEL.get(bot, bot)), kind=RISK_LIMIT))
    seen = set()
    for t in w.trades:
        if t.bot != BOT_TNB or t.key in seen:
            continue
        seen.add(t.key)
        if t.key in segs:
            continue
        if w.pinned(t.key) is not None:
            continue                                  # under the pin's own key
        state = STATE_PLACED if w.tnb_static_real(t.key) else STATE_PRACTICE
        if t.key not in epoch_now:
            ep = E.latest_epoch(t.key, w.now)
            epoch_now[t.key] = ep.isoformat() if ep else ""
        segs[t.key] = Segment(t.key, registered_utc=now, epoch=epoch_now[t.key], state=state, since_utc=now)
    return moves


# =====================================================================
# The day: moves down only (any tick)
# =====================================================================
def _series(trades: Sequence) -> list[float]:
    return [t.x for t in sorted(trades, key=lambda t: (t.closed_utc, t.ticket))]


def cusum(xs: Sequence[float], sigma: float) -> float:
    """D1: S = max(0, S + 0.05 - x / sigma), in sigma units."""
    s = 0.0
    sig = max(1.0, float(sigma or 1.0))
    for x in xs:
        s = max(0.0, s + CUSUM_K - x / sig)
    return s


def drawdown_r(xs: Sequence[float]) -> tuple[float, float, float]:
    """(peak, last, drawdown) of the cumulative R."""
    cum = peak = 0.0
    for x in xs:
        cum += x
        peak = max(peak, cum)
    return peak, cum, peak - cum


def _sigma(w: World, key: str) -> float:
    e = w.est(key)
    return max(1.0, float(getattr(e, "sigma_real", 1.0) or 1.0)) if e is not None else 1.0


def shadow_checks(w: World, key: str, since: Optional[dt.datetime]) -> list[str]:
    """D1-D5 for a pinned key, computed but never applied: what each would do, in words ("Needs you")."""
    real = w.key_trades(key, real=True, since=since)
    out = []
    if not real:
        return out
    xs = _series(real)
    sig = _sigma(w, key)
    days = len({t.uk_day for t in real})
    s = cusum(xs, sig)
    if s >= CUSUM_H and days >= JUDGEMENT_MIN_DAYS:
        out.append(f"the CUSUM check ({s:.1f}) would send {key_words(key)} back to practice: your call")
    peak, last, dd = drawdown_r(xs)
    if dd >= D4_PRACTICE_SIGMA * sig:
        out.append(f"its real record is {dd:.1f} R below its best point: the drawdown rule would send it to "
                   f"practice: your call")
    elif dd >= D4_STEP_DOWN_SIGMA * sig:
        out.append(f"its real record is {dd:.1f} R below its best point: the drawdown rule would step it down: "
                   f"your call")
    for t in real:
        if t.uk_day == w.today and t.x <= GAP_R:
            out.append(f"a real trade lost {t.x:.2f} R today: the gap rule would pause it for the day: your call")
            break
    return out


def pinned_rules(w: World, key: str, s1_results: Optional[Sequence] = None,
                 s2_results: Optional[Sequence] = None) -> list[str]:
    """Aaron's standing rules S1 and S2 on a pinned key, computed but never applied (section 12): what each
    would do, in words for "Needs you"."""
    out: list[str] = []
    _, approach, _, cond = key.split("|")
    mine = w.key_trades(key)
    if not mine:
        return out
    conds = {t.condition for t in mine}
    syms = {R._core(t.symbol) for t in mine}
    try:
        res1 = s1_results if s1_results is not None else RU.s1(w.trades, w.now, haircuts=w.haircuts, bots=[BOT_TNB])
    except Exception:
        res1 = []
    for r in res1:
        if not r.fires:
            continue
        a, _, c = r.target.partition("|")
        if a == approach and (cond == WILD or c == cond) and c in conds:
            out.append(f"the 3-days rule would switch it off in {c.replace('_', ' ').lower()} markets "
                       f"({r.negative_days} of {r.days} UK days negative, net {gbp(r.net_gbp, True)}): your call")
    try:
        res2 = s2_results if s2_results is not None else RU.s2(w.trades, w.now, haircuts=w.haircuts, bots=[BOT_TNB],
                                                               allowed=todays_rules(w))
    except Exception:
        res2 = []
    for r in res2:
        if r.fires and r.target in syms:
            out.append(f"the market rule would make {r.target} practice-only ({r.negative_days} of {r.days} UK days "
                       f"negative): your call")
    return out


def tick(segs: dict, w: World) -> tuple[list[Move], list[str]]:
    """The day's checks: D1, D2, D4, D5 on every coach-governed real key, S1 (Aaron's three-days rule), K1's
    follow-ups and pauses for unreadable bots. Only ever restricts. Returns (moves, needs_you lines)."""
    moves: list[Move] = []
    needs: list[str] = []
    now = w.now
    midnight = next_uk_midnight(now).isoformat()
    for key, seg in sorted(segs.items()):
        if key.startswith("@"):
            continue
        if seg.state == STATE_PINNED:
            pin_since = parse_utc(getattr(w.cfg.tnb, "live_since_utc", "") or "")
            needs += [f"Formula 1: {x}" for x in shadow_checks(w, key, pin_since) + pinned_rules(w, key)]
            continue
        if seg.state not in GOVERNED_STATES:
            continue
        since = parse_utc(seg.live_since_utc) or parse_utc(seg.since_utc)
        real = w.key_trades(key, real=True, since=since)
        if not real:
            continue
        xs = _series(real)
        sig = _sigma(w, key)
        days = len({t.uk_day for t in real})
        # D2: a gap (risk limit): paused for the rest of the UK day
        gap = [t for t in real if t.uk_day == w.today and t.x <= GAP_R + 1e-12]
        if gap and not (seg.paused_until and parse_utc(seg.paused_until) and parse_utc(seg.paused_until) > now):
            t = min(gap, key=lambda q: q.x)
            moves.append(Move(key, DOWN, "D2", seg.state, seg.step, kind=RISK_LIMIT, pause_until=midnight,
                              words=say("D2", what=key_words(key), x=rwords(t.x)),
                              numbers={"ticket": t.ticket, "x": round(t.x, 3), "net_gbp": t.net_gbp}))
        # D5: a trial's real money (risk limit)
        if seg.state == STATE_LIVE_TRIAL:
            net = round(sum(t.net_gbp for t in real), 2)
            if net <= D5_TRIAL_LOSS_GBP + 1e-9:
                moves.append(Move(key, DOWN, "D5", STATE_PRACTICE, None, kind=RISK_LIMIT, cooloff=True,
                                  words=say("D5", what=key_words(key), net=gbp(net), limit=gbp(D5_TRIAL_LOSS_GBP)),
                                  numbers={"trial_net_gbp": net, "trades": len(real)}))
                continue
        # D1: CUSUM (a judgement: at least 3 UK days of real evidence)
        s = cusum(xs, sig)
        if s >= CUSUM_H and days >= JUDGEMENT_MIN_DAYS:
            moves.append(Move(key, DOWN, "D1", STATE_PRACTICE, None, kind=JUDGEMENT, cooloff=True,
                              words=say("D1", what=key_words(key), cusum=f"{s:.1f}", limit=f"{CUSUM_H:g}"),
                              numbers={"cusum": round(s, 3), "sigma": round(sig, 3), "trades": len(xs), "days": days}))
            continue
        # D4: the key's drawdown in R (risk limit): 12 sigma from its live peak to practice, 8 sigma at this step
        _, _, dd_live = drawdown_r(xs)
        if dd_live >= D4_PRACTICE_SIGMA * sig:
            moves.append(Move(key, DOWN, "D4", STATE_PRACTICE, None, kind=RISK_LIMIT, cooloff=True,
                              words=say("D4_PRACTICE", what=key_words(key), dd=f"{dd_live:.1f} R",
                                        limit=f"{D4_PRACTICE_SIGMA * sig:.1f} R"),
                              numbers={"drawdown_r": round(dd_live, 3), "sigma": round(sig, 3)}))
            continue
        step_since = parse_utc(seg.since_utc) or since
        xs_step = _series([t for t in real if step_since is None or t.opened_utc >= step_since])
        _, _, dd_step = drawdown_r(xs_step)
        if seg.state == STATE_LIVE and dd_step >= D4_STEP_DOWN_SIGMA * sig:
            down = w.sizes.prev_step(seg.step, w.demo)
            if down is not None:
                moves.append(Move(key, DOWN, "D4", STATE_LIVE, down, kind=RISK_LIMIT, hold=True,
                                  words=say("D4_STEP", what=key_words(key), dd=f"{dd_step:.1f} R",
                                            limit=f"{D4_STEP_DOWN_SIGMA * sig:.1f} R", size=step_words(down)),
                                  numbers={"drawdown_r": round(dd_step, 3), "sigma": round(sig, 3)}))
            else:
                moves.append(Move(key, DOWN, "D4", STATE_PRACTICE, None, kind=RISK_LIMIT, cooloff=True,
                                  words=say("D4_PRACTICE", what=key_words(key), dd=f"{dd_step:.1f} R",
                                            limit=f"{D4_STEP_DOWN_SIGMA * sig:.1f} R"),
                                  numbers={"drawdown_r": round(dd_step, 3), "sigma": round(sig, 3)}))
    moves += s1_moves(segs, w)
    moves += placed_moves(segs, w)
    return moves, needs


def s1_moves(segs: dict, w: World) -> list[Move]:
    """S1, Aaron's rule exactly (rules.s1): an approach in a condition negative over 3 or more UK days in the
    current epoch is banned from real money (practice continues). Pinned keys: shadow only."""
    out = []
    try:
        results = RU.s1(w.trades, w.now, haircuts=w.haircuts, bots=[BOT_TNB])
    except Exception:
        return out
    firing = {(r.target): r for r in results if r.fires}
    if not firing:
        return out
    for key, seg in sorted(segs.items()):
        if key.startswith("@") or not key.startswith("tnb|") or seg.state in (STATE_BANNED_REAL, STATE_PINNED):
            continue
        _, approach, kind, cond = key.split("|")
        res = firing.get(f"{approach}|{cond}")
        if res is None:
            continue
        ep = E.latest_epoch(key, w.now)
        out.append(Move(key, DOWN, "S1", STATE_BANNED_REAL, None, kind=JUDGEMENT, cooloff=True,
                        words=say("S1", what=key_words(key), detail=res.words()),
                        numbers=dict(res.numbers, epoch=ep.isoformat() if ep else "")))
    return out


class TodaysRules:
    """The standing rules' filter (17 Oct): does today's Trend & Breakout take this trade? Two parts:

    * the approach rules in ``cfg`` (engine.tactics.allowed_by_rules: approach, market condition, kind of
      market, twins, the score floors; the market list aside);
    * today's news window (coach.newswin, the Mac's own calendar archive): a Trend & Breakout trade entered
      before the window came in (newswin.WINDOW_SINCE, 10 Oct) in the ``news.blackout_before_seconds`` before
      a release that closes it (15 minutes) does not count. Trades from 10 Oct were taken with the window in
      force, so they are not checked again. A day the archive does not cover counts the trade, as before.

    Called with a trade, it says True (counts) or False; it never raises (an error counts the trade). What the
    news window took out is kept in ``left_out`` (ticket -> what), and Trend & Breakout trades it could not
    check in ``unchecked``."""

    def __init__(self, cfg, news=None):
        from ..engine.tactics import allowed_by_rules
        self.cfg, self.news, self._allowed = cfg, news, allowed_by_rules
        self.suffix = str(getattr(cfg.scan, "twin_suffix", "") or "_2X")
        self.left_out: dict = {}
        self.unchecked: set = set()

    def __call__(self, t) -> bool:
        try:
            approach = t.approach + self.suffix if R.F_TWIN in (t.flags or ()) else t.approach
            if not self._allowed(self.cfg, approach, t.condition, t.kind, t.score):
                return False
        except Exception:
            pass
        if self.news is None or getattr(t, "bot", None) != BOT_TNB:
            return True
        try:
            from .newswin import WINDOW_SINCE
            if t.opened_utc >= WINDOW_SINCE:
                return True
        except Exception:
            return True
        try:
            inside = self.news.inside(t.symbol, t.opened_utc)
            if inside is None:
                self.unchecked.add(t.ticket)
                return True
            if inside:
                self.left_out[t.ticket] = {"ticket": t.ticket, "symbol": t.symbol,
                                           "opened_utc": t.opened_utc.isoformat(), "real": bool(t.real),
                                           "net_gbp": round(float(t.net_gbp), 2),
                                           "before": self.news.release(t.symbol, t.opened_utc)}
                return False
        except Exception:
            pass
        return True


def todays_rules(w: World):
    """S2's and the market read-out's filter (17 Oct): a TodaysRules on ``w.cfg`` and the calendar archive in
    ``w.data_dir``, built once for each World. The approach part follows Aaron's "Keep the approaches off and
    continue with how we should be if they weren't there"; the news part is Claude's reading of "today's rules"
    (the 10 Oct window is one of them), shown to Aaron with the one verdict it decides. None without a readable
    config (every trade counts, as before). Never raises."""
    cfg = getattr(w, "cfg", None)
    if cfg is None or getattr(cfg, "scan", None) is None:
        return None
    got = getattr(w, "_todays_rules", None)
    if got is not None:
        return got
    news = None
    try:
        from . import newswin as NW
        mine = [t.opened_utc for t in w.trades if t.bot == BOT_TNB]
        if mine:
            news = NW.load(getattr(w, "data_dir", None), cfg, min(mine), max(mine))
    except Exception:
        news = None
    try:
        got = TodaysRules(cfg, news)
    except Exception:
        return None
    try:
        w._todays_rules = got
    except Exception:
        pass
    return got


def market_watch(w: World) -> list[dict]:
    """The market rule on Trend & Breakout's whole record, DEMO and PAPER added together, on the trades today's
    rules allow (rules.market_counts): what the nightly review reads to judge the rule (17 Oct). Reported only.
    Never raises."""
    try:
        return RU.market_counts(w.trades, w.now, bots=[BOT_TNB], allowed=todays_rules(w))
    except Exception:
        return []


def news_watch(w: World) -> dict:
    """What today's news window took out of the standing rules' count (17 Oct), for the nightly review: the
    window, the trades it left out (at most 20, newest first) and how many Trend & Breakout trades the calendar
    archive could not check. Reported only. Never raises."""
    try:
        f = todays_rules(w)
        if f is None:
            return {"window_minutes": None, "calendar": False, "left_out": [], "left_out_count": 0, "unchecked": 0}
        for t in w.trades:                           # the filter notes what it leaves out as it is used
            if t.bot == BOT_TNB and t.closed_utc <= w.now:
                f(t)
        rows = sorted(f.left_out.values(), key=lambda r: r["opened_utc"], reverse=True)
        return {"window_minutes": round(f.news.window_s / 60.0, 2) if f.news else None,
                "calendar": f.news is not None,
                "calendar_days": len(f.news.days) if f.news else 0,
                "left_out": rows[:20], "left_out_count": len(rows), "unchecked": len(f.unchecked)}
    except Exception:
        return {"window_minutes": None, "calendar": False, "left_out": [], "left_out_count": 0, "unchecked": 0,
                "error": "not read"}


def s2_blocks(w: World) -> list:
    """S2, Aaron's rule: a symbol negative on at least 2/3 of 6 or more traded days is practice-only for the
    coach's keys. Returns the rules' results that fire (Trend & Breakout)."""
    try:
        return [r for r in RU.s2(w.trades, w.now, haircuts=w.haircuts, bots=[BOT_TNB], allowed=todays_rules(w))
                if r.fires]
    except Exception:
        return []


def placed_moves(segs: dict, w: World) -> list[Move]:
    """A placed bot (Momentum Runner, Financial Ian, or Band Breaker or Crowd Fader if Aaron made one LIVE)
    judged after 30 real trades by verdicts' rule: "back to paper" (NO EDGE) is K1 - no new entries, and
    Aaron is asked to switch it to PAPER. After a judgement that keeps it, D2 pauses it for a day; D4 (12
    sigma) stops new entries. Its money never rises."""
    out = []
    for bot, key in BOT_KEYS.items():
        seg = segs.get(key)
        if seg is None or seg.state != STATE_PLACED or seg.rule == "K1":
            continue
        real = [t for t in w.trades if t.bot == bot and t.real and t.closed_utc <= w.now]
        if len(real) < PLACED_JUDGE_AFTER:
            continue
        try:
            from ..ops.verdicts import NO_EDGE, decide as verdict_rule, stats
            s = stats([{"closed": t.closed_utc, "net": t.net_gbp, "costs": t.commission_gbp} for t in real])
            code, why = verdict_rule(s)
        except Exception:
            continue
        if code == NO_EDGE:
            out.append(Move(key, DOWN, "K1", STATE_PLACED, None, kind=JUDGEMENT,
                            words=say("K1", what=BOT_LABEL.get(bot, bot), n=len(real), why=why),
                            numbers={"trades": len(real), "net": s.get("net"), "verdict": code}))
            continue
        xs = _series(real)
        _, _, dd = drawdown_r(xs)
        sig = max(1.0, statistics_sd(xs))
        if dd >= D4_PRACTICE_SIGMA * sig:
            out.append(Move(key, DOWN, "D4", STATE_PLACED, None, kind=RISK_LIMIT,
                            words=say("D4_PRACTICE", what=BOT_LABEL.get(bot, bot), dd=f"{dd:.1f} R",
                                      limit=f"{D4_PRACTICE_SIGMA * sig:.1f} R"),
                            numbers={"drawdown_r": round(dd, 3)}))
            continue
        gap = [t for t in real if t.uk_day == w.today and t.x <= GAP_R]
        if gap and not (seg.paused_until and parse_utc(seg.paused_until) and parse_utc(seg.paused_until) > w.now):
            out.append(Move(key, DOWN, "D2", seg.state, None, kind=RISK_LIMIT, pause_until=next_uk_midnight(w.now).isoformat(),
                            words=say("D2", what=BOT_LABEL.get(bot, bot), x=rwords(min(t.x for t in gap)))))
    return out


def statistics_sd(xs: Sequence[float]) -> float:
    if len(xs) < 2:
        return 1.0
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


# =====================================================================
# The nightly: moves down now, moves up into tomorrow's plan
# =====================================================================
@dataclass
class Plan:
    """Tomorrow's game plan (section 8): moves up released at 06:30 UK on the next UK trading day unless
    vetoed; announced this evening."""
    date: str                          # the UK date of the nightly
    release_utc: str
    items: list = field(default_factory=list)          # Move dicts, each with its item id
    luck: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)          # needs you / needs your yes
    floors: list = field(default_factory=list)         # tighten-only floor changes (released with the plan)

    def as_dict(self) -> dict:
        return {"date": self.date, "release_utc": self.release_utc, "items": list(self.items),
                "luck": dict(self.luck), "notes": list(self.notes), "floors": list(self.floors)}


def stop_safety(keys: Sequence[tuple[float, Sequence[float], float]], equity: float, stop_pct: float,
                seed: int, days: int = STOP_SAFETY_DAYS) -> float:
    """The share of bootstrap days that hit -stop% of equity: each key (rate a day, its outcomes in R, money a
    trade) trades Poisson(rate) times a day with outcomes resampled from its own; fixed seed (the date)."""
    rng = random.Random(int(seed))
    limit = -abs(float(stop_pct)) / 100.0 * float(equity)
    hits = 0
    usable = [(lam, list(xs), money) for lam, xs, money in keys if xs and lam > 0 and money > 0]
    if not usable:
        return 0.0
    for _ in range(int(days)):
        day = 0.0
        for lam, xs, money in usable:
            n = _poisson(rng, lam)
            for _i in range(n):
                day += rng.choice(xs) * money
        if day <= limit:
            hits += 1
    return hits / float(days)


def _poisson(rng: random.Random, lam: float) -> int:
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1
        if k > 200:
            return k


def outcomes_for(w: World, key: str) -> list[float]:
    """A key's outcomes for the bootstrap: its real x, or with fewer than 20 real, every source's x with the
    practice haircut taken off."""
    real = w.key_trades(key, real=True)
    if len(real) >= 20:
        return [t.x for t in real]
    h = float(getattr(w.haircuts.get(key), "h", R.HAIRCUT_DEFAULT) if w.haircuts.get(key) is not None
              else R.HAIRCUT_DEFAULT)
    return [t.x - (0.0 if t.real else h) for t in w.key_trades(key)]


def _live_book(segs: dict, w: World, override: Optional[tuple[str, str]] = None) -> list[tuple[float, list, float]]:
    """Every LIVE key at its (proposed) size, for the stop-safety bootstrap."""
    book = []
    eq = float(w.equity or 0.0)
    for key, seg in segs.items():
        if key.startswith("@") or seg.state not in GOVERNED_STATES:
            continue
        step = seg.step or (TRIAL_STEP if seg.state == STATE_LIVE_TRIAL else BASE_STEP)
        if override is not None and override[0] == key:
            step = override[1]
        _, money = w.sizes.size(step, eq, w.demo, seg.state)
        e = w.est(key)
        lam = float(getattr(e, "lam", 0.0) or 0.0) if e is not None else 0.0
        book.append((lam, outcomes_for(w, key), money))
    if override is not None and override[0] not in {k for k, s in segs.items() if s.state in GOVERNED_STATES}:
        e = w.est(override[0])
        _, money = w.sizes.size(override[1], eq, w.demo)
        book.append((float(getattr(e, "lam", 0.0) or 0.0) if e else 0.0, outcomes_for(w, override[0]), money))
    return book


def _seed(day: dt.date) -> int:
    return int(day.strftime("%Y%m%d"))


def _steps_this_week(changes: Sequence, today: dt.date) -> int:
    wk = iso_week(today)
    n = 0
    for c in changes:
        if c.rule not in ("G3",) or not c.ts_utc:
            continue
        t = parse_utc(c.ts_utc)
        if t is not None and iso_week(uk_day(t)) == wk:
            n += 1
    return n


def formula1_live(w: World) -> dict:
    """Formula 1's live record from the records: Trend & Breakout's REAL positions on minor pairs opened since
    tnb.live_since_utc, any approach (C2: verdicts.formula1_live counts them the same way)."""
    since = parse_utc(getattr(getattr(w.cfg, "tnb", None), "live_since_utc", "") or "")
    if since is None:
        return {"since": None, "trades": [], "n": 0}
    rows = [t for t in w.trades if t.bot == BOT_TNB and t.real and t.kind == "FX_MINOR" and t.opened_utc >= since
            and t.closed_utc <= w.now]
    rec = day_record(rows)
    return {"since": since, "trades": rows, "n": len(rows), **rec}


def _g1_eligible(key: str, seg: Segment, w: World, blocked_symbols: set) -> str:
    """Why this key may not be considered for a trial ("" when it may)."""
    bot, approach, kind, cond = key.split("|")
    if bot != BOT_TNB:
        return "only Trend & Breakout keys take trials in v1"
    if BOT_TNB not in w.env.govern:
        return "Trend & Breakout is not governed"
    if str(getattr(getattr(w.cfg, "tnb", None), "mode", "") or "").upper() == "OFF":
        return "Trend & Breakout is off"
    if cond == CONDITION_UNCLEAR or WILD in (approach, kind, cond):
        return "UNCLEAR is never granted"
    if seg.state != STATE_PRACTICE:
        return f"it is {seg.state}"
    if seg.cooloff_until and seg.cooloff_until > w.today.isoformat():
        return f"in its cool-off until {seg.cooloff_until}"
    why = w.forbidden(key)
    if why:
        return why
    excluded = {str(s).upper() for s in (getattr(getattr(w.cfg, "universe", None), "excluded_symbols", ()) or ())}
    syms = {R._core(t.symbol) for t in w.key_trades(key)}
    if syms and not (syms - blocked_symbols - excluded):
        return "its markets are practice-only"
    return ""


def nightly(segs: dict, w: World, *, luck: Any = None, changes: Sequence = (), approvals: Sequence = (),
            floors_now: Optional[Mapping] = None, blocked_symbols: Iterable[str] = ()) -> tuple[list[Move], Plan]:
    """The nightly review (22:20 UK): D3, G2's failures, K1 and the release of finished cool-offs apply
    now (they only restrict or end a ban); G1, G2's passes, G3, the demo ladder, Formula 1's judgement and
    tighten-only floors go into tomorrow's plan, released at 06:30 UK on the next UK trading day unless
    vetoed. A luck check above 2% blocks every promotion tonight; so do the brakes; in GUARD nothing is
    planned up (the coach may only restrict). WATCH plans as PILOT would (the would-be decisions are shown
    and logged; the gate uses none of them)."""
    now = w.now
    release = next_release_utc(now + dt.timedelta(minutes=1)).isoformat()
    plan = Plan(uk_day(now).isoformat(), release)
    downs: list[Move] = []
    ups: list[Move] = []
    blocked_symbols = {str(s).upper() for s in blocked_symbols}
    if luck is not None:
        plan.luck = dict(getattr(luck, "numbers", {}) or {})
    luck_blocked = bool(getattr(luck, "blocked", False))
    may_promote = w.mode in (MODE_PILOT, MODE_WATCH) and not luck_blocked and w.data_ok
    br = w.brakes
    n_seq = [0]

    def item_id() -> str:
        n_seq[0] += 1
        return str(n_seq[0])

    # ---- D3 and G2 on the governed keys
    for key, seg in sorted(segs.items()):
        if key.startswith("@") or seg.state not in GOVERNED_STATES:
            continue
        e = w.est(key)
        since = parse_utc(seg.live_since_utc) or parse_utc(seg.since_utc)
        real = w.key_trades(key, real=True, since=since)
        rec = day_record(real)
        enough_days = rec["days"] >= JUDGEMENT_MIN_DAYS
        if seg.state == STATE_LIVE and e is not None and enough_days:
            p = float(e.p_plus_real)
            nums = {"p_plus_real": round(p, 4), "n_real_eff": round(float(e.n_real_eff), 2), "days": rec["days"]}
            if p < D3_TO_PRACTICE_P and float(e.n_real_eff) >= D3_TO_PRACTICE_MIN_NEFF:
                downs.append(Move(key, DOWN, "D3", STATE_PRACTICE, None, kind=JUDGEMENT, cooloff=True,
                                  words=say("D3_PRACTICE", what=key_words(key), p_plus=f"{p:.2f}"), numbers=nums))
                continue
            if p < D3_TO_BASE_P and seg.step not in (BASE_STEP, None):
                downs.append(Move(key, DOWN, "D3", STATE_LIVE, BASE_STEP, kind=JUDGEMENT, hold=True,
                                  words=say("D3_BASE", what=key_words(key), p_plus=f"{p:.2f}",
                                            size=step_words(BASE_STEP)), numbers=nums))
                continue
            if p < D3_STEP_DOWN_P and seg.step not in (BASE_STEP, None):
                down = w.sizes.prev_step(seg.step, w.demo) or BASE_STEP
                downs.append(Move(key, DOWN, "D3", STATE_LIVE, down, kind=JUDGEMENT, hold=True,
                                  words=say("D3_STEP", what=key_words(key), p_plus=f"{p:.2f}", size=step_words(down)),
                                  numbers=nums))
                continue
        if seg.state == STATE_LIVE_TRIAL and rec["trades"] >= G2_MIN_TRADES and rec["days"] >= G2_MIN_DAYS:
            p = float(e.p_plus_real) if e is not None else 0.0
            gap_ok = True
            hc = w.haircuts.get(key)
            if hc is not None and getattr(hc, "measured", False) and hc.gap is not None:
                gap_ok = -float(hc.gap) >= G2_MIN_GAP_R          # real minus practice, once measurable
            passed = rec["net"] > 0 and rec["up"] > rec["down"] and p >= G2_MIN_P_PLUS_REAL and gap_ok
            nums = dict(rec, p_plus_real=round(p, 4))
            if passed:
                if may_promote:
                    ups.append(Move(key, UP, "G2", STATE_LIVE, BASE_STEP, seg.state, seg.step, kind=JUDGEMENT,
                                    item=item_id(),
                                    words=say("G2", what=key_words(key), trades=rec["trades"], days=rec["days"],
                                              net=gbp(rec["net"], True), up=rec["up"], down=rec["down"],
                                              size=step_words(BASE_STEP)), numbers=nums))
            else:
                ep = E.latest_epoch(key, now)
                ep = ep.isoformat() if ep else ""
                second = seg.g2_failures >= 1 and (seg.epoch or "") == ep
                if second:
                    downs.append(Move(key, DOWN, "G2_BAN", STATE_BANNED_REAL, None, kind=JUDGEMENT, cooloff=True,
                                      words=say("G2_BAN", what=key_words(key)), numbers=dict(nums, epoch=ep)))
                else:
                    downs.append(Move(key, DOWN, "G2_FAIL", STATE_PRACTICE, None, kind=JUDGEMENT, cooloff=True,
                                      words=say("G2_FAIL", what=key_words(key), trades=rec["trades"], days=rec["days"],
                                                net=gbp(rec["net"], True), up=rec["up"], down=rec["down"]),
                                      numbers=nums))
    # ---- D3 in shadow for a pinned key (section 12): shown under "Needs you", never applied
    for key, seg in sorted(segs.items()):
        if key.startswith("@") or seg.state != STATE_PINNED or w.pinned(key) is None:
            continue
        e = w.est(key)
        pin_since = parse_utc(getattr(getattr(w.cfg, "tnb", None), "live_since_utc", "") or "")
        rec = day_record(w.key_trades(key, real=True, since=pin_since))
        if e is None or rec["days"] < JUDGEMENT_MIN_DAYS:
            continue
        p = float(e.p_plus_real)
        if p < D3_TO_PRACTICE_P and float(e.n_real_eff) >= D3_TO_PRACTICE_MIN_NEFF:
            plan.notes.append(f"Formula 1: the nightly test (chance of a real edge {p:.2f} on its real trades) "
                              f"would send it to practice: your call.")
    # ---- a ban ends after its cool-off (back to practice; a trial must be earned again through G1)
    for key, seg in sorted(segs.items()):
        if seg.state == STATE_BANNED_REAL and seg.cooloff_until and seg.cooloff_until <= w.today.isoformat():
            ep = E.latest_epoch(key, now)
            if seg.banned_epoch and ep is not None and seg.banned_epoch == ep.isoformat() and seg.rule == "G2_BAN":
                continue                                     # banned for this epoch
            if seg.rule == "S1":
                _, approach, _, cond = key.split("|")
                same = [t for t in w.trades if t.bot == BOT_TNB and t.approach == approach and t.condition == cond]
                if RU.s1_fires_on(same, now, haircuts=w.haircuts):
                    continue                                 # Aaron's rule still fires on it
            downs.append(Move(key, "note", "COOLOFF_END", STATE_PRACTICE, None, kind=JUDGEMENT,
                              words=say("PRACTICE_KEY", what=key_words(key))))
    # ---- Formula 1's judgement (the plan's rule), released at 06:30 UK
    for pin in w.pins:
        if pin.judge_after <= 0 or pin.key in w.judged:
            continue
        f1 = formula1_live(w)
        if f1["n"] < pin.judge_after:
            plan.notes.append(f"Formula 1's judgement after {pin.judge_after} live trades "
                              f"({max(0, pin.judge_after - f1['n'])} to go).")
            continue
        keep = f1["net"] > 0 and f1["up"] > f1["down"]
        verdict = JUDGEMENT_KEEP if keep else JUDGEMENT_PRACTICE
        # live_since_utc: its live record's start (the 20 judged trades count for its first step); the change
        # itself (since_utc) is the release, so G3's 5 UK trading days are counted from then
        nums = {"verdict": verdict, "live_trades": f1["n"], "net": f1["net"], "up": f1["up"], "down": f1["down"],
                "live_since_utc": f1["since"].isoformat() if f1["since"] else "", "judged_utc": now.isoformat()}
        if keep:
            ups.append(Move(pin.key, UP, "F1_KEEP", STATE_LIVE, BASE_STEP, STATE_PINNED, None, kind=JUDGEMENT,
                            item=item_id(),
                            words=say("F1_KEEP", n=f1["n"], net=gbp(f1["net"], True), up=f1["up"], down=f1["down"],
                                      size=step_words(BASE_STEP, w.sizes.base_money)), numbers=nums))
        else:
            ups.append(Move(pin.key, DOWN, "F1_FAIL", STATE_PRACTICE, None, STATE_PINNED, None, kind=JUDGEMENT,
                            item=item_id(),
                            words=say("F1_FAIL", n=f1["n"], net=gbp(f1["net"], True), up=f1["up"], down=f1["down"]),
                            numbers=nums))
    # ---- Formula 1's jump to the top size: Aaron's choice alone (an approval)
    f1seg = segs.get(F1_KEY)
    if (f1seg is not None and f1seg.state == STATE_LIVE and f1seg.rule in ("F1_KEEP", "G3")
            and any(a["id"] == F1_JUMP_ITEM and a["action"] == "approve" for a in approvals)
            and f1seg.step != TOP_V1_STEP and w.mode == MODE_PILOT):
        ups.append(Move(F1_KEY, UP, "F1_JUMP", STATE_LIVE, TOP_V1_STEP, STATE_LIVE, f1seg.step, kind=JUDGEMENT,
                        item=F1_JUMP_ITEM,
                        words=say("F1_JUMP", size=step_words(TOP_V1_STEP)), numbers={"approved_by": "aaron"}))
    if may_promote:
        ups += _g1(segs, w, luck, blocked_symbols, item_id)
        ups += _g3(segs, w, changes, item_id)
        ups += _demo_stop(segs, w, item_id)
    elif luck_blocked:
        plan.notes.append(f"No promotion tonight: {luck.passed} of {luck.tested} made-up segments passed the luck "
                          f"check ({luck.rate:.1%}, above 2%).")
    # ---- tighten-only floors (section 13), released with the plan
    try:
        from . import tweaks as TW
        current = {}
        for k, f in (floors_now or {}).items():
            current[k] = TW.FloorState(Floor(f.get("min_score"), f.get("min_rr")),
                                       parse_utc(f.get("since_utc")) or now)
        for ch in TW.nightly_floors(w.trades, now, w.cfg, current=current, pins=w.pins, haircuts=w.haircuts,
                                    data_dir=w.data_dir):
            plan.floors.append({"key": ch.key, "action": ch.action, "field": ch.field, "value": ch.value,
                                "before": ch.before, "numbers": ch.numbers, "item": item_id()})
    except Exception as exc:                                  # the tweaks are cut-able: never a reason to fail
        plan.notes.append(f"Floors not tested tonight ({type(exc).__name__}).")
    plan.items = [m.as_dict() for m in ups]
    return downs, plan


def _g1(segs: dict, w: World, luck: Any, blocked_symbols: set, item_id) -> list[Move]:
    br = w.brakes
    if br.no_grants or br.trials_blocked:
        return []
    most = DEMO_MAX_TRIALS if w.demo else min(int(w.env.max_trials), MAX_TRIALS_OPEN)
    open_trials = sum(1 for s in segs.values() if s.state == STATE_LIVE_TRIAL)
    if open_trials >= most or not _fin(w.equity):
        return []
    cands = []
    for key, seg in segs.items():
        if key.startswith("@") or not key.startswith("tnb|"):
            continue
        if _g1_eligible(key, seg, w, blocked_symbols):
            continue
        e = w.est(key)
        if e is None:
            continue
        ev = RU.g1_evidence(e, w.key_trades(key), w.now, haircuts=w.haircuts)
        if ev.passed:
            cands.append((e.lcb80, key, e, ev))
    if not cands:
        return []
    cands.sort(key=lambda c: (-c[0], c[1]))                    # ties by LCB80, never by recent profit
    out = []
    for lcb, key, e, ev in cands[:MAX_NEW_TRIALS_NIGHT]:
        pct_, money = w.sizes.size(TRIAL_STEP, float(w.equity), w.demo, STATE_LIVE_TRIAL)
        out.append(Move(key, UP, "G1", STATE_LIVE_TRIAL, TRIAL_STEP, STATE_PRACTICE, None, kind=JUDGEMENT, item=item_id(),
                        words=say("G1", what=key_words(key), size=step_words(TRIAL_STEP, money, pct_),
                                  trades=e.n, days=e.days, p_plus=f"{e.p_plus:.2f}", lcb80=rwords(e.lcb80)),
                        numbers=dict(ev.numbers, money_gbp=money, risk_pct=pct_)))
    return out


def _g3(segs: dict, w: World, changes: Sequence, item_id) -> list[Move]:
    br = w.brakes
    if br.no_steps or not _fin(w.equity):
        return []
    week_cap = DEMO_MAX_STEPS_WEEK if w.demo else MAX_STEPS_WEEK
    if _steps_this_week(changes, w.today) >= week_cap:
        return []
    fraction = DEMO_KELLY_FRACTION if w.demo else KELLY_FRACTION
    stop_cap = STOP_SAFETY_MAX_SHARE
    cands = []
    for key, seg in segs.items():
        if key.startswith("@") or seg.state != STATE_LIVE or not key.startswith("tnb|"):
            continue
        if seg.hold_until and seg.hold_until > w.today.isoformat():
            continue
        nxt = w.sizes.next_step(seg.step, w.demo)
        if nxt is None:
            continue
        e = w.est(key)
        if e is None:
            continue
        since_change = parse_utc(seg.since_utc)
        live_since = parse_utc(seg.live_since_utc) or since_change
        # Formula 1's judged trades count for its first step (it is at base since the judgement's release)
        step_start = live_since if seg.rule == "F1_KEEP" else since_change
        at_step = w.key_trades(key, real=True, since=step_start)
        rec = day_record(at_step)
        days_since = R.trading_days_between(uk_day(since_change), w.today) if since_change else 0
        if rec["trades"] < G3_MIN_TRADES or days_since < G3_MIN_DAYS_SINCE_CHANGE:
            continue
        if not (e.p_plus_real >= G3_MIN_P_PLUS_REAL and e.lcb80_real > 0):
            continue
        if not (rec["net"] > 0 and rec["up"] > rec["down"]):
            continue
        xs = _series(w.key_trades(key, real=True, since=live_since))
        peak, last, _ = drawdown_r(xs)
        if not xs or last < peak - 1e-9:
            continue                                         # never grow right after losses: a new high only
        kelly = e.kelly_pct(fraction)
        nxt_pct = w.sizes.step_pct(nxt)
        if kelly < nxt_pct - 1e-9:
            continue
        share = stop_safety(_live_book(segs, w, (key, nxt)), float(w.equity), float(w.stop_pct), _seed(w.today))
        if share > stop_cap:
            continue
        cands.append((e.lcb80_real, key, seg, nxt, kelly, share, rec, e))
    cands.sort(key=lambda c: (-c[0], c[1]))
    out = []
    for lcb, key, seg, nxt, kelly, share, rec, e in cands[:MAX_STEPS_NIGHT]:
        pct_, money = w.sizes.size(nxt, float(w.equity), w.demo)
        _, before = w.sizes.size(seg.step or BASE_STEP, float(w.equity), w.demo)
        out.append(Move(key, UP, "G3", STATE_LIVE, nxt, STATE_LIVE, seg.step, kind=JUDGEMENT, item=item_id(),
                        words=say("G3", what=key_words(key), from_size=step_words(seg.step, before),
                                  size=step_words(nxt, money, pct_), trades=rec["trades"],
                                  p_plus=f"{e.p_plus_real:.2f}", kelly=pct(round(kelly, 2))),
                        numbers={"trades": rec["trades"], "net": rec["net"], "up": rec["up"], "down": rec["down"],
                                 "p_plus_real": round(e.p_plus_real, 4), "lcb80_real": round(e.lcb80_real, 4),
                                 "kelly_pct": round(kelly, 3), "kelly_fraction": fraction, "stop_share": round(share, 4),
                                 "stop_pct": w.stop_pct, "money_gbp": money, "risk_pct": pct_, "demo": w.demo}))
    return out


# --------------------------------------------------- the demo daily stop --
STOP_KEY = "@daily_stop"


def stop_level(segs: dict, cfg: Any, demo: bool) -> float:
    """The effective daily stop in percent: config.json's unless the demo test passes and the coach has
    lifted it (a released plan item); never above the ladder's 10%."""
    base = float(cfg.risk.max_daily_loss_pct)
    seg = segs.get(STOP_KEY)
    if not demo or seg is None or not seg.step:
        return base
    try:
        lvl = float(seg.step)
    except ValueError:
        return base
    return max(base, min(lvl, DEMO_MAX_DAILY_STOP_PCT))


def stop_blocked_days(data_dir: Optional[str], since: dt.date, proven: Iterable[str], trades: Sequence) -> int:
    """Days in the window where the daily-loss stop blocked an entry of a proven key and the day still ended
    positive (journal ``decisions``, read only). 0 when it cannot be read."""
    if not data_dir:
        return 0
    path = Path(data_dir) / "journal.sqlite"
    if not path.exists():
        return 0
    proven = {tuple(k.split("|")[1:4:2]) for k in proven}                     # (approach, condition)
    real_by_day: dict = {}
    for t in trades:
        if t.real:
            real_by_day[t.uk_day] = real_by_day.get(t.uk_day, 0.0) + t.net_gbp
    days = set()
    try:
        with closing(R.connect_ro(path)) as con:
            cols = R.table_columns(con, "decisions")
            if not {"as_of_utc", "tactic", "regime", "blockers", "acted"} <= cols:
                return 0
            for r in con.execute("SELECT as_of_utc, tactic, regime, blockers FROM decisions WHERE acted = 0 "
                                 "AND as_of_utc >= ?", (dt.datetime(since.year, since.month, since.day,
                                                                    tzinfo=UTC).isoformat(),)):
                b = str(r["blockers"] or "").upper()
                if "DAILY_LOSS" not in b and "DAILY LOSS" not in b:
                    continue
                from ..engine.tactics import approach_of
                if (approach_of(str(r["tactic"] or "")), str(r["regime"] or "").upper()) not in proven:
                    continue
                t = parse_utc(r["as_of_utc"])
                if t is not None and real_by_day.get(uk_day(t), 0.0) > 0:
                    days.add(uk_day(t))
    except Exception:
        return 0
    return len(days)


def _demo_stop(segs: dict, w: World, item_id) -> list[Move]:
    """Section 21: the daily stop one level up (3 -> 4 -> 5 -> 6 -> 8 -> 10%) on the demo, when >= 2 governed
    keys are LIVE at base or above with P+_real >= 0.90, the bootstrap of the proposed day trips the new level
    on <= 5% of days, and in the last 10 UK trading days the stop blocked a proven key on a day that still
    ended positive. Never on a real-money account; never two levels at once; not within 5 UK trading days of
    the last change."""
    if not w.demo or w.brakes.no_steps or not _fin(w.equity):
        return []
    cur = float(w.stop_pct)
    top = min(float(w.env.demo_max_daily_stop_pct), DEMO_MAX_DAILY_STOP_PCT)
    nxt = next((lvl for lvl in DEMO_DAILY_STOP_LADDER if lvl > cur + 1e-9), None)
    if nxt is None or nxt > top + 1e-9:
        return []
    seg = segs.get(STOP_KEY)
    if seg is not None and seg.since_utc:
        since = parse_utc(seg.since_utc)
        if since and R.trading_days_between(uk_day(since), w.today) < G3_MIN_DAYS_SINCE_CHANGE:
            return []
    proven = []
    for key, s in segs.items():
        if key.startswith("@") or s.state != STATE_LIVE:
            continue
        e = w.est(key)
        if e is not None and e.p_plus_real >= DEMO_STOP_RAISE_MIN_P_PLUS:
            proven.append(key)
    if len(proven) < DEMO_STOP_RAISE_MIN_LIVE_KEYS:
        return []
    share = stop_safety(_live_book(segs, w), float(w.equity), nxt, _seed(w.today))
    if share > DEMO_STOP_RAISE_MAX_SHARE:
        return []
    start = R.trading_days_back(w.today, DEMO_STOP_RAISE_LOOKBACK_DAYS)
    blocked = stop_blocked_days(w.data_dir, start, proven, w.trades)
    if blocked < 1:
        return []
    return [Move(STOP_KEY, UP, "STOP_UP", "LIFTED", f"{nxt:g}", seg.state if seg else "", seg.step if seg else None,
                 kind=JUDGEMENT, item=item_id(),
                 words=say("STOP_UP", from_pct=pct(cur), to_pct=pct(nxt), keys=len(proven)),
                 numbers={"from_pct": cur, "to_pct": nxt, "proven_keys": sorted(proven), "trip_share": round(share, 4),
                          "blocked_days": blocked})]


# =====================================================================
# The release (06:30 UK)
# =====================================================================
def _item_content(d: Mapping) -> tuple:
    """What a plan item does, whichever night planned it: a plan item (rule, key, state, step) or a floor
    (key, field, value)."""
    if "field" in d:
        return ("floor", str(d.get("key")), str(d.get("field")), d.get("value"))
    return ("move", str(d.get("rule")), str(d.get("key")), str(d.get("to_state") or ""), d.get("to_step") or None)


def vetoed_items(plan_row: Mapping, approvals: Sequence, plans: Optional[Mapping[str, Mapping]] = None) -> tuple[set, set]:
    """(item ids, item contents) of this plan that Aaron vetoed:
    - a veto naming this plan's date, by item id;
    - a veto of an earlier plan that this one replaced before it was released (the nightly runs every
      day, so Saturday's and Sunday's plans replace Friday's for the same Monday 06:30 release): by what
      the vetoed item does, so re-proposing it does not undo the veto (``plans``: date -> that plan);
    - a veto given with no plan date: only for the first release after it was given, never for later plans."""
    plan = plan_row.get("plan") or {}
    date = str(plan_row.get("date") or plan.get("date") or "")
    rel = parse_utc(plan.get("release_utc"))
    ids: set = set()
    contents: set = set()
    for a in approvals:
        if a.get("action") != "veto":
            continue
        pd = str(a.get("plan") or "")
        if pd == date and pd:
            ids.add(str(a["id"]))
        elif not pd:
            ts = parse_utc(a.get("ts")) if a.get("ts") else None
            if ts is not None and rel is not None and ts < rel and next_release_utc(ts) == rel:
                ids.add(str(a["id"]))
        else:
            old = (plans or {}).get(pd) or {}
            if rel is None or parse_utc(old.get("release_utc")) != rel:
                continue
            for it in list(old.get("items") or []) + list(old.get("floors") or []):
                if str(it.get("item")) == str(a["id"]):
                    contents.add(_item_content(it))
    return ids, contents


def release(segs: dict, plan_row: Optional[dict], w: World, approvals: Sequence,
            plans: Optional[Mapping[str, Mapping]] = None) -> tuple[list[Move], list[str], list]:
    """Tomorrow's plan takes effect: every item not vetoed whose key is still where the plan found it, with
    the mode allowing it and no brake in the way. Returns (moves, vetoed ids, floors released). ``plans``: the
    earlier plans Aaron's vetoes name (date -> plan), so a veto survives a later plan for the same release."""
    if not plan_row or plan_row.get("released_utc"):
        return [], [], []
    plan = plan_row.get("plan") or {}
    rel = parse_utc(plan.get("release_utc"))
    if rel is None or w.now < rel:
        return [], [], []
    vetoed_ids, vetoed_contents = vetoed_items(plan_row, approvals, plans)
    vetoed = {str(d.get("item")) for d in list(plan.get("items") or []) + list(plan.get("floors") or [])
              if str(d.get("item")) in vetoed_ids or _item_content(d) in vetoed_contents}
    moves: list[Move] = []
    skipped: list[str] = []
    br = w.brakes
    for d in plan.get("items") or []:
        m = Move.from_dict(d)
        what = key_words(m.key) if not m.key.startswith("@") else "The demo daily stop"
        if m.item in vetoed:
            moves.append(Move(m.key, "note", "VETO", "", words=say("VETOED", what=what, item=m.item), item=m.item))
            continue
        seg = segs.get(m.key)
        why = ""
        if m.direction == UP:
            if w.mode in (MODE_GUARD, MODE_OFF, MODE_WATCH):
                # WATCH too: a would-be promotion is shown, never applied, so nothing goes real later the
                # moment Aaron switches to PILOT; it must be planned again under PILOT and released at 06:30
                why = f"the coach is in {w.mode}"
            elif not w.data_ok:
                why = "the data checks failed"
            elif m.rule == "G1" and (br.no_grants or br.trials_blocked):
                why = "a brake is on"
            elif m.rule in ("G2", "G3", "STOP_UP") and br.no_steps:
                why = "a brake is on"
            elif m.rule in ("G3", "STOP_UP") and not w.demo and (m.to_step in DEMO_STEP_NAMES or m.rule == "STOP_UP"):
                why = "the demo test does not pass: real-money limits in force"
            elif m.rule == "G1" and seg is not None and seg.state != STATE_PRACTICE:
                why = f"it is now {seg.state}"
            elif m.rule in ("G2",) and (seg is None or seg.state != STATE_LIVE_TRIAL):
                why = "it is no longer on trial"
            elif m.from_state and (seg.state if seg is not None else "") != m.from_state:
                why = "it has changed since the plan was made"
            elif m.from_state and (seg.step or None) != (m.from_step or None):
                why = "it has changed since the plan was made"
            elif seg is not None and _paused(seg, w.now):
                why = "it is paused"
            elif m.rule == "G3" and seg is not None:
                why = _g3_still_holds(seg, w)
        if why:
            moves.append(Move(m.key, "note", "RELEASE_SKIP", "", words=say("RELEASE_SKIP", what=what, item=m.item,
                                                                           why=why), item=m.item))
            continue
        if m.rule in ("F1_KEEP", "F1_FAIL"):
            m.numbers["judged"] = True
        moves.append(m)
    floors = [f for f in (plan.get("floors") or []) if str(f.get("item")) not in vetoed]
    return moves, sorted(vetoed), floors


def _g3_still_holds(seg: Segment, w: World) -> str:
    """At the release: G3's own record checks again on every trade closed by now (a loss after the nightly
    must not be followed by a step up: never grow right after losses). "" when it still holds."""
    since_change = parse_utc(seg.since_utc)
    live_since = parse_utc(seg.live_since_utc) or since_change
    xs = _series(w.key_trades(seg.key, real=True, since=live_since))
    peak, last, _ = drawdown_r(xs)
    if not xs or last < peak - 1e-9:
        return "not at a new high of its real record"
    rec = day_record(w.key_trades(seg.key, real=True, since=since_change))
    if not (rec["net"] > 0 and rec["up"] > rec["down"]):
        return "its real record at this step no longer shows more up days than down and a profit"
    return ""


# =====================================================================
# The policy's entries
# =====================================================================
def _paused(seg: Segment, now: dt.datetime) -> bool:
    t = parse_utc(seg.paused_until) if seg.paused_until else None
    return t is not None and t > now


def entries(segs: dict, w: World) -> dict[str, Entry]:
    """Each key's instruction for the bots, with the brakes applied. Only restrictions, Aaron's pins, placed
    bots and the coach's own grants are written; any other key is on Aaron's settings (BASELINE)."""
    out: dict[str, Entry] = {}
    now = w.now
    br = w.brakes
    midnight = next_uk_midnight(now).isoformat()
    eq = float(w.equity) if _fin(w.equity) and float(w.equity) > 0 else None
    for key, seg in sorted(segs.items()):
        if key.startswith("@"):
            continue
        bot = key.split("|")[0]
        if bot not in w.env.govern:
            continue
        static = w.tnb_static_real(key) if bot == BOT_TNB else w.static_live(bot)
        restrict = ROUTE_PRACTICE if bot == BOT_TNB else ROUTE_SKIP
        if seg.state == STATE_PINNED:
            if w.pinned(key) is not None:
                out[key] = Entry(ROUTE_PIN, STATE_PINNED, rule="PIN",
                                 why=short(say("PINNED", what=key_words(key))))
            continue
        # a pause (D2) or the brakes B2 / B4 deeper on anything governed and real
        paused = _paused(seg, now)
        if seg.state in GOVERNED_STATES or (seg.state == STATE_PLACED and static):
            if paused:
                out[key] = Entry(restrict, STATE_PAUSED, rule=seg.rule if seg.rule in ("D2",) else "D2",
                                 until_utc=seg.paused_until, why=short(f"Paused until midnight UK ({key_words(key)})."))
                continue
            if br.pause_governed:
                rule = "B2" if br.day == "PAUSE" else "B4"
                out[key] = Entry(restrict, STATE_PAUSED, rule=rule, until_utc=midnight if rule == "B2" else None,
                                 why=short(br.why.get("day" if rule == "B2" else "drawdown", "")) or None)
                continue
        if bot in w.unreadable_bots and (seg.state in GOVERNED_STATES or static):
            out[key] = Entry(restrict, STATE_PAUSED, rule="B8",
                             why=short(say("B8_PAUSE", what=BOT_LABEL.get(bot, bot), why="its record failed a check")))
            continue
        if seg.state == STATE_PLACED:
            if seg.rule in ("K1", "D4", "D3"):
                out[key] = Entry(restrict, STATE_PLACED, rule=seg.rule,
                                 why=short(f"{BOT_LABEL.get(bot, bot)}: no new real entries ({seg.rule}); needs you."))
            elif bot != BOT_TNB:
                out[key] = Entry(ROUTE_BASELINE, STATE_PLACED,
                                 why=short(say("PLACED", what=BOT_LABEL.get(bot, bot), n=PLACED_JUDGE_AFTER)))
            continue
        if seg.state in (STATE_BANNED_REAL,) or (seg.state == STATE_PRACTICE and static):
            out[key] = Entry(restrict, seg.state, rule=seg.rule or None,
                             why=short(f"{key_words(key)}: practice only ({seg.rule or 'the coach'})."))
            continue
        if seg.state in GOVERNED_STATES:
            why_not = w.forbidden(key)
            if why_not:                                       # e.g. the Runner went live on Aaron's settings since
                out[key] = Entry(ROUTE_PRACTICE, STATE_PAUSED, rule="V10",
                                 why=short(f"{key_words(key)}: no real money ({why_not})."))
                continue
        if seg.state == STATE_LIVE_TRIAL:
            most = DEMO_MAX_TRIALS if w.demo else int(w.env.max_trials)
            trials_so_far = sum(1 for e in out.values() if e.state == STATE_LIVE_TRIAL)
            if trials_so_far >= most:
                out[key] = Entry(ROUTE_PRACTICE, STATE_PAUSED, step=TRIAL_STEP, rule="V11",
                                 why=short(f"{key_words(key)}: real-money limits allow {most} trials at once."))
                continue
            if br.trials_blocked:
                out[key] = Entry(ROUTE_PRACTICE, STATE_LIVE_TRIAL, step=TRIAL_STEP, rule="B1" if br.day != "OPEN" else "B7",
                                 why=short(br.why.get("day") or br.why.get("trial_budget") or "Trials are paused."))
                continue
            if eq is None:
                continue
            p, money = w.sizes.size(TRIAL_STEP, eq, w.demo, STATE_LIVE_TRIAL)
            if money <= 0:
                continue
            out[key] = Entry(ROUTE_REAL, STATE_LIVE_TRIAL, p, money, step=TRIAL_STEP, rule=seg.rule or "G1",
                             why=short(f"{key_words(key)}: live trial at {gbp(money)} a trade."))
            continue
        if seg.state == STATE_LIVE:
            if eq is None:
                continue
            step = BASE_STEP if br.at_base else (seg.step or BASE_STEP)
            p, money = w.sizes.size(step, eq, w.demo)
            if money <= 0:
                continue
            note = " (the brakes hold it at its normal size)" if step != seg.step else ""
            out[key] = Entry(ROUTE_REAL, STATE_LIVE, p, money, step=step, rule=seg.rule or None,
                             why=short(f"{key_words(key)}: real money at {gbp(money)} a trade{note}."))
    return out


def blocks(results: Sequence) -> tuple[Block, ...]:
    """S2's practice-only markets for the coach's Trend & Breakout keys."""
    syms = sorted({r.target for r in results})
    if not syms:
        return ()
    return (Block((BOT_TNB,), tuple(syms), ROUTE_PRACTICE, "S2", None, None,
                  short("Practice-only for the coach's keys (Aaron's rule: negative on 2/3 of 6 or more days): "
                        + ", ".join(syms))),)


def demo_limits(segs: dict, w: World, ents: Mapping[str, Entry]) -> Optional[DemoLimits]:
    """Section 21's lifted values for the lease: only while the demo test passes, and only values above
    config.json's (the gate re-checks the demo test itself and falls back to config.json's)."""
    if not w.demo:
        return None
    r = w.cfg.risk
    top = max((float(e.risk_pct or 0.0) for e in ents.values() if e.route == ROUTE_REAL), default=0.0)
    # always a block while the demo test passes (the validator and the gate read a demo trial at base only
    # under one); config.json's own ceiling unless a demo step lifts it
    out = {"max_risk_pct": round(min(max(top, float(r.max_risk_pct)), max(w.sizes.demo_cap_pct(),
                                                                         float(r.max_risk_pct))), 4)}
    if top > float(r.max_risk_pct) + 1e-9:
        corr = min(DEMO_TOTAL_MAX_PCT, DEMO_CORRELATED_MULT * top)
        tot = min(DEMO_TOTAL_MAX_PCT, DEMO_TOTAL_MULT * top)
        if corr > float(r.max_correlated_risk_pct):
            out["max_correlated_risk_pct"] = round(corr, 4)
        if tot > float(r.max_total_risk_pct):
            out["max_total_risk_pct"] = round(tot, 4)
    stop = float(w.stop_pct)
    if stop > float(r.max_daily_loss_pct) + 1e-9:
        out["max_daily_loss_pct"] = stop
        if stop >= DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT - 1e-9 and DEMO_DRAWDOWN_BREAKER_PCT > float(r.max_drawdown_pct):
            out["max_drawdown_pct"] = DEMO_DRAWDOWN_BREAKER_PCT
    return DemoLimits(**out) if out else None


# =====================================================================
# The scorecard and the shadow figure
# =====================================================================
def state_at(changes: Sequence, key: str, when: dt.datetime) -> tuple[str, Optional[str]]:
    """The key's state and step at ``when``, from the ledger's state changes (oldest first)."""
    st, step = STATE_PRACTICE, None
    for c in sorted((c for c in changes if c.kind == "state" and c.target == key and c.ts_utc),
                    key=lambda c: (c.ts_utc, c.id or 0)):
        t = parse_utc(c.ts_utc)
        if t is None or t > when:
            break
        text = str(c.to_state or "")
        st, _, step = text.partition(":")
        step = step or None
    return st, step


def shadow_rows(trades: Sequence, changes: Sequence, sizes: Sizes, gate_log: Mapping) -> list[dict]:
    """Section 21: for every real trade the coach sized, ``real_money_gbp`` (what it would have risked under
    the v1 rules) and ``real_money_net_gbp`` (its result rescaled to that risk)."""
    out = []
    for t in trades:
        if not t.real or t.coach_route != "REAL":
            continue
        line = gate_log.get((t.bot, t.ticket)) or {}
        money = float(line.get("money") or t.risk_gbp or 0.0)
        if money <= 0 or t.risk_gbp <= 0:
            continue
        key = str(line.get("key") or t.key)
        st, step = state_at(changes, key, t.opened_utc)
        step = step or (TRIAL_STEP if st == STATE_LIVE_TRIAL else BASE_STEP)
        try:
            pct_ = sizes.step_pct(step) if step != TRIAL_STEP else None
        except ValueError:
            pct_ = None
        logged_eq = line.get("equity")
        trial = step == TRIAL_STEP or st == STATE_LIVE_TRIAL
        if _fin(logged_eq) and float(logged_eq) > 0:
            # the equity the trade was sized at (the gate log's fill line): the v1 size of the same trade at
            # that equity, never more than what it really risked (headroom and brakes already in it)
            v1 = min(money, sizes.v1_money(TRIAL_STEP if trial else step, float(logged_eq),
                                           STATE_LIVE_TRIAL if trial else STATE_LIVE))
        elif trial:
            eq = money / (max(sizes.base_pct, 1e-9) / 100.0)                     # a demo trial is at base
            v1 = min(money, sizes.v1_money(TRIAL_STEP, eq, STATE_LIVE_TRIAL))
        elif pct_:
            eq = money / (pct_ / 100.0)
            v1 = min(money, sizes.v1_money(step, eq))
        else:
            v1 = money
        v1 = round(v1, 2)
        out.append({"ticket": t.ticket, "key": key, "closed_utc": t.closed_utc.isoformat(), "step": step,
                    "money_gbp": round(money, 2), "risk_gbp": round(t.risk_gbp, 2), "net_gbp": round(t.net_gbp, 2),
                    "real_money_gbp": v1, "real_money_net_gbp": round(t.net_gbp * v1 / t.risk_gbp, 2)})
    return out


def scorecard(trades: Sequence, gate_log: Mapping, sizes: Sizes, since: Optional[dt.datetime] = None) -> dict:
    """The coach's own effect (section 6.4), from what actually happened:
    saved - the practice result of trades it routed away from real money, priced at base size;
    added - the real result of its trials and steps less what base size would have made (a trial: all of it,
    since the key would not have been real at all). Returns the totals and the per-trade effects."""
    by_ticket = {(t.bot, t.ticket): t for t in trades}
    effects: list[tuple[dt.date, float]] = []
    saved = added = 0.0
    for (bot, ticket), line in gate_log.items():
        t = by_ticket.get((bot, ticket))
        if t is None or (since is not None and t.closed_utc < since):
            continue
        route = str(line.get("route") or "").upper()
        # saved only when the line says the trade was real on Aaron's own settings: a practice trade the coach
        # never moved away from real money saved nothing (a line that does not say counts as not real)
        if route in (ROUTE_PRACTICE, ROUTE_SKIP) and not t.real and line.get("static_real") is True:
            saved_v = -t.x * sizes.base_money                # a practice loss is money saved
            saved += saved_v
            effects.append((t.uk_day, saved_v))
        elif route == ROUTE_REAL and t.real:
            money = float(line.get("money") or t.risk_gbp)
            if money < sizes.base_money - 0.01:
                v = t.net_gbp
            else:
                v = t.x * (money - sizes.base_money)
            added += v
            effects.append((t.uk_day, v))
    days = {d for d, _ in effects}
    vals = [v for _, v in effects]
    n = len(vals)
    p_neg = None
    if n >= 2:
        m = sum(vals) / n
        sd = statistics_sd(vals)
        se = sd / math.sqrt(n) if sd > 0 else 0.0
        p_neg = (0.5 * (1 + math.erf((0 - m) / (se * math.sqrt(2))))) if se > 0 else (1.0 if m < 0 else 0.0)
    return {"saved_gbp": round(saved, 2), "added_gbp": round(added, 2), "net_gbp": round(saved + added, 2),
            "trades": n, "days": len(days), "p_negative": None if p_neg is None else round(p_neg, 4)}
