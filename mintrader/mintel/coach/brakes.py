"""Brakes and limits (spec section 7, with section 21's demo scaling).

Every brake here only restricts: none ever raises a size, and none looks at
streaks of wins or losses (the record shows no hot or cold hand).

What they count: real money on the UK day, every magic number including
Financial Ian, after commission: the page's standing block ``today`` (closed
since 00:00 UK) plus ``open`` (floating). E0 is the equity at UK midnight.

    B1 day line      real today <= -1.5% of E0     no grants, trials paused, governed keys at base
    B2 day pause     <= -2.25% of E0               governed real entries paused until UK midnight
                                                   (Trend & Breakout's granted keys to practice, Runner
                                                   rides skipped, Ian no new entries; never pinned F1)
    B3 week line     last 5 UK trading days <= -5% no grants and no steps until back above -2.5%
    B4 drawdown      equity <= 95% of the saved peak: no grants, no steps, keys at base
                     <= 92%: every governed real entry paused, Runner and Ian too; "Needs you"
    B5 headroom      H = L.E0 + min(0, real today) - open real risk (all bots); in the policy
    B6 one idea      inside the trader's gate (not here)
    B7 trial budget  trial losses this ISO week >= £40: no new trials until Monday's release
    B8 data          a failed data check: no grants; an unreadable governed bot paused
    B9 coach breaker in run.py: GUARD on > 20 policy changes a day, > 3 trial starts a week, an audit
                     finding or 3 tick errors in a row

On the demo (section 21, only while the demo test passes and the coach's
daily stop has been lifted): B1 and B2 sit at half and three quarters of the
effective daily stop; once that stop is 6% or more B4 sits at 10% / 15%; the
trial budget is £150 a week. On a real-money account the v1 values apply.

The 3% stop, the 12% drawdown breaker and every existing breaker are
untouched: this module never writes them. The coach saves its own equity peak
in its ledger, so the peak survives a restart. Drawdown brakes are released
only at the nightly review, once the drawdown is under half the trigger.
Brake states are never released on unknown data.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import asdict, dataclass, field
from typing import Optional

from . import (DEMO_DAY_LINE_SHARE, DEMO_DAY_PAUSE_SHARE, DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT,
               DEMO_DRAWDOWN_LINE_PCT, DEMO_DRAWDOWN_PAUSE_PCT, DEMO_TRIAL_LOSS_BUDGET_GBP_WEEK,
               DRAWDOWN_RELEASE_SHARE, WEEK_LINE_RELEASE_PCT, Envelope, as_utc, uk_day)
from .words import gbp, pct, say

OPEN, LINE, PAUSE = "OPEN", "LINE", "PAUSE"
_RANK = {OPEN: 0, LINE: 1, PAUSE: 2}


def _fin(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(float(x))


@dataclass(frozen=True)
class Lines:
    """The brake lines in force, in percent (section 7, or section 21 on the demo)."""
    stop_pct: float                   # the effective daily stop (config.json's, or the demo's lifted one)
    day_line_pct: float
    day_pause_pct: float
    week_line_pct: float
    drawdown_line_pct: float
    drawdown_pause_pct: float
    trial_budget_gbp: float
    demo: bool = False


def lines(env: Envelope, config_stop_pct: float, stop_pct: Optional[float] = None, demo: bool = False) -> Lines:
    """The lines for the envelope and the effective daily stop. A demo line is used only when ``demo`` (the
    demo test passed) and the stop has been lifted above config.json's; otherwise the envelope's v1 lines."""
    stop = float(stop_pct if stop_pct is not None else config_stop_pct)
    lifted = demo and stop > float(config_stop_pct) + 1e-9
    if not lifted:
        return Lines(float(config_stop_pct), env.day_line_pct, env.day_pause_pct, env.week_line_pct,
                     env.drawdown_line_pct, env.drawdown_pause_pct,
                     DEMO_TRIAL_LOSS_BUDGET_GBP_WEEK if demo else env.trial_loss_budget_gbp_week, demo)
    dd_line, dd_pause = env.drawdown_line_pct, env.drawdown_pause_pct
    if stop >= DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT - 1e-9:
        dd_line, dd_pause = DEMO_DRAWDOWN_LINE_PCT, DEMO_DRAWDOWN_PAUSE_PCT
    return Lines(stop, DEMO_DAY_LINE_SHARE * stop, DEMO_DAY_PAUSE_SHARE * stop, env.week_line_pct, dd_line,
                 dd_pause, DEMO_TRIAL_LOSS_BUDGET_GBP_WEEK, True)


@dataclass
class Account:
    """The account's real-money figures, from the page's standing block (broker truth), every bot."""
    equity: Optional[float] = None
    e0: Optional[float] = None                 # equity at UK midnight
    real_today: Optional[float] = None         # closed today + floating, after commission, every magic
    open_real_risk: Optional[float] = None     # money at the stops of every open real position, every bot
    week_net: Optional[float] = None           # the last 5 UK trading days (today included)

    @property
    def readable(self) -> bool:
        return _fin(self.equity) and _fin(self.e0) and _fin(self.real_today) and float(self.e0) > 0


@dataclass
class BrakeState:
    """The brakes in force. ``day_date``: the UK day the day brake belongs to."""
    day: str = OPEN
    day_date: str = ""
    week: str = OPEN
    drawdown: str = OPEN
    trial_losses_gbp: float = 0.0
    trial_budget_gbp: float = 40.0
    headroom_gbp: Optional[float] = None
    peak: Optional[float] = None
    data_ok: bool = True
    why: dict = field(default_factory=dict)    # brake -> its sentence

    # --- what they mean for the decisions
    @property
    def trial_budget_left(self) -> float:
        return max(0.0, round(self.trial_budget_gbp - self.trial_losses_gbp, 2))

    @property
    def trials_blocked(self) -> bool:            # B7 (and B1: trials paused)
        return self.trial_budget_left <= 0 or self.day != OPEN

    @property
    def no_grants(self) -> bool:                 # B1, B3, B4, B8
        return self.day != OPEN or self.week != OPEN or self.drawdown != OPEN or not self.data_ok

    @property
    def no_steps(self) -> bool:                  # B1, B3, B4
        return self.day != OPEN or self.week != OPEN or self.drawdown != OPEN

    @property
    def at_base(self) -> bool:                   # B1, B4: every governed key at base
        return self.day != OPEN or self.drawdown != OPEN

    @property
    def pause_governed(self) -> bool:            # B2, B4 deeper
        return self.day == PAUSE or self.drawdown == PAUSE

    @property
    def any(self) -> bool:
        return self.no_grants or self.trials_blocked

    def policy_block(self) -> dict:
        return {"day": self.day, "week": self.week, "drawdown": self.drawdown,
                "trial_budget_left_gbp": self.trial_budget_left}

    def to_json(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_json(cls, d: Optional[dict]) -> "BrakeState":
        if not isinstance(d, dict):
            return cls()
        keep = {k: d[k] for k in ("day", "day_date", "week", "drawdown", "trial_losses_gbp", "trial_budget_gbp",
                                  "headroom_gbp", "peak", "data_ok", "why") if k in d}
        st = cls(**keep)
        for name, allowed in (("day", (OPEN, LINE, PAUSE)), ("week", (OPEN, LINE)), ("drawdown", (OPEN, LINE, PAUSE))):
            if getattr(st, name) not in allowed:
                setattr(st, name, OPEN)
        return st


def headroom(acct: Account, stop_pct: float) -> Optional[float]:
    """B5: ``H = L x E0 + min(0, real today) - open real risk (all bots)``, never below 0; None when a figure
    is unknown (then no grant: the data check has failed)."""
    if not (acct.readable and _fin(acct.open_real_risk)):
        return None
    h = float(stop_pct) / 100.0 * float(acct.e0) + min(0.0, float(acct.real_today)) - float(acct.open_real_risk)
    return round(max(0.0, h), 2)


def evaluate(prev: BrakeState, acct: Account, ln: Lines, *, now: dt.datetime, data_ok: bool,
             trial_losses_gbp: float, nightly: bool = False) -> tuple[BrakeState, list[tuple[str, str, str, str, dict]]]:
    """The brakes now, and what changed: (brake, from, to, sentence, numbers) for each change.

    Only restricts during the day; the day brake ends with the UK day, the week line once the week is back
    above -2.5%, and the drawdown brakes only at the nightly review once the drawdown is under half the
    trigger. Nothing is released on unknown data (the account unreadable or a data check failed)."""
    now = as_utc(now)
    today = uk_day(now).isoformat()
    st = BrakeState(day=prev.day, day_date=prev.day_date, week=prev.week, drawdown=prev.drawdown,
                    trial_losses_gbp=round(max(0.0, float(trial_losses_gbp or 0.0)), 2),
                    trial_budget_gbp=ln.trial_budget_gbp, peak=prev.peak, data_ok=bool(data_ok),
                    why=dict(prev.why or {}))
    known = acct.readable and data_ok
    moves: list[tuple[str, str, str, str, dict]] = []

    def move(name: str, to: str, sentence: str, numbers: dict):
        before = getattr(st, name)
        if before == to:
            if to != OPEN:
                st.why[name] = sentence
            return
        setattr(st, name, to)
        if to == OPEN:
            st.why.pop(name, None)
        else:
            st.why[name] = sentence
        moves.append((name, before, to, sentence, numbers))

    # ---- the peak (only on readable figures; never lowered)
    if acct.readable and _fin(acct.equity):
        eq = float(acct.equity)
        if st.peak is None or eq > float(st.peak):
            st.peak = round(eq, 2)
    # ---- B1 / B2: the day
    if st.day_date != today:
        if known:                                      # a new UK day: the day brake ends with the old one
            if st.day != OPEN:
                move("day", OPEN, say("BRAKE_OFF", what="The day brake", why="a new UK day"), {"day": today})
            st.day_date = today
        elif st.day == OPEN:
            st.day_date = today
        # otherwise: the old day's brake stays (never released on unknown data)
    if acct.readable:
        e0, today_pnl = float(acct.e0), float(acct.real_today)
        line, pause = -ln.day_line_pct / 100.0 * e0, -ln.day_pause_pct / 100.0 * e0
        nums = {"real_today": round(today_pnl, 2), "e0": round(e0, 2), "line": round(line, 2), "pause": round(pause, 2),
                "line_pct": ln.day_line_pct, "pause_pct": ln.day_pause_pct, "stop_pct": ln.stop_pct}
        # only ever escalates during the day
        if today_pnl <= pause + 1e-9 and _RANK[st.day] < _RANK[PAUSE]:
            move("day", PAUSE, say("B2", loss=gbp(-today_pnl), line=gbp(-pause), line_pct=pct(round(ln.day_pause_pct, 3)),
                                   what="every governed real entry"), nums)
        elif today_pnl <= line + 1e-9 and _RANK[st.day] < _RANK[LINE]:
            move("day", LINE, say("B1", loss=gbp(-today_pnl), line=gbp(-line), line_pct=pct(round(ln.day_line_pct, 3))),
                 nums)
    # ---- B3: the week
    if acct.readable and _fin(acct.week_net):
        e0, wk = float(acct.e0), float(acct.week_net)
        line, release = -ln.week_line_pct / 100.0 * e0, -WEEK_LINE_RELEASE_PCT / 100.0 * e0
        nums = {"week_net": round(wk, 2), "line": round(line, 2), "release": round(release, 2)}
        if wk <= line + 1e-9:
            move("week", LINE, say("B3", loss=gbp(-wk), line_pct=pct(ln.week_line_pct),
                                   release_pct=pct(WEEK_LINE_RELEASE_PCT)), nums)
        elif st.week == LINE and wk > release and known:
            move("week", OPEN, say("BRAKE_OFF", what="The week line",
                                   why=f"the last 5 UK trading days are back above -{pct(WEEK_LINE_RELEASE_PCT)}"), nums)
    # ---- B4: the drawdown from the coach's saved peak
    if acct.readable and st.peak:
        eq, peak = float(acct.equity), float(st.peak)
        dd_pct = max(0.0, (peak - eq) / peak * 100.0)
        nums = {"equity": round(eq, 2), "peak": round(peak, 2), "drawdown_pct": round(dd_pct, 3),
                "line_pct": ln.drawdown_line_pct, "pause_pct": ln.drawdown_pause_pct}
        if dd_pct >= ln.drawdown_pause_pct - 1e-9:
            if _RANK[st.drawdown] < _RANK[PAUSE]:
                move("drawdown", PAUSE, say("B4_PAUSE", dd=pct(round(dd_pct, 2)), peak=gbp(peak),
                                            line_pct=pct(ln.drawdown_pause_pct),
                                            what="every governed real entry, the Momentum Runner and Financial Ian "
                                                 "included,"), nums)
        elif dd_pct >= ln.drawdown_line_pct - 1e-9:
            if _RANK[st.drawdown] < _RANK[LINE]:
                move("drawdown", LINE, say("B4", dd=pct(round(dd_pct, 2)), peak=gbp(peak),
                                           line_pct=pct(ln.drawdown_line_pct)), nums)
        if nightly and known and st.drawdown != OPEN:
            if st.drawdown == PAUSE and dd_pct < ln.drawdown_pause_pct * DRAWDOWN_RELEASE_SHARE:
                to = LINE if dd_pct >= ln.drawdown_line_pct * DRAWDOWN_RELEASE_SHARE else OPEN
                move("drawdown", to, say("BRAKE_OFF", what="The deeper drawdown brake",
                                         why=f"the drawdown is {pct(round(dd_pct, 2))}, under half its line")
                     if to == OPEN else say("B4", dd=pct(round(dd_pct, 2)), peak=gbp(peak),
                                            line_pct=pct(ln.drawdown_line_pct)), nums)
            elif st.drawdown == LINE and dd_pct < ln.drawdown_line_pct * DRAWDOWN_RELEASE_SHARE:
                move("drawdown", OPEN, say("BRAKE_OFF", what="The drawdown brake",
                                           why=f"the drawdown is {pct(round(dd_pct, 2))}, under half its line"), nums)
    # ---- B5: headroom
    st.headroom_gbp = headroom(acct, ln.stop_pct)
    # ---- B7: the trial budget
    if st.trial_losses_gbp >= ln.trial_budget_gbp - 1e-9:
        st.why["trial_budget"] = say("B7", loss=gbp(st.trial_losses_gbp), budget=gbp(ln.trial_budget_gbp))
    else:
        st.why.pop("trial_budget", None)
    # ---- B8: data
    if not data_ok:
        st.why["data"] = "the coach's data checks failed: no grants"
    else:
        st.why.pop("data", None)
    return st, moves
