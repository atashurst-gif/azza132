"""Rapid Scalper version 3, "High-Velocity Session Rider": the entry engine.

Seconds, not candles. From the tick buffer it measures several very short
horizons at once (0.5 s to 10 s): velocity, acceleration, directional
consistency, tick frequency, micro-range expansion, distance travelled
against what is normal, retracement, spread movement, breakout distance and
the count of fresh micro highs or lows. From those it names the setup it
sees (acceleration, micro breakout, pullback launch, sweep reversal,
session burst), scores how confident it is that price is ACCELERATING with
room to go (0-100), scores whether this market is scalpable right now
(costs against the realistic move), refuses to chase a move that has
already run, and sets a micro structural stop.

Everything measurable is measured; nothing is pretended. There is no order
book over this feed, so bid/ask pressure is read from the tick tape only.
"""
from __future__ import annotations

import datetime as dt
import statistics as st
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Tick
from .config import ScalperConfig
from .features import Features, TickBuffer

HORIZONS = (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)
SETUPS = ("ACCELERATION", "MICRO_BREAKOUT", "PULLBACK_LAUNCH", "SWEEP_REVERSAL", "SESSION_BURST")


@dataclass
class Micro:
    """The micro-momentum picture, all in points (signed: + is up)."""
    ok: bool = False
    reason: str = ""
    mid: float = 0.0
    spread_points: float = 0.0
    spread_median_points: float = 0.0
    spread_ratio: float = 1.0
    velocity: dict = field(default_factory=dict)      # horizon seconds -> points per second
    acceleration: float = 0.0                         # v(1s) - v(5s), points per second
    accel_ratio: float = 0.0                          # acceleration / noise per second
    consistency: float = 0.0                          # share of the last 3 s of tick moves in the direction
    ticks_per_second: float = 0.0                     # over the last 10 s
    noise_points: float = 0.0                         # median 5-second range over the last 60 s ("micro ATR")
    range_3s: float = 0.0
    range_prior_10s: float = 0.0
    range_expansion: float = 1.0                      # range of the last 3 s against the prior 10 s, per second
    distance_5s: float = 0.0                          # signed move over 5 s
    distance_ratio: float = 0.0                       # |move over 5 s| against the usual 5-second move
    move_start_mid: float = 0.0                       # where the last 10 s move began
    move_points: float = 0.0                          # signed, from move_start_mid
    retracement: float = 0.0                          # 0..1 of the 10-second move given back
    micro_high_30: float = 0.0                        # of the 30 s BEFORE the last 3 s
    micro_low_30: float = 0.0
    high_60: float = 0.0
    low_60: float = 0.0
    breakout_points: float = 0.0                      # signed distance beyond the prior 30 s range
    new_extremes_5s: int = 0                          # fresh 30-second highs (or lows) in the last 5 s
    swept_high: bool = False                          # the 60 s high was taken in the last 5 s
    swept_low: bool = False
    direction: int = 0                                # the immediate direction, if any
    pressure: float = 0.0                             # -1..1: up-ticks against down-ticks, last 3 s

    def v(self, h: float) -> float:
        return float(self.velocity.get(h, 0.0))


def _window(ticks: Sequence[Tick], now: dt.datetime, seconds: float) -> list:
    cutoff = now - dt.timedelta(seconds=seconds)
    return [t for t in ticks if t.time >= cutoff]


def micro(buf: TickBuffer, point: float, now: Optional[dt.datetime] = None) -> Micro:
    m = Micro()
    ticks = list(buf.ticks)
    if len(ticks) < 10 or point <= 0:
        m.reason = "not enough ticks yet"
        return m
    now = now or ticks[-1].time
    last = ticks[-1]
    m.mid = last.mid
    spreads = [(t.ask - t.bid) / point for t in ticks[-200:]]
    m.spread_points = spreads[-1]
    m.spread_median_points = st.median(spreads)
    m.spread_ratio = (m.spread_points / m.spread_median_points) if m.spread_median_points > 0 else 1.0

    def vel(h: float) -> float:
        w = _window(ticks, now, h)
        if len(w) < 2:
            return 0.0
        span = (w[-1].time - w[0].time).total_seconds()
        return ((w[-1].mid - w[0].mid) / point) / span if span > 0 else 0.0
    for h in HORIZONS:
        m.velocity[h] = vel(h)
    # noise: the median 5-second range over the last minute
    w60 = _window(ticks, now, 60.0)
    ranges = []
    for k in range(12):
        end = now - dt.timedelta(seconds=5 * k)
        start = end - dt.timedelta(seconds=5)
        w = [t.mid for t in w60 if start <= t.time <= end]
        if len(w) >= 2:
            ranges.append((max(w) - min(w)) / point)
    m.noise_points = st.median(ranges) if ranges else 0.0
    noise = max(m.noise_points, m.spread_points, 1e-9)
    m.acceleration = m.v(1.0) - m.v(5.0)
    m.accel_ratio = m.acceleration / (noise / 5.0)
    # consistency and pressure from the last 3 s of tick moves
    w3 = _window(ticks, now, 3.0)
    ups = downs = 0
    for a, b in zip(w3[:-1], w3[1:]):
        if b.mid > a.mid:
            ups += 1
        elif b.mid < a.mid:
            downs += 1
    moves = ups + downs
    m.pressure = (ups - downs) / moves if moves else 0.0
    w10 = _window(ticks, now, 10.0)
    m.ticks_per_second = len(w10) / 10.0
    mids3 = [t.mid for t in w3]
    m.range_3s = (max(mids3) - min(mids3)) / point if len(mids3) >= 2 else 0.0
    prior = [t.mid for t in ticks if now - dt.timedelta(seconds=13) <= t.time < now - dt.timedelta(seconds=3)]
    m.range_prior_10s = (max(prior) - min(prior)) / point if len(prior) >= 2 else 0.0
    m.range_expansion = ((m.range_3s / 3.0) / (m.range_prior_10s / 10.0)) if m.range_prior_10s > 0 else (2.0 if m.range_3s > 0 else 1.0)
    w5 = _window(ticks, now, 5.0)
    m.distance_5s = ((w5[-1].mid - w5[0].mid) / point) if len(w5) >= 2 else 0.0
    m.distance_ratio = abs(m.distance_5s) / noise
    # the last 10 s move and how much of it has been given back
    if len(w10) >= 2:
        m.move_start_mid = w10[0].mid
        m.move_points = (m.mid - m.move_start_mid) / point
        ext = max((t.mid for t in w10), default=m.mid) if m.move_points >= 0 else min((t.mid for t in w10), default=m.mid)
        full = abs(ext - m.move_start_mid)
        m.retracement = (abs(ext - m.mid) / full) if full > 0 else 0.0
    # structure: the 30 s before the last 3 s, and the whole last 60 s
    pre30 = [t for t in ticks if now - dt.timedelta(seconds=33) <= t.time < now - dt.timedelta(seconds=3)]
    if pre30:
        m.micro_high_30 = max(t.mid for t in pre30)
        m.micro_low_30 = min(t.mid for t in pre30)
        if m.mid > m.micro_high_30:
            m.breakout_points = (m.mid - m.micro_high_30) / point
        elif m.mid < m.micro_low_30:
            m.breakout_points = (m.mid - m.micro_low_30) / point
    if w60:
        m.high_60 = max(t.mid for t in w60)
        m.low_60 = min(t.mid for t in w60)
        before5 = [t for t in w60 if t.time < now - dt.timedelta(seconds=5)]
        if before5 and w5:
            h5 = max(t.mid for t in w5); l5 = min(t.mid for t in w5)
            hb = max(t.mid for t in before5); lb = min(t.mid for t in before5)
            m.swept_high = h5 > hb and m.mid < hb
            m.swept_low = l5 < lb and m.mid > lb
            running = hb; count_h = 0
            running_l = lb; count_l = 0
            for t in w5:
                if t.mid > running:
                    running = t.mid; count_h += 1
                if t.mid < running_l:
                    running_l = t.mid; count_l += 1
            m.new_extremes_5s = count_h if m.distance_5s >= 0 else count_l
    # the immediate direction: 1 s and 3 s agree and the tape agrees
    # a direction needs the tape leaning, not one more down-tick than up-tick
    d = 0
    if m.v(1.0) > 0 and m.v(3.0) > 0 and m.pressure >= 0.25:
        d = 1
    elif m.v(1.0) < 0 and m.v(3.0) < 0 and m.pressure <= -0.25:
        d = -1
    m.direction = d
    m.consistency = abs(m.pressure) if d else 0.0
    m.ok = True
    return m


# ------------------------------------------------------------------ scores --
def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


@dataclass
class Read:
    """What the engine makes of one market right now."""
    direction: int = 0
    setup: str = ""
    confidence: float = 0.0
    scalpability: float = 0.0
    contributions: dict = field(default_factory=dict)
    penalties: dict = field(default_factory=dict)
    blockers: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    expected_move_points: float = 0.0
    cost_points: float = 0.0
    invalidation: float = 0.0                       # the micro structural stop
    chase_points: float = 0.0
    micro: Optional[Micro] = None


def detect_setup(m: Micro, cfg: ScalperConfig, in_open_window: bool) -> tuple[str, int]:
    """Name the setup, or ('', 0)."""
    d = m.direction
    noise = max(m.noise_points, m.spread_points, 1e-9)
    c = cfg
    if d == 0:
        # a sweep reversal is the one setup that starts AGAINST the last move
        if m.swept_high and m.v(1.0) < 0 and m.pressure <= -c.min_consistency and m.accel_ratio <= -c.min_accel_ratio:
            return "SWEEP_REVERSAL", -1
        if m.swept_low and m.v(1.0) > 0 and m.pressure >= c.min_consistency and m.accel_ratio >= c.min_accel_ratio:
            return "SWEEP_REVERSAL", 1
        return "", 0
    if m.consistency < c.min_consistency:
        return "", 0
    if (m.swept_high and d < 0) or (m.swept_low and d > 0):
        return "SWEEP_REVERSAL", d
    if m.breakout_points * d > max(c.breakout_min_spreads * m.spread_points, 0.0) and m.range_prior_10s <= c.compression_max_noise * noise \
            and m.accel_ratio * d >= c.min_accel_ratio * 0.5:
        return "MICRO_BREAKOUT", d
    if m.distance_ratio >= c.launch_min_distance_ratio and c.launch_retrace_min <= m.retracement <= c.launch_retrace_max \
            and m.v(1.0) * d > 0 and m.move_points * d > 0:
        return "PULLBACK_LAUNCH", d
    if m.accel_ratio * d >= c.min_accel_ratio and m.distance_ratio >= c.accel_min_distance_ratio:
        return "ACCELERATION", d
    if in_open_window and m.consistency >= c.burst_min_consistency and m.distance_ratio >= c.burst_min_distance_ratio \
            and m.range_expansion >= c.burst_min_expansion:
        return "SESSION_BURST", d
    return "", 0


def read_market(m: Micro, cfg: ScalperConfig, *, commission_points: float, slippage_points: float,
                in_open_window: bool, session_bonus: float, threshold: float) -> Read:
    r = Read(micro=m)
    if not m.ok:
        r.blockers.append(m.reason or "no features")
        return r
    setup, d = detect_setup(m, cfg, in_open_window)
    if d == 0:
        r.blockers.append("no acceleration - movement without acceleration is not a trade")
        return r
    r.direction, r.setup = d, setup
    noise = max(m.noise_points, m.spread_points, 1e-9)
    w = cfg.velocity_weights
    # the evidence, each 0..1
    vq = _clamp(abs(m.v(1.0)) / (cfg.full_marks_velocity_noise_per_s * noise))
    aq = _clamp((m.accel_ratio * d) / cfg.full_marks_accel_ratio)
    cq = _clamp((m.consistency - cfg.min_consistency) / (1.0 - cfg.min_consistency)) if m.consistency >= cfg.min_consistency else 0.0
    xq = _clamp((m.range_expansion - 1.0) / 2.0)
    bq = _clamp((m.breakout_points * d) / (2.0 * noise)) if m.breakout_points * d > 0 else (0.5 if setup in ("PULLBACK_LAUNCH", "SWEEP_REVERSAL") else 0.0)
    lq = _clamp((m.ticks_per_second - cfg.min_ticks_per_second) / (3.0 * cfg.min_ticks_per_second))
    sq = _clamp(1.0 - max(0.0, m.spread_ratio - 1.0))
    nq = _clamp(m.new_extremes_5s / 4.0)
    # room: with the trend of the last minute the room is the move itself;
    # against it (a sweep), the room is back to the other side of the minute's range
    if setup == "SWEEP_REVERSAL":
        room_q = 0.6
    else:
        room_q = _clamp(1.0 - m.retracement) if setup != "PULLBACK_LAUNCH" else 0.7
    r.contributions = {
        "Velocity": round(w["velocity"] * vq, 1), "Acceleration": round(w["acceleration"] * aq, 1),
        "Consistency": round(w["consistency"] * cq, 1), "Volatility expansion": round(w["expansion"] * xq, 1),
        "Breakout quality": round(w["breakout"] * bq, 1), "Liquidity": round(w["liquidity"] * lq, 1),
        "Fresh highs/lows": round(w["fresh_extremes"] * nq, 1), "Room ahead": round(w["room"] * room_q, 1),
        "Spread": round(w["spread"] * sq, 1), "Session": round(w["session"] * _clamp(session_bonus), 1),
        "Order flow (no book over this feed)": 0.0,
    }
    # expected move: the usual 5-second move, scaled by how abnormal this one is, at least two noises
    r.expected_move_points = max(2.0 * noise, noise * min(m.distance_ratio, 4.0))
    r.cost_points = m.spread_points + commission_points + slippage_points
    # the chase test: how far from where the move began, against what is normal
    r.chase_points = abs(m.move_points)
    chase_limit = cfg.max_chase_noise * noise
    if r.chase_points > chase_limit and setup != "PULLBACK_LAUNCH":
        r.penalties["Chasing"] = -round(cfg.chase_penalty_max * _clamp((r.chase_points - chase_limit) / chase_limit), 1)
        if r.chase_points > chase_limit * 1.5:
            r.blockers.append(f"already moved {r.chase_points:.1f} points without a pullback (limit {chase_limit:.1f}): wait for one")
    if m.spread_ratio > cfg.spread_expansion_limit:
        r.penalties["Spread widening"] = -10.0
        r.blockers.append(f"spread has widened ({m.spread_ratio:.1f}x normal)")
    if m.retracement > cfg.launch_retrace_max and setup != "SWEEP_REVERSAL":
        r.penalties["Deep retracement"] = -10.0
    total = sum(r.contributions.values()) + sum(r.penalties.values())
    r.confidence = round(_clamp(total, 0.0, 100.0), 1)
    # scalpability: the realistic move against every cost, times the conditions that make seconds-trading sane
    edge = r.expected_move_points / max(r.cost_points, 1e-9)
    chop = 1.0 - 0.5 * _clamp((0.6 - m.consistency) / 0.6)
    r.scalpability = round(100.0 * _clamp((edge - 1.0) / (cfg.scalpable_edge_full - 1.0)) * _clamp(0.5 + 0.5 * lq)
                           * _clamp(0.6 + 0.4 * xq) * chop, 1)
    if m.ticks_per_second < cfg.min_ticks_per_second:
        r.blockers.append(f"too thin ({m.ticks_per_second:.1f} ticks/s)")
    if r.cost_points > cfg.max_cost_share_of_move * r.expected_move_points:
        r.blockers.append(f"costs {r.cost_points:.1f} points would eat {r.cost_points / r.expected_move_points:.0%} "
                          f"of the realistic {r.expected_move_points:.1f}-point move")
    if r.scalpability < cfg.min_scalpability:
        r.blockers.append(f"not scalpable here right now ({r.scalpability:.0f}/100, need {cfg.min_scalpability:.0f})")
    if r.confidence < threshold:
        r.blockers.append(f"momentum {r.confidence:.0f}/100 below {threshold:.0f} for this window")
    return r


def structural_stop(buf: TickBuffer, m: Micro, direction: int, cfg: ScalperConfig, point: float,
                    now: Optional[dt.datetime] = None) -> float:
    """Beyond the micro swing of the last ``stop_swing_seconds``, plus a buffer
    of spread and expected noise. Never inside ``min_stop_spreads`` spreads."""
    ticks = list(buf.ticks)
    now = now or (ticks[-1].time if ticks else None)
    w = _window(ticks, now, cfg.stop_swing_seconds) if now else ticks[-20:]
    if not w:
        return 0.0
    spread = m.spread_points * point
    buffer = max(cfg.stop_buffer_spreads * spread, cfg.stop_buffer_noise * m.noise_points * point)
    if direction > 0:
        swing = min(t.bid for t in w)
        stop = swing - buffer
        floor = m.mid - cfg.min_stop_spreads * spread
        return min(stop, floor)
    swing = max(t.ask for t in w)
    stop = swing + buffer
    ceiling = m.mid + cfg.min_stop_spreads * spread
    return max(stop, ceiling)
