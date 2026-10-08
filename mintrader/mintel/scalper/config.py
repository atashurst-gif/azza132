"""Rapid Scalper configuration - its own file, never config.json.

    data/scalper.json

Mode is OFF / PAPER / LIVE and defaults to PAPER. Going LIVE is a deliberate
edit of that file; a restart never changes it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

MODES = ("OFF", "PAPER", "LIVE")


@dataclass
class ScoreWeights:
    """Maximum points per component; all weights are configurable and logged."""
    momentum: float = 22.0
    trend_alignment: float = 14.0
    entry_quality: float = 16.0
    spread: float = 12.0
    liquidity: float = 10.0
    volume: float = 12.0
    volatility: float = 8.0
    structure: float = 6.0
    order_flow: float = 0.0        # no genuine DOM/L2 over this feed: disabled


@dataclass
class ScalperConfig:
    mode: str = "PAPER"
    strategy_id: str = "rapid_scalper"
    magic: int = 990_411                   # NEVER the existing bot's magic
    # Gold and the commission-free indices only. 6 Oct review: 8 FX scalps
    # made +9.87 on price and paid 22.20 commission - on a one-minute
    # horizon the commission is bigger than the move.
    symbols: tuple[str, ...] = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "US500", "US30", "USTEC", "DE40", "UK100")
    # ---- version 3, "High-Velocity Session Rider" (7 Oct) ----------------
    # How it enters. "VELOCITY" (7 Oct, the default): seconds-level
    # acceleration in a market that is scalpable right now, within session
    # windows; see velocity.py, rider.py, sessions.py and
    # docs/strategies/rapid_scalper.md. The older styles are kept so the
    # three can be compared on the same ticks.
    # "PULLBACK" (6 Oct): trade WITH the 20-minute trend, wait
    # for a one-minute pullback towards the fast EMA, enter when price takes
    # out the last one-minute bar in the trend's direction, stop beyond the
    # pullback. "BURST" is the original: chase a 5-second spike with a stop
    # at the last minute's tick extreme (82 trades, -67.15; 29 of the 32
    # trades it cut early never got even 0.1 R into profit).
    entry_style: str = "VELOCITY"
    # sessions (London local time, DST-aware): the opening window is aggressive
    # ONLY when the market is measured to be moving; quiet hours set a higher bar
    session_windows: tuple = ()                  # empty = sessions.default_windows()
    opening_bonus: float = 10.0                  # momentum bar lowered by this in a moving opening window
    quiet_penalty: float = 15.0                  # ...raised by this outside every window
    session_moving_ratio: float = 1.3            # last 5 min against the typical minute of the hour
    # micro-momentum engine thresholds (all in units of the market's own noise)
    min_consistency: float = 0.6                 # share of the last 3 s of ticks going our way
    min_accel_ratio: float = 1.0                 # (v1s - v5s) against noise per second
    accel_min_distance_ratio: float = 1.5        # the 5 s move against the usual 5 s move
    breakout_min_spreads: float = 1.0
    compression_max_noise: float = 1.5           # the prior 10 s range, in noises, for a micro breakout
    launch_min_distance_ratio: float = 2.5
    launch_retrace_min: float = 0.2
    launch_retrace_max: float = 0.6
    burst_min_consistency: float = 0.75
    burst_min_distance_ratio: float = 2.0
    burst_min_expansion: float = 1.5
    min_ticks_per_second: float = 0.8
    full_marks_velocity_noise_per_s: float = 0.6
    full_marks_accel_ratio: float = 3.0
    max_chase_noise: float = 3.0                 # how far the move may already have run, in noises
    chase_penalty_max: float = 25.0
    # scalpability: the realistic move against every cost
    scalpable_edge_full: float = 5.0             # expected move / cost for full marks
    max_cost_share_of_move: float = 0.35
    min_scalpability: float = 50.0
    velocity_weights: dict = field(default_factory=lambda: {
        "velocity": 20.0, "acceleration": 20.0, "consistency": 15.0, "expansion": 10.0, "breakout": 10.0,
        "liquidity": 8.0, "fresh_extremes": 5.0, "room": 5.0, "spread": 4.0, "session": 3.0})
    # the micro structural stop
    stop_swing_seconds: float = 10.0
    stop_buffer_spreads: float = 1.0
    stop_buffer_noise: float = 0.5
    # survival and the time stop
    validate_seconds: float = 3.0
    validate_fail_r: float = 0.25
    collapse_pressure: float = 0.5
    collapse_fail_r: float = 0.15
    max_no_progress_seconds: float = 25.0
    no_progress_min_r: float = 0.3
    # protection and riding (R = the planned loss)
    protect_at_r: float = 0.6
    ride_at_r: float = 1.5
    ride_min_momentum: float = 55.0
    trail_strong_momentum: float = 75.0
    floor_ladder_r: tuple = ((0.6, 0.0), (1.0, 0.3), (2.0, 1.0), (3.0, 1.8), (4.0, 2.6), (6.0, 4.0), (8.0, 5.5))
    giveback_by_state: dict = field(default_factory=lambda: {"PROFIT_PROTECTION": 0.5, "MOMENTUM_RIDE": 0.6, "MAXIMUM_RIDE": 0.75})
    trail_weak_noise: float = 0.6
    trail_normal_noise: float = 1.2
    trail_strong_noise: float = 2.0
    trail_max_ride_noise: float = 3.0
    min_trail_spreads: float = 1.5
    collapse_momentum: float = 30.0
    opposing_burst_ratio: float = 2.0
    stall_seconds: float = 12.0
    # maximum ride
    max_ride_momentum: float = 85.0
    max_ride_exit_momentum: float = 60.0
    max_ride_max_retrace: float = 0.2
    max_ride_min_fresh: int = 3
    # consecutive-loss ladder: size multiplier and extra bar per loss in a row; never martingale
    loss_ladder_size: tuple = (1.0, 1.0, 0.75, 0.5)
    loss_ladder_threshold: tuple = (0.0, 0.0, 5.0, 10.0)
    max_session_loss_gbp: float = 25.0
    pullback_touch_atr: float = 0.35             # the pullback came within this of the fast EMA...
    pullback_max_depth_atr: float = 1.0          # ...without going this far past the slow EMA
    stop_buffer_atr: float = 0.15                # the stop sits this far beyond the pullback extreme
    min_room_r: float = 1.0                      # room to the last 20-minute high/low, in R
    # --- risk ------------------------------------------------------------
    planned_max_trade_risk_gbp: float = 10.0      # a CEILING, not a target
    slippage_allowance_points: float = 3.0        # per side, in points
    commission_per_lot_round_turn: float = 6.0    # account currency, configurable
    invalidation_buffer_spreads: float = 1.0      # invalidation sits this many spreads past the micro swing
    min_stop_points: float = 20.0
    min_stop_spreads: float = 4.0                # and never inside 4 spreads: the spread is not a signal
    max_stop_points: float = 120.0
    max_open_positions: int = 1
    # --- entry gates -----------------------------------------------------
    min_confidence: float = 60.0
    max_spread_points: float = 25.0
    max_spread_fraction_of_expected_move: float = 0.35
    spread_expansion_limit: float = 1.8          # now / rolling average
    min_ticks_per_minute: float = 20.0           # liquidity floor
    chop_block: bool = True
    late_entry_penalty_max: float = 25.0
    min_seconds_between_entries: float = 10.0
    symbol_reentry_seconds: float = 20.0        # a market rests briefly after any exit
    # Look before moving: the last 1, 5 and 20 minutes must all point the
    # way of the trade (Aaron, 5 Oct). False switches the check off.
    require_timeframe_alignment: bool = True
    # Hours (UTC, 0-23) in which new trades may open. Empty = every hour.
    # 5 Oct: gold, 00:00-00:59 UTC: Fri 6 trades +17.34, Mon 6 trades +74.38;
    # every other hour of the three live days: 57 trades -128. Two days is
    # not proof, so PAPER trades every hour to measure it; going LIVE in
    # that hour alone is one line in scalper.json: "entry_hours_utc": [0].
    entry_hours_utc: tuple[int, ...] = ()
    max_cost_fraction_of_risk: float = 0.25      # commission+spread+slippage <= 25% of the money at the stop
    min_expected_move_over_cost: float = 3.0     # the expected move must be >= 3x the round-trip cost
    cooldown_after_loss_seconds: float = 30.0    # no new trade anywhere for 30 s after a loss
    symbol_pause_after_loss_seconds: float = 120.0   # and none on THAT market for 2 min
    loss_streak_pause_after: int = 4             # 4 losses in a row: pause everything...
    loss_streak_pause_seconds: float = 1800.0    # ...for 30 min
    # --- management ------------------------------------------------------
    prove_it_seconds: float = 15.0               # the thesis must show within this
    prove_it_min_progress_r: float = 0.15        # else: THESIS_FAILED
    # The tick-speed cuts (early_tick_cut) closed 32 trades for -182 within
    # seconds, inside the normal wobble of a one-minute pullback. Off: the
    # stop beyond the pullback, the context cut and the follow-through
    # check do that job on the one-minute picture.
    early_tick_cut: bool = False
    context_cut_r: float = 0.4                   # under this AND the 1- and 5-minute moves against: out
    no_follow_through_seconds: float = 300.0     # 5 min without reaching...
    no_follow_through_peak_r: float = 0.5        # ...+0.5 R, and under water: out
    thesis_fail_adverse_r: float = 0.6           # mid this far against AND still moving against = exit early (5 Oct: cut losers sooner)
    thesis_fail_stale_r: float = 0.5             # no progress, this far under AND moving against = exit
    thesis_fail_min_ticks_against: int = 4       # "moving against": velocity < 0 or this many ticks in a row
    risk_reduction_at_r: float = 0.6
    capital_safe_at_r: float = 0.8               # break-even after costs once 0.8 R is reached (2 Oct: 1.0)
    profit_protect_at_r: float = 1.6
    runner_at_r: float = 2.5
    giveback_early: float = 0.50                 # fraction of the high-water mark allowed back (5 Oct: ride highs)
    giveback_established: float = 0.50
    giveback_runner: float = 0.60
    runner_min_probability: float = 60.0
    enable_partial_profit: bool = False
    partial_at_r: float = 2.0
    partial_fraction: float = 0.5
    momentum_decay_velocity_ratio: float = -0.4  # velocity now vs at peak
    # --- execution -------------------------------------------------------
    order_type: str = "MARKET"                   # MARKET or LIMIT
    limit_offset_points: float = 1.0
    filling: str = "IOC"
    paper_slippage_points: float = 1.0
    paper_latency_ms: float = 150.0
    # --- circuit breakers --------------------------------------------------
    max_consecutive_losses: int = 6              # 6 in a row: done for the day
    max_daily_loss_gbp: float = 40.0
    max_account_daily_loss_gbp: float = 80.0
    max_total_open_risk_gbp: float = 60.0
    min_margin_level_pct: float = 300.0
    max_avg_slippage_points: float = 4.0         # absolute floor, in points...
    max_avg_slippage_spreads: float = 1.5        # ...but never stricter than 1.5 spreads (gold has 0.01 points)
    slippage_window_trades: int = 10
    slippage_pause_seconds: float = 900.0        # bad fills: rest 15 min, then measure afresh
    stale_tick_seconds: float = 5.0
    max_latency_ms: float = 1500.0
    max_api_errors_per_hour: int = 10
    max_rejects_per_hour: int = 5
    # --- news ------------------------------------------------------------
    news_filter_enabled: bool = True
    news_blackout_before_seconds: float = 300.0
    news_blackout_after_seconds: float = 180.0
    news_min_importance: int = 3
    # --- loop ------------------------------------------------------------
    manage_interval_seconds: float = 0.2
    scan_interval_seconds: float = 0.5
    status_file: str = "scalper-status.json"
    status_interval_seconds: float = 2.0         # the page refreshes every 10 s; no need for more
    broker_day_cache_seconds: float = 30.0
    rejected_log_seconds: float = 60.0           # one row per market per reason per minute
    journal_file: str = "scalper.sqlite"
    weights: ScoreWeights = field(default_factory=ScoreWeights)
    # Per-market limits in PRICE units (index points, not broker points).
    # 4 Oct: the point-based limits above meant a 0.25 index-point spread cap
    # and a 1.2 index-point maximum stop on US30/US500/DE40/UK100, where a
    # point is 0.01 - so the commission-free indices could never trade.
    # Markets not listed here use the point-based limits above.
    symbol_limits: dict = field(default_factory=lambda: {
        "US500": {"max_spread": 1.0, "min_stop": 1.5, "max_stop": 15.0, "commission": 0.0},
        "US30": {"max_spread": 4.0, "min_stop": 6.0, "max_stop": 60.0, "commission": 0.0},
        "DE40": {"max_spread": 3.0, "min_stop": 5.0, "max_stop": 50.0, "commission": 0.0},
        "UK100": {"max_spread": 2.5, "min_stop": 4.0, "max_stop": 40.0, "commission": 0.0},
        # gold: a one-minute pullback is $1-5; the old 1.20 maximum stop
        # forced stops inside the noise (6 Oct)
        "XAUUSD": {"max_spread": 0.30, "min_stop": 0.40, "max_stop": 6.0},
    })

    def commission_for(self, symbol: str) -> float:
        """Round-trip commission per lot for this market (indices: none)."""
        lim = (self.symbol_limits or {}).get(str(symbol).upper()) or {}
        if "commission" in lim:
            return float(lim["commission"])
        return float(self.commission_per_lot_round_turn)

    def limits_points(self, symbol: str, point: float) -> tuple[float, float, float]:
        """(max spread, min stop, max stop) in this market's broker points."""
        lim = (self.symbol_limits or {}).get(str(symbol).upper())
        if lim and point and point > 0:
            return (float(lim["max_spread"]) / point, float(lim["min_stop"]) / point,
                    float(lim["max_stop"]) / point)
        return float(self.max_spread_points), float(self.min_stop_points), float(self.max_stop_points)

    # ----------------------------------------------------------------- io --
    @property
    def enabled(self) -> bool:
        return self.mode in ("PAPER", "LIVE")

    @property
    def is_live(self) -> bool:
        return self.mode == "LIVE"

    @classmethod
    def load(cls, path: str | Path) -> "ScalperConfig":
        p = Path(path)
        cfg = cls()
        if not p.exists():
            return cfg
        try:
            raw = json.loads(p.read_text())
        except Exception:
            return cfg
        if not isinstance(raw, dict):
            return cfg
        for k, v in raw.items():
            if k == "weights" and isinstance(v, dict):
                for wk, wv in v.items():
                    if hasattr(cfg.weights, wk):
                        setattr(cfg.weights, wk, float(wv))
            elif k == "symbols" and isinstance(v, (list, tuple)):
                cfg.symbols = tuple(str(s) for s in v)
            elif k == "entry_hours_utc" and isinstance(v, (list, tuple)):
                cfg.entry_hours_utc = tuple(int(h) % 24 for h in v)
            elif k in ("floor_ladder_r",) and isinstance(v, (list, tuple)):
                try:
                    cfg.floor_ladder_r = tuple((float(a), float(b)) for a, b in v)
                except Exception:
                    pass                                   # a malformed ladder keeps the default
            elif k in ("loss_ladder_size", "loss_ladder_threshold") and isinstance(v, (list, tuple)):
                try:
                    setattr(cfg, k, tuple(float(x) for x in v))
                except Exception:
                    pass
            elif k in ("velocity_weights", "giveback_by_state") and isinstance(v, dict):
                try:
                    getattr(cfg, k).update({str(a): float(b) for a, b in v.items()})
                except Exception:
                    pass
            elif hasattr(cfg, k) and k != "weights":
                cur = getattr(cfg, k)
                try:
                    setattr(cfg, k, type(cur)(v) if not isinstance(cur, bool) else bool(v))
                except Exception:
                    pass
        cfg.mode = str(cfg.mode).upper()
        if cfg.mode not in MODES:
            cfg.mode = "OFF"            # an unknown mode fails closed
        return cfg

    @staticmethod
    def save_minimal(path: str | Path, mode: str) -> None:
        """Write a file holding ONLY the mode.

        A full dump of every setting would freeze today's defaults on the
        Mac, so later builds could never change them. Settings only go in
        this file when a person puts them there, with "keep_overrides": true.
        """
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"mode": mode}, indent=2) + "\n")

    @staticmethod
    def normalise_file(path: str | Path) -> bool:
        """Collapse a bot-written full dump back to just the mode.

        Returns True when the file was rewritten. A file carrying
        "keep_overrides": true is left exactly as it is."""
        p = Path(path)
        try:
            raw = json.loads(p.read_text())
        except Exception:
            return False
        if not isinstance(raw, dict) or raw.get("keep_overrides"):
            return False
        extra = [k for k in raw if k != "mode"]
        if not extra:
            return False
        ScalperConfig.save_minimal(p, str(raw.get("mode", "PAPER")).upper())
        return True

    def save(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        d = asdict(self)
        d["symbols"] = list(self.symbols)
        p.write_text(json.dumps(d, indent=2))

    @staticmethod
    def default_path(data_dir: str | Path) -> Path:
        return Path(data_dir) / "scalper.json"
