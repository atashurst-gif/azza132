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
    symbols: tuple[str, ...] = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
                                "USDCHF", "NZDUSD", "EURJPY", "GBPJPY", "EURGBP",
                                "XAUUSD", "US500", "US30", "DE40", "UK100")
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
    min_confidence: float = 70.0
    max_spread_points: float = 25.0
    max_spread_fraction_of_expected_move: float = 0.35
    spread_expansion_limit: float = 1.8          # now / rolling average
    min_ticks_per_minute: float = 20.0           # liquidity floor
    chop_block: bool = True
    late_entry_penalty_max: float = 25.0
    min_seconds_between_entries: float = 20.0
    max_cost_fraction_of_risk: float = 0.25      # commission+spread+slippage <= 25% of the money at the stop
    min_expected_move_over_cost: float = 3.0     # the expected move must be >= 3x the round-trip cost
    cooldown_after_loss_seconds: float = 120.0   # no new trade anywhere for 2 min after a loss
    symbol_pause_after_loss_seconds: float = 600.0   # and none on THAT market for 10 min
    loss_streak_pause_after: int = 3             # 3 losses in a row: pause everything...
    loss_streak_pause_seconds: float = 1800.0    # ...for 30 min
    # --- management ------------------------------------------------------
    prove_it_seconds: float = 15.0               # the thesis must show within this
    prove_it_min_progress_r: float = 0.15        # else: THESIS_FAILED
    thesis_fail_adverse_r: float = 0.8           # mid this far against AND still moving against = exit early
    thesis_fail_stale_r: float = 0.5             # no progress, this far under AND moving against = exit
    thesis_fail_min_ticks_against: int = 4       # "moving against": velocity < 0 or this many ticks in a row
    risk_reduction_at_r: float = 0.6
    capital_safe_at_r: float = 0.8               # break-even after costs once 0.8 R is reached (2 Oct: 1.0)
    profit_protect_at_r: float = 1.6
    runner_at_r: float = 2.5
    giveback_early: float = 0.35                 # fraction of the high-water mark allowed back
    giveback_established: float = 0.45
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
    manage_interval_seconds: float = 0.25
    scan_interval_seconds: float = 1.0
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
        "US500": {"max_spread": 1.0, "min_stop": 1.5, "max_stop": 15.0},
        "US30": {"max_spread": 4.0, "min_stop": 6.0, "max_stop": 60.0},
        "DE40": {"max_spread": 3.0, "min_stop": 5.0, "max_stop": 50.0},
        "UK100": {"max_spread": 2.5, "min_stop": 4.0, "max_stop": 40.0},
    })

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
