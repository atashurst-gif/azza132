"""Rapid Momentum Rider settings - its own file, never config.json.

    data/rider.json

A missing file gives the defaults. A malformed file gives the defaults. A
malformed ENTRY keeps that entry's default and every other entry still
loads. The mode is OFF / PAPER / LIVE and defaults to PAPER; the installer
writes LIVE. ``user_pip_value_gbp`` is Aaron's exposure setting (about GBP 1
per pip): the bot reads it and never changes it, in memory or on disk.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

MODES = ("OFF", "PAPER", "LIVE")


@dataclass
class RiderConfig:
    mode: str = "PAPER"
    # ---- exposure: Aaron's setting, never changed by the bot ---------------
    user_pip_value_gbp: float = 1.0
    # ---- markets ----------------------------------------------------------
    # empty = every liquid FX pair the broker offers (the 28 pairs of USD EUR
    # GBP JPY CHF CAD AUD NZD that it can size). No permanent bans: history
    # only changes confidence (the HISTORICAL MATCH category).
    symbols: tuple[str, ...] = ()
    # ---- costs ------------------------------------------------------------
    # IC Markets Raw: commission per 1.0 lot per side, in commission_currency,
    # converted to the account currency through the broker's own tick values.
    # Replaced by the figure learned from the broker's deals once one is known.
    commission_per_lot_side: float = 3.5
    commission_currency: str = "USD"
    learn_commission_from_deals: bool = True
    slippage_pips: float = 0.2                 # per side, in pips: the cost model and the breakeven buffer
    latency_ms: float = 250.0                  # replay only: signal -> fill delay the research uses
    # ---- loop -------------------------------------------------------------
    scan_interval_seconds: float = 1.0         # the decisions depend on this cadence (replay uses the same)
    status_interval_seconds: float = 2.0
    tick_history: bool = True                  # every tick since the last pass (ticks_range), as replay sees them
    specs_refresh_seconds: float = 300.0
    calendar_refresh_seconds: float = 300.0
    seed_bars_m1: int = 400
    seed_bars_m5: int = 400
    # ---- positions ----------------------------------------------------------
    cooldown_seconds: float = 20.0             # against duplicates only; a fresh trigger is still required
    max_concurrent_positions: int = 4          # a sanity cap; one position per market always
    # ---- guards added on 9 Oct (RiderCore.guard_hold; live and replay alike) ---------------------
    # They only ever REFUSE a new entry: never a size change, never a score, threshold, gate or stop.
    # Currency cluster: short EURJPY and short GBPJPY are both long JPY - one idea, not two.
    currency_cluster_guard: bool = True        # refuse an entry that adds to an open position's currency side
    max_same_currency_positions: int = 1       # ... beyond this many positions long (or short) one currency
    # Loss cooldown: after losing exits (net of every cost), wait.
    loss_cooldown_guard: bool = True
    loss_cooldown_minutes: float = 15.0        # a losing exit on a market: no new entry there for this long
    loss_pause_minutes: float = 120.0          # two losing exits in a row on a market: none there for this long
    loss_cooldown_same_currency: bool = True   # a losing exit also rests its currency sides (long JPY, short EUR)
    loss_brake_losses: int = 4                 # the global brake: this many losing exits ...
    loss_brake_window_minutes: float = 10.0    # ... within this many minutes ...
    loss_brake_minutes: float = 15.0           # ... and no new entry anywhere for this long
    # ---- entry states -------------------------------------------------------
    building_score: float = 40.0
    ready_score: float = 55.0
    trigger_score: float = 60.0
    state_hysteresis: float = 5.0
    max_cost_ratio: float = 0.15               # (spread + commission + slippage) / expected available move; 0.25 until 9 Oct (Aaron: wins were being eaten by commission)
    expected_run_atr: float = 3.0              # the expected available move: the largest of these three,
    expected_run_atr5: float = 1.5             # capped by the open space to the next level (headroom)
    expected_run_impulse: float = 1.0
    max_chop: float = 0.6                      # anti-chop score at or over this: WAIT
    min_headroom_atr: float = 1.2              # open space to the next level, in one-minute ATRs
    max_spread_atr: float = 0.5                # the spread itself against the one-minute ATR
    trigger_break_seconds: float = 20.0        # the micro high/low that a trigger must take out
    trigger_velocity_seconds: float = 5.0      # ... with the last few seconds moving the same way
    trigger_min_break_atr: float = 0.05
    min_warm_m1_bars: int = 30
    min_ticks_per_minute: float = 6.0
    # ---- news ---------------------------------------------------------------
    news_min_importance: int = 2
    news_pre_block_seconds: float = 120.0      # no new entry just before high-impact news for that currency
    news_settle_seconds: float = 20.0          # never chase the first tick after a release
    news_window_minutes: float = 15.0
    # ---- initial stop ---------------------------------------------------------
    stop_swing_seconds: float = 60.0
    stop_buffer_atr: float = 0.15
    stop_min_atr: float = 0.6
    stop_min_spreads: float = 3.0
    stop_max_atr: float = 3.0
    stop_max_pips: float = 30.0
    # ---- stop moves sent to the broker (RiderCore.stop_move_due; live and replay alike) --
    stop_min_step_pips: float = 0.3            # a move must gain at least this many pips ...
    stop_min_step_spreads: float = 1.0         # ... and at least this many spreads
    stop_min_modify_seconds: float = 5.0       # and come this long after the last move sent (or refused)
    # ---- FlowLock X and scoring overrides (name -> value) -------------------
    flowlock: dict = field(default_factory=dict)
    weights: dict = field(default_factory=dict)
    # ---- learning -------------------------------------------------------------
    learning_min_samples: int = 30
    learning_window_trades: int = 600
    learning_min_days: int = 3
    # ---- health ---------------------------------------------------------------
    stale_tick_seconds: float = 30.0
    max_rejects_per_10min: int = 5
    min_free_disk_mb: float = 200.0
    min_free_memory_mb: float = 150.0
    max_spread_valid_fraction: float = 0.5     # under half the markets with a valid spread is a failure
    status_file: str = "rider-status.json"
    journal_file: str = "rider.sqlite"
    scan_snapshot_seconds: float = 60.0

    # ------------------------------------------------------------------ io --
    @property
    def enabled(self) -> bool:
        return self.mode in ("PAPER", "LIVE")

    @classmethod
    def from_dict(cls, raw: Any) -> "RiderConfig":
        cfg = cls()
        if not isinstance(raw, dict):
            return cfg
        types = {f.name: f.type for f in fields(cls)}
        for k, v in raw.items():
            if k not in types:
                continue
            cur = getattr(cfg, k)
            try:
                if k == "mode":
                    m = str(v).strip().upper()
                    if m in MODES:
                        cfg.mode = m
                elif k == "symbols":
                    if isinstance(v, (list, tuple)):
                        cfg.symbols = tuple(str(s).strip() for s in v if str(s).strip())
                elif isinstance(cur, bool):
                    if isinstance(v, bool):
                        setattr(cfg, k, v)
                elif isinstance(cur, int):
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        setattr(cfg, k, int(v))
                elif isinstance(cur, float):
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        fv = float(v)
                        if fv == fv and abs(fv) != float("inf"):
                            setattr(cfg, k, fv)
                elif isinstance(cur, str):
                    if isinstance(v, str):
                        setattr(cfg, k, v)
                elif isinstance(cur, dict):
                    if isinstance(v, dict):
                        setattr(cfg, k, {str(a): b for a, b in v.items() if isinstance(b, (int, float, str, bool))})
            except Exception:
                pass                                        # a malformed entry keeps its default
        if not (cfg.user_pip_value_gbp > 0):
            cfg.user_pip_value_gbp = cls.user_pip_value_gbp
        if cfg.max_concurrent_positions < 1:
            cfg.max_concurrent_positions = 1
        if cfg.max_same_currency_positions < 1:
            cfg.max_same_currency_positions = 1
        if cfg.loss_brake_losses < 1:
            cfg.loss_brake_losses = cls.loss_brake_losses
        for k in ("loss_cooldown_minutes", "loss_pause_minutes", "loss_brake_window_minutes", "loss_brake_minutes"):
            if getattr(cfg, k) < 0:
                setattr(cfg, k, getattr(cls, k))
        return cfg

    @classmethod
    def load(cls, path: str | Path) -> "RiderConfig":
        p = Path(path)
        if not p.exists():
            return cls()
        try:
            raw = json.loads(p.read_text())
        except Exception:
            return cls()
        return cls.from_dict(raw)

    @staticmethod
    def save_minimal(path: str | Path, mode: str, user_pip_value_gbp: float | None = None) -> None:
        """Write a file holding ONLY the mode (and the GBP per pip when a
        person or the installer gives it). A full dump would freeze today's
        defaults on the machine so later builds could never change them."""
        m = str(mode).upper()
        if m not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        d: dict = {"mode": m}
        if user_pip_value_gbp is not None:
            d["user_pip_value_gbp"] = float(user_pip_value_gbp)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, indent=2) + "\n")
        tmp.replace(p)

    @staticmethod
    def default_path(data_dir: str | Path) -> Path:
        return Path(data_dir) / "rider.json"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["symbols"] = list(self.symbols)
        return d
