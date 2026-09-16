"""Configuration.

Design rules:

* Risk boundaries are owned by the operator.  ``base_risk_pct`` and
  ``max_risk_pct`` are the only two numbers that decide how large a trade can
  be, and nothing in the codebase may exceed ``max_risk_pct``.
* ``live`` mode requires an explicit, persistent, deliberate marker.  A corrupt
  or missing config can only ever fall back to DEMO, never to LIVE.
* Secrets never live in source control.  They are read from the config file in
  the data directory (created by the setup command) or from the environment.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass, field, asdict, fields
from pathlib import Path
from typing import Any, Optional

CONFIG_VERSION = 1
LIVE_MARKER = "I-UNDERSTAND-THIS-TRADES-REAL-MONEY"


@dataclass
class RiskConfig:
    # --- operator-owned boundaries -------------------------------------------
    base_risk_pct: float = 0.50          # risk at NORMAL confidence
    max_risk_pct: float = 1.50           # hard ceiling; never exceeded
    min_risk_pct: float = 0.10
    # --- portfolio limits ----------------------------------------------------
    max_total_risk_pct: float = 4.00     # sum of open risk
    max_correlated_risk_pct: float = 2.00
    max_open_positions: int = 8
    max_positions_per_symbol: int = 1
    max_daily_loss_pct: float = 3.00
    max_drawdown_pct: float = 12.00
    min_margin_level_pct: float = 300.0
    # --- software-failure breakers -------------------------------------------
    max_rejects_per_10min: int = 6
    max_spread_multiple: float = 3.0     # vs typical spread
    # Round-trip commission per 1.0 lot in account currency. 0 means "learn it
    # from the broker's own deal records" (what IC Markets etc. actually
    # charged on this account); set it to override. Counted in every
    # reward:risk test, because on a 3-pip scalp it is most of the cost.
    commission_per_lot: float = 0.0
    max_orders_per_hour: int = 30
    # --- Kelly (conservative, capped) ---------------------------------------
    use_fractional_kelly: bool = True
    kelly_fraction: float = 0.25
    kelly_min_samples: int = 60

    def validate(self) -> list[str]:
        errs = []
        if self.base_risk_pct <= 0:
            errs.append("base_risk_pct must be > 0")
        if self.max_risk_pct < self.base_risk_pct:
            errs.append("max_risk_pct must be >= base_risk_pct")
        if self.max_risk_pct > 10.0:
            errs.append("max_risk_pct > 10% refused as a safety guard")
        if self.min_risk_pct > self.base_risk_pct:
            errs.append("min_risk_pct must be <= base_risk_pct")
        if self.max_total_risk_pct < self.max_risk_pct:
            errs.append("max_total_risk_pct must be >= max_risk_pct")
        if not (0 < self.kelly_fraction <= 0.5):
            errs.append("kelly_fraction must be in (0, 0.5]")
        return errs


@dataclass
class UniverseConfig:
    """Which instruments are allowed to compete.

    These are *groups*, not a whitelist of blessed symbols: every instrument the
    broker offers inside an enabled group is discovered automatically and
    competes on merit.  ``exclude_patterns`` exists only for junk symbols.
    """
    groups: tuple[str, ...] = ("FX_MAJOR", "FX_MINOR", "GOLD", "SILVER",
                               "INDEX", "ENERGY")
    max_symbols: int = 60
    min_history_bars: int = 400
    exclude_patterns: tuple[str, ...] = ("BTC", "ETH", ".b", "-2", "TEST")
    require_full_spec: bool = True


@dataclass
class ScanConfig:
    scan_interval_seconds: float = 5.0
    manage_interval_seconds: float = 1.0
    news_refresh_seconds: float = 120.0
    max_tick_age_seconds: float = 20.0
    max_tick_age_seconds_weekend: float = 4 * 3600.0
    top_n_reported: int = 12
    # Confidence tiers on the 0-100 opportunity scale.
    tier_normal: float = 60.0
    tier_strong: float = 70.0
    tier_exceptional: float = 82.0
    # --- half the pace, wiser choices ----------------------------------------
    # Day two's lesson: many small-edge trades lose to the broker's fees even
    # when half of them win. NORMAL setups are still traded, but only when the
    # realistic move is at least this many times the risk after every cost
    # (day one accepted 1.15), and at half day one's pace.
    #
    # 1.8 sat out a whole London afternoon, then found four trades in the
    # evening including a 3.3:1 index trade that ran to its target - the best
    # trade so far. Quiet spells are the rule doing its job.
    entry_tier: str = "NORMAL"
    min_reward_risk: float = 1.8
    cooldown_seconds_after_exit: float = 180.0
    cooldown_seconds_after_reject: float = 60.0
    # --- lessons from the first live day ------------------------------------
    # Round-trip costs (spread + slippage + commission) may not exceed this
    # fraction of the stop distance: a 3-pip stop with 0.9 pips of cost is a
    # coin toss that pays the broker whichever way it lands.
    max_cost_fraction_of_stop: float = 0.20
    # The stop is never closer than this many ATRs (decision timeframe) from
    # the entry, whatever the structure says: inside that band is noise, and
    # noise took out most of day one's trades within minutes.
    # Day three: 10 of 26 trades were over inside five minutes on a 0.5 ATR
    # floor. One full ATR keeps the stop outside the wobble; position size
    # shrinks to match, so the money at risk per trade is unchanged.
    stop_noise_floor_atr: float = 1.0
    # After a losing trade, no re-entry in the same direction on that market
    # for this long; and after this many losses on one market in a day, no
    # more entries there until tomorrow. Re-entry stays possible - churn does not.
    cooldown_seconds_after_loss: float = 1800.0
    max_losses_per_symbol_per_day: int = 2
    # No bursts: day one's new build opened eight trades in forty seconds at
    # the Asian open and lost six of them inside half an hour. One entry at a
    # time, spaced out, capped per hour, and none during the daily rollover
    # when spreads are at their widest (times are UTC, "HH:MM-HH:MM").
    # No count limits (operator's decision, day three): if six setups clear
    # the bar it takes six, if twenty then twenty. The quality guards above,
    # the risk breakers and the exposure limits are what say no. Only a
    # one-minute gap between orders remains, so one scan cannot fire the
    # same idea into eight markets in the same second. Caps stay available
    # for anyone who wants them (0 / empty = off).
    min_seconds_between_entries: float = 60.0
    max_new_positions_per_hour: int = 0
    max_new_positions_per_day: int = 0
    session_caps_utc: tuple[tuple[str, str, int], ...] = ()
    no_entry_utc_windows: tuple[str, ...] = ("21:45-23:30",)


@dataclass
class FlowLockConfig:
    initial_atr_multiple: float = 1.6
    proving_r: float = 0.5            # R multiple that ends INITIAL
    strong_flow_atr: float = 2.6      # breathing room while flow is strong
    normal_flow_atr: float = 1.5
    decay_atr: float = 0.7
    # Winners are given room: nothing is locked in until +1.5R, a third is
    # banked only at +2.5R and the rest is trailed loosely, so a real move
    # can pay for several small losses.
    breakeven_at_r: float = 1.5
    # Day three: winners averaged +0.7R because a 50% giveback trail clipped
    # them on the first pullback. Until +1R only the structural stop applies
    # (proving_trail=False); after that a trade may give back 40% of its
    # best gain, not half.
    proving_trail: bool = False
    mfe_giveback_normal: float = 0.40  # fraction of MFE we allow back
    mfe_giveback_strong: float = 0.65
    mfe_giveback_decay: float = 0.22
    stall_bars_to_decay: int = 20
    reversal_thesis_floor: float = 15.0
    # A structure break against the trade closes it only while it has not yet
    # travelled this far in our favour; later on, the trailed stop decides.
    structure_break_max_r: float = 0.3
    partial_enabled: bool = True
    partial_at_r: float = 2.5
    partial_fraction: float = 0.3
    min_stop_step_points: float = 1.0   # ignore microscopic improvements


@dataclass
class NewsConfig:
    enabled: bool = True
    use_mt5_calendar: bool = True
    # A public weekly calendar feed as a second source. Needed on macOS and
    # Linux: the MetaTrader5 Python package that the bridge uses has no
    # calendar API at all, so without this the bot trades on charts alone.
    public_calendar_feed: bool = True
    store_path: str = "data/calendar.sqlite"
    external_adapters: tuple[str, ...] = ()
    api_keys: dict[str, str] = field(default_factory=dict)
    max_staleness_seconds: float = 900.0
    blackout_before_seconds: float = 90.0    # no new entries just before
    blackout_after_seconds: float = 45.0     # first-spike chaos window
    second_wave_window_seconds: float = 900.0
    min_importance_for_blackout: int = 3
    allow_social_sources: bool = False


@dataclass
class OpsConfig:
    data_dir: str = "data"
    log_dir: str = "logs"
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8787
    heartbeat_stale_seconds: float = 45.0
    watchdog_interval_seconds: float = 15.0
    watchdog_restart_backoff_seconds: float = 20.0
    max_restarts_per_hour: int = 12
    min_free_disk_mb: float = 500.0
    max_memory_pct: float = 92.0
    max_clock_skew_seconds: float = 30.0


# Bump this whenever the trading rules change in a way that makes earlier
# results a different experiment. The status page resets to it automatically.
STRATEGY_VERSION = "2026-09-15 balanced - half the pace, wiser choices"


@dataclass
class Config:
    version: int = CONFIG_VERSION
    mode: str = "DEMO"                  # DEMO | LIVE
    live_marker: str = ""
    magic: int = 990_311
    # Where "since start" measurement begins (ISO UTC). Set by the installer
    # the first time the bot is set up; the status page measures the bot's
    # own trades from here using the broker's records.
    tracking_start_utc: str = ""
    # Which strategy the measurement belongs to. When the code's strategy
    # changes, the bot restarts the measuring clock itself on start-up, so the
    # status page only ever shows the CURRENT strategy's trades.
    tracking_strategy: str = ""
    account_login: int = 0
    account_password: str = ""
    account_server: str = ""
    mt5_terminal_path: str = ""
    # How the engine reaches MetaTrader 5.
    #   "direct" - this Python imports MetaTrader5 itself (Windows).
    #   "bridge" - MT5 and a small Windows Python run inside a Wine prefix and
    #              are reached over a localhost socket (macOS and Linux).
    broker_mode: str = "direct"
    bridge_host: str = "127.0.0.1"
    bridge_port: int = 8790
    wine_prefix: str = ""
    wine_python: str = ""
    # Full path to the wine binary. launchd starts the watchdog with a bare
    # PATH, so "wine" is not findable there unless we are told where it is.
    wine_binary: str = ""
    aggression: str = "NORMAL"          # CONSERVATIVE|NORMAL|AGGRESSIVE|MAXIMUM
    risk: RiskConfig = field(default_factory=RiskConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    scan: ScanConfig = field(default_factory=ScanConfig)
    flowlock: FlowLockConfig = field(default_factory=FlowLockConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    ops: OpsConfig = field(default_factory=OpsConfig)

    # ------------------------------------------------------------- live gate --
    @property
    def is_live(self) -> bool:
        """LIVE requires both the mode *and* the explicit marker.

        Two independent conditions mean neither a typo in ``mode`` nor a stale
        marker can move real money on its own.
        """
        return self.mode.upper() == "LIVE" and self.live_marker == LIVE_MARKER

    @property
    def effective_mode(self) -> str:
        return "LIVE" if self.is_live else "DEMO"

    # ----------------------------------------------------------- aggression --
    def aggression_scale(self) -> float:
        """Multiplier applied to the *confidence-to-risk* curve, not to the cap.

        MAXIMUM lets the engine reach ``max_risk_pct`` on merely STRONG evidence;
        it never lets it exceed ``max_risk_pct``, and it never disables a
        circuit breaker.
        """
        return {"CONSERVATIVE": 0.45, "NORMAL": 1.0,
                "AGGRESSIVE": 1.5, "MAXIMUM": 2.2}.get(self.aggression.upper(), 1.0)

    # ------------------------------------------------------------ validation --
    def validate(self) -> list[str]:
        errs = list(self.risk.validate())
        if self.mode.upper() not in ("DEMO", "LIVE"):
            errs.append(f"mode must be DEMO or LIVE, got {self.mode!r}")
        if self.mode.upper() == "LIVE" and self.live_marker != LIVE_MARKER:
            errs.append("mode=LIVE requires live_marker to be set exactly; "
                        "refusing to trade live")
        if self.aggression.upper() not in ("CONSERVATIVE", "NORMAL",
                                           "AGGRESSIVE", "MAXIMUM"):
            errs.append(f"unknown aggression {self.aggression!r}")
        if not self.universe.groups:
            errs.append("universe.groups is empty - nothing would be traded")
        if not (0 < self.scan.tier_normal < self.scan.tier_strong
                < self.scan.tier_exceptional <= 100):
            errs.append("confidence tiers must be strictly increasing")
        if self.magic <= 0:
            errs.append("magic must be a positive integer")
        if self.broker_mode not in ("direct", "bridge"):
            errs.append(f"broker_mode must be 'direct' or 'bridge', "
                        f"got {self.broker_mode!r}")
        if self.broker_mode == "bridge" and not (0 < self.bridge_port < 65536):
            errs.append("bridge_port must be a valid port number")
        return errs

    @property
    def bridge_token_file(self) -> str:
        return str(Path(self.ops.data_dir) / "bridge-token.txt")

    # ------------------------------------------------------------------- io --
    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("groups", "exclude_patterns", "external_adapters"):
            for sub in ("universe", "news"):
                if k in d.get(sub, {}):
                    d[sub][k] = list(d[sub][k])
        return d

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        d = self.to_dict()
        secrets = {"account_password": d.pop("account_password", "")}
        d["news"]["api_keys"] = {k: "" for k in d["news"].get("api_keys", {})}
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(d, indent=2, sort_keys=True))
        tmp.replace(path)
        # Secrets go to a sibling file with tight permissions.
        sec = path.parent / "secrets.json"
        payload = {"account_password": secrets["account_password"],
                   "api_keys": self.news.api_keys}
        sec.write_text(json.dumps(payload, indent=2))
        try:
            os.chmod(sec, 0o600)
        except OSError:
            pass

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        """Load, tolerating unknown/missing keys.

        A partially corrupt config yields defaults for the damaged sections and
        DEMO mode - the failure mode is always "trade nothing / trade demo",
        never "trade live by accident".
        """
        path = Path(path)
        if not path.exists():
            return cls()
        try:
            raw = json.loads(path.read_text())
            if not isinstance(raw, dict):
                raise ValueError("config root is not an object")
        except Exception:
            return cls()
        cfg = cls()
        nested = {"risk": RiskConfig, "universe": UniverseConfig,
                  "scan": ScanConfig, "flowlock": FlowLockConfig,
                  "news": NewsConfig, "ops": OpsConfig}
        for f in fields(cls):
            if f.name in nested:
                sub = raw.get(f.name)
                if isinstance(sub, dict):
                    setattr(cfg, f.name, _build(nested[f.name], sub))
            elif f.name in raw:
                try:
                    setattr(cfg, f.name, type(getattr(cfg, f.name))(raw[f.name]))
                except Exception:
                    pass
        sec = path.parent / "secrets.json"
        if sec.exists():
            try:
                s = json.loads(sec.read_text())
                cfg.account_password = str(s.get("account_password", "") or "")
                if isinstance(s.get("api_keys"), dict):
                    cfg.news.api_keys.update(
                        {str(k): str(v) for k, v in s["api_keys"].items()})
            except Exception:
                pass
        cfg.account_password = os.environ.get("MINTEL_PASSWORD",
                                              cfg.account_password)
        if cfg.mode.upper() == "LIVE" and cfg.live_marker != LIVE_MARKER:
            cfg.mode = "DEMO"          # fail safe, loudly, downstream
        return cfg


def _build(kls, raw: dict):
    obj = kls()
    for f in fields(kls):
        if f.name not in raw:
            continue
        cur = getattr(obj, f.name)
        val = raw[f.name]
        try:
            if isinstance(cur, tuple):
                setattr(obj, f.name, tuple(val))
            elif isinstance(cur, bool):
                setattr(obj, f.name, bool(val))
            elif isinstance(cur, dict):
                setattr(obj, f.name, dict(val))
            else:
                setattr(obj, f.name, type(cur)(val))
        except Exception:
            pass
    return obj
