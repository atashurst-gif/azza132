"""The coach (conductor): the shared contract every part of it reads.

Build spec: ``docs/conductor/spec.md`` (12 Oct 2026). Section 21, Aaron's
"no limits on the demo" instruction, overrides the earlier sections where they
conflict: on a DEMO account the coach may lift limits step by step as the
evidence earns it; on a real-money account the v1 values below always apply.

This module holds no logic that trades. It is the one place for:

* the paths under ``data/`` (the coach writes only under ``data/coach/``;
  ``data/coach.json``, ``data/HALT`` and ``data/COACH-OFF`` are Aaron's);
* the frozen constants and thresholds of sections 4-8, 10, 13, 17 and 21
  (the coach never tunes them; changing one takes an install);
* the step ladders (v1 and demo);
* the policy file's schema version and the dataclasses the writer (the coach
  process), the reader (``gate.py``, inside each bot) and the page share:
  :class:`Key`, :class:`Envelope`, :class:`Pin`, :class:`Entry`,
  :class:`Floor`, :class:`Block`, :class:`Gates`, :class:`DemoLimits`,
  :class:`Judgement`, :class:`Policy`, :class:`Decision`, :class:`RiskCtx`
  and :class:`Limits`.

Written by Builder P; Builders E, D and G read it and never edit it.

Imports: the standard library only, so the gate can be imported by every bot
(Trend & Breakout's trader, Financial Ian) without pulling the coach's own
code into a trading process.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from zoneinfo import ZoneInfo

# =====================================================================
# Schema versions
# =====================================================================
SCHEMA_VERSION = 1                  # data/coach/policy.json
ENVELOPE_SCHEMA = 1                 # data/coach.json
WEATHER_SCHEMA = 1                  # data/coach/weather.json
STATUS_SCHEMA = 1                   # data/coach/status.json
CLASSIFIER_VERSION = 1              # the market-condition labels the evidence was recorded with (epochs.py)

# =====================================================================
# Paths (all relative to cfg.ops.data_dir, "data" on the Mac)
# =====================================================================
COACH_DIR = "coach"                         # data/coach/: the only folder the coach writes
ENVELOPE_NAME = "coach.json"                # data/coach.json: Aaron's envelope (installer and his CLI only)
HALT_NAME = "HALT"                          # data/HALT: Aaron's - no new real-money entries anywhere
COACH_OFF_NAME = "COACH-OFF"                # data/COACH-OFF: Aaron's - every bot on his own settings
SECRETS_NAME = "secrets.json"               # data/secrets.json: never opened by the coach
POLICY_NAME = "policy.json"                 # data/coach/policy.json: the coach's one instruction to the bots
POLICY_PREV_NAME = "policy.prev.json"       # the previous policy, kept for the fallback (step 6 of the gate)
POLICY_TMP_NAME = "policy.json.tmp"         # written, fsynced, then os.replace'd over policy.json
WEATHER_NAME = "weather.json"               # written by the trader every 60 s (the market board, account_demo)
STATUS_NAME = "status.json"                 # the coach's status (the watchdog probes its freshness; no heartbeat)
LEDGER_NAME = "ledger.sqlite"               # the append-only ledger (Builder D)
APPROVALS_NAME = "approvals.json"           # Aaron's approve/veto (his CLI only)
GATE_LOG_DIR = "gate"                       # data/coach/gate/<bot>-<UK date>.jsonl: one line per real decision
PLAN_PATTERN = "plan-{date}.json"           # tomorrow's game plan (nightly)
REPORT_PATTERN = "report-{date}.md"         # the nightly digest


@dataclass(frozen=True)
class Paths:
    """Every file the coach and the gate name, from the data folder."""
    data_dir: Path

    @classmethod
    def of(cls, data_dir: "str | Path") -> "Paths":
        return cls(Path(data_dir))

    @property
    def coach_dir(self) -> Path:
        return self.data_dir / COACH_DIR

    @property
    def envelope(self) -> Path:
        return self.data_dir / ENVELOPE_NAME

    @property
    def halt(self) -> Path:
        return self.data_dir / HALT_NAME

    @property
    def coach_off(self) -> Path:
        return self.data_dir / COACH_OFF_NAME

    @property
    def policy(self) -> Path:
        return self.coach_dir / POLICY_NAME

    @property
    def policy_prev(self) -> Path:
        return self.coach_dir / POLICY_PREV_NAME

    @property
    def policy_tmp(self) -> Path:
        return self.coach_dir / POLICY_TMP_NAME

    @property
    def weather(self) -> Path:
        return self.coach_dir / WEATHER_NAME

    @property
    def status(self) -> Path:
        return self.coach_dir / STATUS_NAME

    @property
    def ledger(self) -> Path:
        return self.coach_dir / LEDGER_NAME

    @property
    def approvals(self) -> Path:
        return self.coach_dir / APPROVALS_NAME

    @property
    def gate_dir(self) -> Path:
        return self.coach_dir / GATE_LOG_DIR

    def gate_log(self, bot: str, day: "dt.date | str") -> Path:
        day = day.isoformat() if isinstance(day, dt.date) else str(day)
        return self.gate_dir / f"{bot}-{day}.jsonl"

    def plan(self, day: "dt.date | str") -> Path:
        day = day.isoformat() if isinstance(day, dt.date) else str(day)
        return self.coach_dir / PLAN_PATTERN.format(date=day)

    def report(self, day: "dt.date | str") -> Path:
        day = day.isoformat() if isinstance(day, dt.date) else str(day)
        return self.coach_dir / REPORT_PATTERN.format(date=day)


# =====================================================================
# Time: UK days and the market clocks
# =====================================================================
UTC = dt.timezone.utc
TZ_UK = ZoneInfo("Europe/London")
TZ_NY = ZoneInfo("America/New_York")
TZ_TOKYO = ZoneInfo("Asia/Tokyo")

NIGHTLY_UK = "22:20"                 # the nightly review (after the 22:10 upload, before the lab at 23:10)
RELEASE_UK = "06:30"                 # grants and steps from last night's plan take effect (UK trading days)
TICK_SECONDS = 60.0                  # the coach's tick, and how often it rewrites the policy
WEATHER_EVERY_SECONDS = 60.0         # how often the trader writes weather.json
PROGRESS_FROM = "2026-10-12"         # the progress line counts real money from this UK day
TARGET_GBP_DAY = 100.0               # the £100 a day line (never an input to anything that adds risk)
TARGET_TRADING_DAYS = 15             # 12-30 Oct
NO_PACE_BEFORE_DAYS = 5              # no "a day" pace line before the 5th trading day (today counts)


def as_utc(ts: dt.datetime) -> dt.datetime:
    """A time as UTC; a naive one is read as UTC."""
    return ts.astimezone(UTC) if ts.tzinfo is not None else ts.replace(tzinfo=UTC)


def uk_day(ts: dt.datetime) -> dt.date:
    """The UK calendar day of ``ts`` (BST or GMT as the date requires)."""
    return as_utc(ts).astimezone(TZ_UK).date()


def is_uk_trading_day(day: dt.date) -> bool:
    """Monday to Friday (UK). Holidays are not removed in v1."""
    return day.weekday() < 5


def parse_utc(text: Any) -> Optional[dt.datetime]:
    """An ISO time as UTC ("Z" allowed; a time without a zone is UTC), or None."""
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        t = dt.datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return as_utc(t)


def next_release_utc(after: dt.datetime) -> dt.datetime:
    """The first 06:30 UK on a UK trading day at or after ``after``."""
    hh, mm = (int(x) for x in RELEASE_UK.split(":"))
    local = as_utc(after).astimezone(TZ_UK)
    day = local.date()
    for _ in range(10):
        cand = dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=TZ_UK)
        if is_uk_trading_day(day) and cand >= local:
            return cand.astimezone(UTC)
        day += dt.timedelta(days=1)
    return as_utc(after)                                          # not reached


# =====================================================================
# Modes, routes, states, sources
# =====================================================================
MODE_OFF, MODE_WATCH, MODE_GUARD, MODE_PILOT = "OFF", "WATCH", "GUARD", "PILOT"
MODES = (MODE_OFF, MODE_WATCH, MODE_GUARD, MODE_PILOT)        # from the least the coach may do to the most
MODE_RANK = {m: i for i, m in enumerate(MODES)}

ROUTE_BASELINE = "BASELINE"      # Aaron's static settings decide (today's code path exactly)
ROUTE_PIN = "PIN"                # the same, marked as Aaron's pin (only HALT applies)
ROUTE_REAL = "REAL"              # a coach grant: real money at risk_pct / money_gbp (Trend & Breakout keys only in v1)
ROUTE_PRACTICE = "PRACTICE"      # forced to paper; for a bot with no paper path this means SKIP
ROUTE_SKIP = "SKIP"              # no new entry
ROUTES = (ROUTE_BASELINE, ROUTE_PIN, ROUTE_REAL, ROUTE_PRACTICE, ROUTE_SKIP)
RESTRICTING_ROUTES = (ROUTE_PRACTICE, ROUTE_SKIP)

STATE_PRACTICE = "PRACTICE"
STATE_LIVE_TRIAL = "LIVE_TRIAL"
STATE_LIVE = "LIVE"              # with a step: base, S1..S4 (S5..S9 on the demo)
STATE_BANNED_REAL = "BANNED_REAL"
STATE_PINNED = "PINNED"
STATE_PLACED = "PLACED"
STATE_PAUSED = "PAUSED"
STATES = (STATE_PRACTICE, STATE_LIVE_TRIAL, STATE_LIVE, STATE_BANNED_REAL, STATE_PINNED, STATE_PLACED,
          STATE_PAUSED)

SOURCE_HALT = "halt"             # data/HALT
SOURCE_OFF = "off"               # COACH-OFF, envelope OFF or invalid, bot not governed
SOURCE_WATCH = "watch"           # WATCH (or before watch_until): BASELINE, the would-be decision logged
SOURCE_POLICY = "policy"         # a valid policy decided
SOURCE_FALLBACK = "fallback"     # stale/broken policy: BASELINE tightened by the last good restrictions
SOURCE_ERROR = "error"           # a bug inside the gate: BASELINE, logged
SOURCES = (SOURCE_HALT, SOURCE_OFF, SOURCE_WATCH, SOURCE_POLICY, SOURCE_FALLBACK, SOURCE_ERROR)

BRAKE_DAY = ("OPEN", "LINE", "PAUSE")
BRAKE_WEEK = ("OPEN", "LINE")
BRAKE_DRAWDOWN = ("OPEN", "LINE", "PAUSE")

# =====================================================================
# Bots, kinds of market, conditions
# =====================================================================
BOT_TNB, BOT_RUNNER, BOT_IAN, BOT_BANDBREAKER, BOT_CROWD = "tnb", "runner", "ian", "bandbreaker", "crowd"
GOVERNABLE_BOTS = (BOT_TNB, BOT_RUNNER, BOT_IAN, BOT_BANDBREAKER, BOT_CROWD)
NEVER_GOVERNED = ("rider", "lab")            # in the envelope's govern: the envelope is invalid
KEY_BOTS = GOVERNABLE_BOTS + ("lab",)       # bots a key may name (lab keys: evidence only in v1)
BOT_MAGIC = {BOT_TNB: 990_311, BOT_RUNNER: 990_511, BOT_BANDBREAKER: 990_611, BOT_CROWD: 990_711}
BOT_LABEL = {BOT_TNB: "Trend & Breakout", BOT_RUNNER: "Momentum Runner", BOT_IAN: "Financial Ian",
             BOT_BANDBREAKER: "Band Breaker", BOT_CROWD: "Crowd Fader", "lab": "the strategy lab"}
REAL_ROUTE_BOTS = (BOT_TNB,)                 # v1: only Trend & Breakout keys can be granted REAL

KINDS = ("FX_MAJOR", "FX_MINOR", "FX_EXOTIC", "INDEX", "GOLD", "SILVER", "METAL", "ENERGY", "EQUITY",
         "CRYPTO", "UNKNOWN")               # contracts.infer_group's names
CONDITIONS = ("TREND", "RANGE", "SQUEEZE", "HIGH_VOL", "LOW_VOL", "NEWS", "UNCLEAR")   # data.regime.Regime
CONDITION_UNCLEAR = "UNCLEAR"                # never granted
WILD = "*"

# =====================================================================
# Formula 1 (pinned until its 20-live-trade judgement; Aaron, 10 Oct)
# =====================================================================
F1_KEY = "tnb|MOMENTUM_CONTINUATION|FX_MINOR|*"
F1_KIND = "FX_MINOR"
F1_JUDGE_AFTER = 20                  # live trades (verdicts.formula1_live)
F1_MONEY_GBP = 13.0                  # flat £13 a trade until judged
F1_PIN_WHY = "Formula 1: flat £13 until 20 live trades, then judged (Aaron, 10 Oct)"
JUDGEMENT_KEEP = "KEEP"              # coach-governed at base (£13), steps by evidence
JUDGEMENT_PRACTICE = "PRACTICE"      # back to practice (a downward move the coach may make alone)
JUDGEMENTS = (JUDGEMENT_KEEP, JUDGEMENT_PRACTICE)

# =====================================================================
# Size steps (section 6.1) and the demo ladder (section 21)
# =====================================================================
# (name, percent of equity); None = Trend & Breakout's own base size: its base percent with its money cap
# (mintel.engine.risk.base_risk(cfg.risk); today 0.70% capped at £13)
STEPS_PCT: tuple[tuple[str, Optional[float]], ...] = (
    ("trial", 0.35), ("base", None), ("S1", 0.85), ("S2", 1.00), ("S3", 1.25), ("S4", 1.50))
DEMO_STEPS_PCT: tuple[tuple[str, float], ...] = (
    ("S5", 2.00), ("S6", 2.50), ("S7", 3.00), ("S8", 4.00), ("S9", 5.00))
STEP_ORDER = tuple(n for n, _ in STEPS_PCT) + tuple(n for n, _ in DEMO_STEPS_PCT)
DEMO_STEP_NAMES = tuple(n for n, _ in DEMO_STEPS_PCT)
TRIAL_STEP = "trial"
BASE_STEP = "base"
TRIAL_MAX_GBP = 8.0                  # never above £8; no trial if the minimum lot risks more
V1_MAX_STEP_PCT = 1.50               # S4: never above cfg.risk.max_risk_pct either
DEMO_MAX_STEP_PCT = 5.00             # S9: the highest the envelope's demo_max_step_pct may say

# Daily stop on the demo: 3 -> 4 -> 5 -> 6 -> 8 -> 10 % (never skips a level)
DEMO_DAILY_STOP_LADDER: tuple[float, ...] = (3.0, 4.0, 5.0, 6.0, 8.0, 10.0)
DEMO_MAX_DAILY_STOP_PCT = 10.0
# Demo trials: start at base, at most 4 at once, at most £150 of trial losses a week
DEMO_MAX_TRIALS = 4
DEMO_TRIAL_LOSS_BUDGET_GBP_WEEK = 150.0
# Coach brakes on the demo follow the effective stop: B1 at half of it, B2 at three quarters
DEMO_DAY_LINE_SHARE = 0.50
DEMO_DAY_PAUSE_SHARE = 0.75
# Drawdown on the demo: the 12% breaker may go to 25% once the stop is >= 6%; the coach's B4 at 10% / 15%
DEMO_DRAWDOWN_BREAKER_PCT = 25.0
DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT = 6.0
DEMO_DRAWDOWN_LINE_PCT = 10.0
DEMO_DRAWDOWN_PAUSE_PCT = 15.0
# Exposure on the demo: correlated = 1.5 x the highest live step, total = 3 x it, never above 15%
DEMO_CORRELATED_MULT = 1.5
DEMO_TOTAL_MULT = 3.0
DEMO_TOTAL_MAX_PCT = 15.0
# The demo test: config.json DEMO and the broker reports a demo account, as the trader writes it into
# weather.json (account_demo), no older than this
DEMO_WEATHER_MAX_AGE_S = 180.0


def step_pct(name: str, base_pct: float) -> Optional[float]:
    """The percent of equity a step risks ("base" is Trend & Breakout's base percent, ``base_pct``, from
    mintel.engine.risk.base_risk(cfg.risk)); None for an unknown name."""
    if name == BASE_STEP:
        return float(base_pct)
    for n, pct in STEPS_PCT + DEMO_STEPS_PCT:
        if n == name:
            return float(pct) if pct is not None else float(base_pct)
    return None


def step_money(name: str, equity: float, base_pct: float, money_cap: float) -> Optional[float]:
    """The money a step risks at ``equity``: the trial is never above £8; base is held to Trend & Breakout's
    money cap, ``money_cap`` (today 0.70% capped at £13; mintel.engine.risk.base_risk). None for an unknown
    step."""
    pct = step_pct(name, base_pct)
    if pct is None:
        return None
    money = float(equity) * pct / 100.0
    if name == TRIAL_STEP:
        money = min(money, TRIAL_MAX_GBP)
    if name == BASE_STEP and money_cap and money_cap > 0:
        money = min(money, float(money_cap))
    return round(money, 2)


# =====================================================================
# The policy file and the gate (section 10)
# =====================================================================
POLICY_MAX_BYTES = 256 * 1024        # V1
LEASE_SECONDS = 180.0                # every grant is a 180-second lease (valid_s may not be longer)
FUTURE_SKEW_SECONDS = 120.0          # V6: written no more than this far in the future
POLICY_KEYS = ("schema", "seq", "written_utc", "valid_s", "account_login", "envelope_sha", "build", "mode",
               "grants_allowed", "data", "account", "brakes", "headroom_gbp", "entries", "floors", "blocks",
               "gates", "demo", "judged", "sha256")
POLICY_REQUIRED = ("schema", "seq", "written_utc", "valid_s", "envelope_sha", "mode", "grants_allowed",
                   "entries", "sha256")
ENTRY_KEYS = ("route", "state", "risk_pct", "money_gbp", "step", "rule", "why_id", "why", "until_utc")
FLOOR_KEYS = ("min_score", "min_rr")
BLOCK_KEYS = ("bots", "symbols", "route", "rule", "until_utc", "why_id", "why")
GATES_KEYS = ("rollover_ny", "friday_cutoff_ny")
DATA_KEYS = ("ok", "failed", "broker_check")
BROKER_CHECK_KEYS = ("records_today", "page_today")
ACCOUNT_KEYS = ("equity", "e0", "real_today", "open_real_risk", "peak")
BRAKES_KEYS = ("day", "week", "drawdown", "trial_budget_left_gbp")
DEMO_KEYS = ("max_daily_loss_pct", "max_risk_pct", "max_drawdown_pct", "max_correlated_risk_pct",
             "max_total_risk_pct")
JUDGED_KEYS = ("verdict", "live_trades", "judged_utc")
WHY_MAX_CHARS = 400
ROLLOVER_NY = ("16:45", "17:15")     # no new granted entry inside (NY time)
FRIDAY_CUTOFF_NY = "16:00"           # no new granted FX or index entry after this on Fridays (NY time)
ONE_IDEA_SECONDS = 300.0             # B6: another bot's real position on the symbol opened within 5 minutes
SEND_CHECK_SLACK = 0.05              # PaperBroker.send: risk at the stop may sit this share over the ceiling
#                                     (the price and the equity move in the seconds between sizing and the send;
#                                     RiskManager.size and the trader's own check stay exact)

# =====================================================================
# The envelope (section 10.1)
# =====================================================================
ENVELOPE_KEYS = ("schema", "mode", "watch_until_utc", "govern", "tnb_real_kinds", "trial_risk_pct",
                 "trial_max_gbp", "max_trials", "trial_loss_budget_gbp_week", "max_step_pct", "announce_steps",
                 "day_line_pct", "day_pause_pct", "week_line_pct", "drawdown_line_pct", "drawdown_pause_pct",
                 "pins", "set_by", "set_utc", "demo_max_step_pct", "demo_max_daily_stop_pct")
PIN_KEYS = ("key", "why", "judge_after", "set_utc")

# =====================================================================
# Evidence (section 4; Builder E)
# =====================================================================
R_CLIP_LO, R_CLIP_HI = -3.0, 5.0               # x = net after commission / planned risk, clipped
PRACTICE_WEIGHT = 0.5                          # lambda: bot practice and lab practice
DECAY_HALF_LIFE_DAYS = 30.0                    # d = 0.5 ** (age in UK trading days / 30)
OLD_EPOCH_WEIGHT = 0.5                         # v: closed before the key's latest epoch
HAIRCUT_DEFAULT_R = 0.10                       # practice outcomes have this subtracted ...
HAIRCUT_MIN_EACH = 20                          # ... until >= 20 real and >= 20 practice in overlapping weeks
HAIRCUT_MIN_R, HAIRCUT_MAX_R = 0.05, 0.30
HAIRCUT_MOVE_SE = 2.0                          # the measured haircut moves only on a gap shift > 2 SE
CLUSTER_MINUTES = 60.0                         # same bot, same direction, a shared leg, within 60 min of the first
SQLITE_BUSY_TIMEOUT_S = 2.0
DATA_MAX_EXCLUDED_SHARE = 0.05                 # under 5% of rows excluded
RECONCILE_MIN_GBP = 3.0                        # records vs the page: within max(£3, 10% x |page today|)
RECONCILE_SHARE = 0.10
RUNNER_FALLBACK_RISK_GBP = 10.0                # runner.max_risk_money at the time; flagged risk_estimated
BOT_FALLBACK_RISK_GBP = 10.0                   # Band Breaker / Crowd Fader risk_money; flagged
COMMISSION_RECORDED_FROM = "2026-10-09"        # before this UK day pnl_money lacks the opening commission
INDEX_LEGS = {"US500": "USEQ", "US30": "USEQ", "USTEC": "USEQ", "DE40": "EUEQ", "UK100": "UKEQ",
              "JP225": "JPEQ"}                 # other indices: their own symbol; XAU*: XAU; XAG*: XAG

# =====================================================================
# The estimator (section 5; Builder E)
# =====================================================================
PRIOR_M0_R = -0.05
PRIOR_S0_R = 0.10
TAU_DEFAULT = {1: 0.10, 2: 0.10, 3: 0.15}       # approach, kind, condition
TAU_MIN, TAU_MAX = 0.05, 0.30
TAU_MIN_SIBLINGS = 5
TAU_SIBLING_MIN_NEFF = 5.0
SIGMA_FLOOR_R = 1.0
CLUSTER_RATIO_MIN, CLUSTER_RATIO_MAX = 1.0, 3.0
SCEPTICAL_CAP_R = 0.05
Z80, Z90 = 0.8416, 1.2816
COST_STRESS = 1.5                               # m_cost15: commission x 1.5
LAB_FAMILY_MIN_MEMBERS = 8
LAB_FAMILY_FLOOR_R = -0.15

# =====================================================================
# Moves up and down (section 6; Builder D)
# =====================================================================
G1_MIN_TRADES = 30
G1_MIN_NEFF = 20.0
G1_MIN_CLUSTERS = 12
G1_MIN_DAYS = 8
G1_MIN_FORWARD = 15
G1_MIN_P_PLUS = 0.90
G1_LOOKBACK_DAYS = 30
G1_MAX_BEST_DAY_SHARE = 0.40
MAX_TRIALS_OPEN = 2
MAX_NEW_TRIALS_NIGHT = 1
G2_MIN_TRADES = 20
G2_MIN_DAYS = 5
G2_MIN_P_PLUS_REAL = 0.80
G2_MIN_GAP_R = -0.15
G3_MIN_TRADES = 20
G3_MIN_DAYS_SINCE_CHANGE = 5
G3_MIN_P_PLUS_REAL = 0.90
KELLY_FRACTION = 0.25                           # k% = 100 x 0.25 x max(0, LCB80) / (sigma^2 x c)
DEMO_KELLY_FRACTION = 0.50                      # half-Kelly on the demo ladder
STOP_SAFETY_DAYS = 2000                         # the fixed-seed bootstrap (seed = the date as an int)
STOP_SAFETY_MAX_SHARE = 0.02                    # must hit -stop on <= 2% of days
DEMO_STOP_RAISE_MAX_SHARE = 0.05                # a raised demo stop: the new level tripped on <= 5% of days
DEMO_STOP_RAISE_MIN_LIVE_KEYS = 2               # >= 2 governed keys LIVE at base or above ...
DEMO_STOP_RAISE_MIN_P_PLUS = 0.90               # ... each with P+_real >= 0.90
DEMO_STOP_RAISE_LOOKBACK_DAYS = 10              # the stop blocked a proven key on a day that ended positive
MAX_STEPS_NIGHT = 1
MAX_STEPS_WEEK = 2
DEMO_MAX_STEPS_WEEK = 3
COOLOFF_DAYS = 10                               # a key dropped to practice: no re-promotion for 10 UK trading days
STEP_DOWN_HOLD_DAYS = 5                         # a step down cannot be reversed for 5
CUSUM_K = 0.05                                  # D1: S = max(0, S + 0.05 - x/sigma)
CUSUM_H = 12.0                                  # alarm at S >= 12
GAP_R = -2.5                                    # D2: one real fill at <= -2.5 R: paused for the UK day
D3_STEP_DOWN_P = 0.80
D3_TO_BASE_P = 0.60
D3_TO_PRACTICE_P = 0.50
D3_TO_PRACTICE_MIN_NEFF = 20.0
D4_STEP_DOWN_SIGMA = 8.0
D4_PRACTICE_SIGMA = 12.0
D5_TRIAL_LOSS_GBP = -30.0
S1_MIN_DAYS = 3                                 # approach x condition negative on >= 3 UK days: banned for real
S2_MIN_DAYS = 6                                 # a symbol negative on >= 2/3 of >= 6 traded days
S2_SHARE = 2.0 / 3.0
JUDGEMENT_MIN_DAYS = 3                          # S3: no judgement on fewer than 3 UK days
LUCK_SHUFFLES = 200
LUCK_MAX_PASS_RATE = 0.02
SCORECARD_MIN_DAYS = 20
SCORECARD_MIN_TRADES = 40
SCORECARD_GUARD_P = 0.80
PLACED_JUDGE_AFTER = 30                         # Runner and Ian: judged at 30 real trades

# =====================================================================
# Brakes (section 7; Builder D). The percents are the envelope's (v1) or the demo shares above.
# =====================================================================
WEEK_LINE_RELEASE_PCT = 2.5                     # B3 releases once back above -2.5%
DRAWDOWN_RELEASE_SHARE = 0.5                    # drawdown brakes release once under half the trigger
B9_MAX_POLICY_CHANGES_24H = 20
B9_MAX_TRIAL_STARTS_WEEK = 3
B9_MAX_TICK_ERRORS = 3

# =====================================================================
# Market board (section 8; Builder D)
# =====================================================================
BOARD_HOLD_MINUTES = 2.0                        # a label must hold 2 minutes to show (NEWS at once); display only
SESSION_PHASES = ("ASIA", "LONDON_AM", "OVERLAP", "NY_PM", "ROLLOVER", "LATE")
WEATHER_ENTRY_KEYS = ("symbol", "kind", "regime", "confidence", "vol_state", "spread_points", "news_state",
                      "tradable", "as_of_utc")

# =====================================================================
# Tweaks (section 13; Builder E)
# =====================================================================
MAX_CHALLENGERS = 3
CHALLENGER_SCORE_STEPS = (3, 6)                 # min_score = the bot's threshold + 3, + 6
CHALLENGER_MIN_RR = (2.0, 2.2)
IAN_CHALLENGER_SCORE_STEP = 5
TWEAK_PRIOR_SD_R = 0.10
TWEAK_MIN_TRADES = 40
TWEAK_MIN_DAYS = 8
TWEAK_MIN_SKIPPED = 10
TWEAK_MIN_DELTA_R = 0.05
TWEAK_MIN_P = 0.983
TWEAK_REVERT_TRADES = 40
TWEAK_REVERT_P = 0.5
MAX_FLOOR_CHANGES_NIGHT = 1


# =====================================================================
# Dataclasses
# =====================================================================
def _token_ok(text: str) -> bool:
    return text == WILD or (bool(text) and text.replace("_", "").isalnum() and text == text.upper())


@dataclass(frozen=True)
class Key:
    """``bot|APPROACH|KIND|CONDITION``; "*" is any. The condition is the raw label at entry."""
    bot: str
    approach: str = WILD
    kind: str = WILD
    condition: str = WILD

    def __str__(self) -> str:
        return f"{self.bot}|{self.approach}|{self.kind}|{self.condition}"

    @property
    def text(self) -> str:
        return str(self)

    @classmethod
    def parse(cls, text: Any) -> "Key":
        """Raises ValueError for anything that is not a well-formed key."""
        if not isinstance(text, str):
            raise ValueError(f"a key must be text, not {type(text).__name__}")
        parts = text.split("|")
        if len(parts) != 4:
            raise ValueError(f"key {text!r} does not have four parts")
        bot, approach, kind, condition = parts
        if bot not in KEY_BOTS:
            raise ValueError(f"key {text!r} names an unknown bot")
        for tok in (approach, kind, condition):
            if not _token_ok(tok):
                raise ValueError(f"key {text!r} has a malformed part {tok!r}")
        return cls(bot, approach, kind, condition)

    @classmethod
    def make(cls, bot: str, approach: str = WILD, kind: str = WILD, condition: str = WILD) -> "Key":
        def norm(x: str) -> str:
            s = str(x or "").strip().upper()
            return s or WILD
        return cls(str(bot), norm(approach), norm(kind), norm(condition))

    def lookup_chain(self) -> tuple["Key", ...]:
        """Where the gate looks, most specific first: exact, then ``..|*``, then ``bot|*|KIND|*``, then
        ``bot|*|*|*`` (then BASELINE). Duplicates are left out."""
        chain = [self, Key(self.bot, self.approach, self.kind, WILD), Key(self.bot, WILD, self.kind, WILD),
                 Key(self.bot, WILD, WILD, WILD)]
        out: list[Key] = []
        for k in chain:
            if k not in out:
                out.append(k)
        return tuple(out)

    def covers(self, other: "Key") -> bool:
        """Does this key (possibly with "*") include ``other``?"""
        return self.bot == other.bot and all(
            a == WILD or a == b for a, b in ((self.approach, other.approach), (self.kind, other.kind),
                                             (self.condition, other.condition)))


@dataclass(frozen=True)
class Pin:
    """Aaron's pin: the coach leaves the key alone (only HALT applies). ``judge_after`` > 0: released only
    by a recorded judgement in the policy after that many live trades (Formula 1: 20)."""
    key: str
    why: str = ""
    judge_after: int = 0
    set_utc: str = ""

    def to_dict(self) -> dict:
        out: dict = {"key": self.key, "why": self.why}
        if self.judge_after:
            out["judge_after"] = int(self.judge_after)
        if self.set_utc:
            out["set_utc"] = self.set_utc
        return out


@dataclass(frozen=True)
class Envelope:
    """Aaron's envelope, ``data/coach.json``. Written only by the installer's one-time step and Aaron's CLI;
    the coach process reads it and never writes it. Field names are the file's keys (ENVELOPE_KEYS)."""
    schema: int = ENVELOPE_SCHEMA
    mode: str = MODE_OFF
    watch_until_utc: str = ""
    govern: tuple[str, ...] = (BOT_TNB, BOT_RUNNER, BOT_IAN, BOT_BANDBREAKER, BOT_CROWD)
    tnb_real_kinds: tuple[str, ...] = ("FX_MINOR", "FX_MAJOR", "INDEX", "GOLD")
    trial_risk_pct: float = 0.35
    trial_max_gbp: float = 8.0
    max_trials: int = 2
    trial_loss_budget_gbp_week: float = 40.0
    max_step_pct: float = 1.5
    announce_steps: bool = True
    day_line_pct: float = 1.5
    day_pause_pct: float = 2.25
    week_line_pct: float = 5.0
    drawdown_line_pct: float = 5.0
    drawdown_pause_pct: float = 8.0
    pins: tuple[Pin, ...] = (Pin(F1_KEY, F1_PIN_WHY, F1_JUDGE_AFTER),)
    set_by: str = ""
    set_utc: str = ""
    # section 21: the most the demo ladder may reach (never used on a real-money account)
    demo_max_step_pct: float = DEMO_MAX_STEP_PCT
    demo_max_daily_stop_pct: float = DEMO_MAX_DAILY_STOP_PCT

    def governs(self, bot: str) -> bool:
        return bot in self.govern

    def to_dict(self) -> dict:
        return {"schema": self.schema, "mode": self.mode, "watch_until_utc": self.watch_until_utc,
                "govern": list(self.govern), "tnb_real_kinds": list(self.tnb_real_kinds),
                "trial_risk_pct": self.trial_risk_pct, "trial_max_gbp": self.trial_max_gbp,
                "max_trials": self.max_trials, "trial_loss_budget_gbp_week": self.trial_loss_budget_gbp_week,
                "max_step_pct": self.max_step_pct, "announce_steps": self.announce_steps,
                "day_line_pct": self.day_line_pct, "day_pause_pct": self.day_pause_pct,
                "week_line_pct": self.week_line_pct, "drawdown_line_pct": self.drawdown_line_pct,
                "drawdown_pause_pct": self.drawdown_pause_pct, "pins": [p.to_dict() for p in self.pins],
                "set_by": self.set_by, "set_utc": self.set_utc, "demo_max_step_pct": self.demo_max_step_pct,
                "demo_max_daily_stop_pct": self.demo_max_daily_stop_pct}


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


@dataclass(frozen=True)
class Entry:
    """One key's instruction. REAL needs risk_pct and money_gbp. On any other route money_gbp, when set, is
    a cap that can only lower a placed bot's money (the Momentum Runner, Financial Ian)."""
    route: str
    state: str
    risk_pct: Optional[float] = None
    money_gbp: Optional[float] = None
    step: Optional[str] = None
    rule: Optional[str] = None
    why_id: Optional[int] = None
    why: Optional[str] = None
    until_utc: Optional[str] = None

    def to_dict(self) -> dict:
        return _drop_none({"route": self.route, "state": self.state, "risk_pct": self.risk_pct,
                           "money_gbp": self.money_gbp, "step": self.step, "rule": self.rule,
                           "why_id": self.why_id, "why": self.why, "until_utc": self.until_utc})


@dataclass(frozen=True)
class Floor:
    """Tighten-only (section 13): a key's weaker signals go to practice."""
    min_score: Optional[float] = None
    min_rr: Optional[float] = None

    def to_dict(self) -> dict:
        return _drop_none({"min_score": self.min_score, "min_rr": self.min_rr})


@dataclass(frozen=True)
class Block:
    """Symbols forced to PRACTICE or SKIP for coach-granted keys of the named bots (e.g. S2)."""
    bots: tuple[str, ...]
    symbols: tuple[str, ...]
    route: str = ROUTE_PRACTICE
    rule: Optional[str] = None
    until_utc: Optional[str] = None
    why_id: Optional[int] = None
    why: Optional[str] = None

    def to_dict(self) -> dict:
        out = {"bots": list(self.bots), "symbols": list(self.symbols), "route": self.route,
               "until_utc": self.until_utc}
        out.update(_drop_none({"rule": self.rule, "why_id": self.why_id, "why": self.why}))
        return out


@dataclass(frozen=True)
class Gates:
    """The two market-hours gates for coach-granted keys (NY time)."""
    rollover_ny: tuple[str, str] = ROLLOVER_NY
    friday_cutoff_ny: str = FRIDAY_CUTOFF_NY

    def to_dict(self) -> dict:
        return {"rollover_ny": list(self.rollover_ny), "friday_cutoff_ny": self.friday_cutoff_ny}


@dataclass(frozen=True)
class DemoLimits:
    """Section 21: the lifted values a lease may carry. Used only while the demo test passes
    (config.json DEMO and the broker's account is a demo, fresh in weather.json); otherwise config.json's
    own values apply. Each only ever LIFTS the config value (a lower number here is ignored)."""
    max_daily_loss_pct: Optional[float] = None
    max_risk_pct: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    max_correlated_risk_pct: Optional[float] = None
    max_total_risk_pct: Optional[float] = None

    def to_dict(self) -> dict:
        return _drop_none({"max_daily_loss_pct": self.max_daily_loss_pct, "max_risk_pct": self.max_risk_pct,
                           "max_drawdown_pct": self.max_drawdown_pct,
                           "max_correlated_risk_pct": self.max_correlated_risk_pct,
                           "max_total_risk_pct": self.max_total_risk_pct})


@dataclass(frozen=True)
class Judgement:
    """A pinned key's recorded judgement (Formula 1 after 20 live trades: verdicts.formula1_judgement)."""
    verdict: str
    live_trades: int
    judged_utc: str

    def to_dict(self) -> dict:
        return {"verdict": self.verdict, "live_trades": int(self.live_trades), "judged_utc": self.judged_utc}


@dataclass(frozen=True)
class Policy:
    """``data/coach/policy.json``, schema 1 (section 10.2). ``sha256`` is the canonical JSON's hash without
    that field (policy.py computes and checks it)."""
    seq: int
    written_utc: str
    envelope_sha: str
    mode: str
    grants_allowed: bool
    entries: dict[str, Entry] = field(default_factory=dict)
    valid_s: float = LEASE_SECONDS
    account_login: int = 0
    build: str = ""
    data: dict = field(default_factory=dict)
    account: dict = field(default_factory=dict)
    brakes: dict = field(default_factory=dict)
    headroom_gbp: Optional[float] = None
    floors: dict[str, Floor] = field(default_factory=dict)
    blocks: tuple[Block, ...] = ()
    gates: Optional[Gates] = None
    demo: Optional[DemoLimits] = None
    judged: dict[str, Judgement] = field(default_factory=dict)
    schema: int = SCHEMA_VERSION
    sha256: str = ""

    def to_dict(self, with_sha: bool = True) -> dict:
        out: dict = {"schema": self.schema, "seq": int(self.seq), "written_utc": self.written_utc,
                     "valid_s": self.valid_s, "account_login": int(self.account_login),
                     "envelope_sha": self.envelope_sha, "build": self.build, "mode": self.mode,
                     "grants_allowed": bool(self.grants_allowed), "data": dict(self.data),
                     "account": dict(self.account), "brakes": dict(self.brakes),
                     "entries": {k: e.to_dict() for k, e in self.entries.items()},
                     "floors": {k: f.to_dict() for k, f in self.floors.items()},
                     "blocks": [b.to_dict() for b in self.blocks]}
        if self.headroom_gbp is not None:
            out["headroom_gbp"] = self.headroom_gbp
        if self.gates is not None:
            out["gates"] = self.gates.to_dict()
        if self.demo is not None:
            out["demo"] = self.demo.to_dict()
        if self.judged:
            out["judged"] = {k: j.to_dict() for k, j in self.judged.items()}
        if with_sha:
            out["sha256"] = self.sha256
        return out


@dataclass(frozen=True)
class Decision:
    """What the gate says for one entry decision (section 10.3).

    ``route``: BASELINE / PIN (today's code path exactly), REAL (size with ``money_gbp``), PRACTICE or SKIP.
    ``money_gbp`` on REAL is already clamped (step, ceiling, envelope cap, headroom); on BASELINE for a
    placed bot (Runner, Ian) it may be a cap that only lowers the bot's money. ``ceiling_pct`` is the
    max_risk_pct for that one sizing call (config's, or a demo step's when the demo test passes);
    ``exposure`` (correlated, total) the demo exposure caps for it, else None."""
    route: str
    risk_pct: Optional[float] = None
    money_gbp: Optional[float] = None
    why: str = ""
    seq: int = 0
    source: str = SOURCE_OFF
    key: str = ""
    rule: str = ""
    state: str = ""
    ceiling_pct: Optional[float] = None
    exposure: Optional[tuple[float, float]] = None
    demo: bool = False

    @property
    def baseline(self) -> bool:
        return self.route in (ROUTE_BASELINE, ROUTE_PIN)

    @property
    def restricts(self) -> bool:
        return self.route in RESTRICTING_ROUTES


BASELINE_DECISION = Decision(ROUTE_BASELINE, why="", source=SOURCE_OFF)


@dataclass
class RiskCtx:
    """What the bot knows at the moment of decision, for the gate's clamp, B5 and B6.

    ``equity``: the live account equity. ``live_headroom_gbp``: the bot's own live headroom
    (broker's day, real only) or None. ``own_open``: this bot's open REAL trades as (opened_utc, risk_gbp);
    those opened after the policy was written are taken off its headroom (the policy lags by up to 60 s).
    ``others``: a callable giving the other bots' REAL positions (objects with symbol, side, open_time,
    magic), read only when a governed real entry needs B6. ``has_paper``: False when the bot has no paper
    path (PRACTICE then means SKIP). ``planned_gbp``: what this entry would risk on Aaron's own settings
    (for B5 on an entry that is real by those settings)."""
    equity: float = 0.0
    live_headroom_gbp: Optional[float] = None
    own_open: Sequence[tuple[dt.datetime, float]] = ()
    others: Optional[Callable[[], Sequence[Any]]] = None
    has_paper: bool = True
    planned_gbp: Optional[float] = None


@dataclass(frozen=True)
class Limits:
    """``CoachGate.effective_limits()``: the account-level limits the trader reads. config.json's own
    values unless a valid lease carries demo values AND the demo test passes (section 21)."""
    max_daily_loss_pct: float
    max_risk_pct: float
    max_drawdown_pct: float
    max_correlated_risk_pct: float
    max_total_risk_pct: float
    demo: bool = False
    why: str = ""

    @classmethod
    def of(cls, risk_cfg: Any, why: str = "") -> "Limits":
        return cls(float(risk_cfg.max_daily_loss_pct), float(risk_cfg.max_risk_pct),
                   float(risk_cfg.max_drawdown_pct), float(risk_cfg.max_correlated_risk_pct),
                   float(risk_cfg.max_total_risk_pct), False, why)

    def lifted(self, risk_cfg: Any) -> dict:
        """The fields that are above config.json's (only ever higher), for dataclasses.replace."""
        out = {}
        for name in DEMO_KEYS:
            mine, theirs = float(getattr(self, name)), float(getattr(risk_cfg, name))
            if mine > theirs:
                out[name] = mine
        return out

