"""The squad's pick: everything practises, and the best proven tests go to the DEMO account.

    python -m mintel.ops.weekly --config data/config.json            # what is picked, what is on the demo
    python -m mintel.ops.weekly --config data/config.json rank       # the ranking, worked out now
    python -m mintel.ops.weekly --config data/config.json veto       # cancel the latest pick

(The module keeps its old name, "weekly", so every file and page that reads it
still does; since 14 Oct the pick is made every UK trading day.)

Aaron, 13 Oct 2026: "put everything out of live and into the lab, once they
begin proving move the best each week into real money and live". Every bot
went to PAPER that day (deploy/mac/mintel_mac.sh ``apply_all_practice_2026_10_13``).
Aaron, 14 Oct 2026 (this version; it SUPERSEDES the 13 Oct weekly cadence, the
30-trade bar and the 20-trade demotion, which are kept in the requirements
ledger marked SUPERSEDED): "promising evidence, potentially after 10-15 trades,
depending on strategy type and trading sessions"; "don't demand full real-money
qualification standards before demo"; "avoid treating short winning streaks as
proof"; lab ideas really trade on the demo; 8-12 strategies on it; sizes
compared by tier. The method in plain words: docs/ops/promotion.md.

DEMO here means real orders on IC Markets' DEMO account (the "LIVE" of this
codebase); PAPER means practice on live prices. They are never mixed.

THE CANDIDATES - everything that is practising, one pool:

* the Momentum Runner, Band Breaker, Crowd Fader and Financial Ian, each from
  its own PAPER record (``verdicts.practice_trades``);
* Trend & Breakout's parts: each approach x kind of market, its paper record
  joined by ticket to its journal;
* EVERY lab idea (data/lab/lab.sqlite, read-only) on its own PRACTICE trades.
  Since 14 Oct a lab idea HAS a demo path: it is promoted by writing
  data/labdemo/roster.json, which the lab-demo engine inside the trader re-reads
  (``mintel/labdemo``; no restart). The control family (CONTROL_TIME_OF_DAY, the
  clock trades) is never promoted: it is the luck benchmark.

THE EARLY-PROMOTION BAR, on practice trades of the last LOOKBACK_DAYS (28) days
after all costs (for a test taken back to PAPER before, only those since):

1. enough forward paper trades for how often it trades: 15 for a frequent test
   (3+ a UK trading day), 12 for a medium one, 10 for a slow one (under 1 a
   day); in every case spread over at least 3 UK days;
2. positive after all costs, a profit factor of at least 1.3, and still
   positive without its single best trade (a short winning streak is not proof);
3. selection-bias protection: its average R a trade (net / the money it risked)
   is shrunk toward the pool's average by empirical Bayes (``shrink``), so a
   few lucky trades in a pool of ~640 count for little; the shrunk figure must
   be above zero AND beat the best control idea's shrunk figure by
   CONTROL_MARGIN_R (0.05 R): a test that does no better than a clock trade is
   indistinguishable from luck;
4. its past and fresh lab tests are a WEIGHT on its rank, never a gate: strongly
   negative ones halve it, mildly negative ones take 15% off.

THE RANKING: shrunk R a trade x trades a week x that weight - the R a week it
should make, cautiously. The pick fills the demo's free places (``demo_target``
in data/weekly.json, default 10, at most 12; Aaron: 8-12) best first, with at
most DIVERSITY_MAX (2) on the demo per market and direction, preferring a
spread across markets and sessions (each one already there in the same market
or session lowers a test's place in the queue, never its figures).

THE CADENCE (UK time, Europe/London, BST and GMT alike):

* every UK trading day at or after 22:30: the pick is worked out and written to
  data/weekly/plan-<day>.json and data/weekly/latest.json (the ranking, the
  picks, the reasons, the release time). A watchdog that was down catches up
  until the release;
* 06:30 on the next UK trading day: released - a bot or a Trend & Breakout
  part switched LIVE through ``modes.set_modes`` (its process restarted), a lab
  idea added to the lab-demo roster with its own magic number and its demo risk
  tier (0.5%, 1%, 2% of equity a trade, in turn: ``roster.next_tier``). A demoted
  test's place is filled from the qualifiers at the next release;
* ``veto`` (Aaron) cancels the latest pick; a pick already on the demo is taken
  back. data/HALT: nothing is applied while it exists (a pick waits, and lapses
  at the next pick). A pick taken back is never re-released by the same plan.

THE DEMO JUDGEMENT, on the broker's own figures after costs (MetaTrader's
closed positions by magic number; a lab idea's from data/labdemo/labdemo.sqlite,
booked from the broker's closed deals):

* after JUDGE_AFTER (20) demo trades: back to PAPER when the bootstrap upper
  bound (90th percentile) of its mean net a trade is below zero; kept (recorded
  once) when the lower bound (10th percentile) is above zero;
* after JUDGE_NET_AFTER (40) demo trades: back to PAPER when net <= 0;
* at any point: back to PAPER at a demo loss of EARLY_EXIT_RISKS (15) x its risk
  a trade, or a drawdown from its best of DD_GUARD_RISKS (20) x its risk a trade.

A demotion only removes risk, so it is applied at the next check (never under
HALT). When the page cannot be read, the bots' own records are read instead -
only ever to take something back, never to keep it.

Every promotion, keep and demotion is one line of data/weekly/history.jsonl,
with its reason. Everything here is idempotent: before any switch its intent is
saved in state.json, the switch is read back, and a failure puts every file back
as it was; a change already made (a crash in between) is recorded, not made
again. A state.json, config.json or roster that cannot be read stops the pass.
Nothing here doubles after a loss or adds exposure because of one; every trade
keeps its own broker stop. config.json is read with ``json`` alone; the secrets
file is never opened here.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import math
import os
import random
import sys
from pathlib import Path
from statistics import median
from typing import Callable, Iterable, Optional, Sequence

from ..clock import TZ_LONDON, to_utc, utcnow

log = logging.getLogger("mintel.weekly")

# ------------------------------------------------------------ the rule --
LOOKBACK_DAYS = 28              # only practice trades this recent count
LUCK_MAX = 0.10                 # 13 Oct's luck line: still worked out and shown, no longer a gate (14 Oct)
LCB_QUANTILE = 0.10             # the cautious average a trade: this quantile of the resampled averages
UCB_QUANTILE = 0.90             # the hopeful one: the demo judgement demotes when even this is below zero
RESAMPLES = 2000
# 14 Oct, the early-promotion bar (supersedes 13 Oct's 30 trades over 5 days, PF 1.2, more up days, luck)
MIN_DAYS = 3                    # UK days the trades are spread over, whatever the frequency
NEED_FREQUENT, NEED_MEDIUM, NEED_SLOW = 15, 12, 10   # forward paper trades needed, by frequency
FREQUENT_PER_DAY, SLOW_PER_DAY = 3.0, 1.0            # trades a UK trading day: frequent at or over, slow under
MIN_PF = 1.3
CONTROL_MARGIN_R = 0.05         # beat the best control idea's shrunk R a trade by at least this
CONTROL_MIN_TRADES = 10         # a control idea counts in the benchmark once it has this many trades
POOL_MIN_TRADES = 5             # a test counts in the shrinkage pool once it has this many trades
POOL_MIN_SIZE = 5               # with fewer tests in the pool, a fixed prior is used
TAU2_DEFAULT = 0.04             # the fixed prior's spread of true R a trade (0.2 R), around 0
# Promotion rule v2 (Aaron, 15 Oct 2026, "Fast"): the pool's spread of true R a trade is never taken as less
# than 0.2 R, so a strategy averaging about +1 R a trade reaches demo by about 15 trades roughly 7-9 times in 10 (v1: 1-2 in 10); the control
# (luck) benchmark, the bar, the diversity cap and the demo judgement still stand. v1 (14 Oct) floored it at
# 0.05 R (TAU2_FLOOR_V1), kept for comparison (docs/ops/promotion.md, docs/REQUIREMENTS_LEDGER.md).
TAU2_FLOOR_V1 = 0.0025
TAU2_FLOOR = 0.04
PROMOTION_RULES = "v2"
VAR_FLOOR_R2 = 0.25             # a test's own R spread is never taken as less than 0.5 R (few trades) in its
                                # own shrinkage; the pool's prior uses the unfloored spread (15 Oct, r_noise)
WEIGHT_STRONG_NEG, WEIGHT_MILD_NEG = 0.5, 0.85      # past / fresh lab tests: a weight on the rank, not a gate
DIVERSITY_MAX = 2               # on the demo at once, per market and direction
SPREAD_MARKET, SPREAD_SESSION = 0.85, 0.90           # the queue's preference for a new market / session
DEMO_TARGET, DEMO_TARGET_MAX = 10, 12                # on the demo at once (data/weekly.json demo_target)
# 14 Oct, the demo judgement (supersedes 13 Oct's "after 20 demo trades net <= 0" and 10 x risk)
JUDGE_AFTER = 20                # demo trades before the evidence test (bootstrap bounds)
JUDGE_NET_AFTER = 40            # demo trades after which net <= 0 is enough
EARLY_EXIT_RISKS = 15.0         # a demo loss of this many times its risk a trade: back to PAPER
DD_GUARD_RISKS = 20.0           # a drawdown from its best of this many risks: back to PAPER
TOO_EARLY_TRADES = 10           # Home: fewer trades than this is TOO EARLY
HOME_TOP, HOME_BOTTOM = 10, 3   # Home lists the best ten and the worst three; the Practice page lists them all
PICK_AT = (22, 30)              # every UK trading day, 22:30 UK (14 Oct; Fridays only before)
PICK_WEEKDAY = 4                # 13 Oct's Friday: kept for older readers, no longer the cadence
RELEASE_AT = (6, 30)            # 06:30 UK on the next UK trading day
CHECK_SECONDS = 300.0           # how often the watchdog looks
FALLBACK_RISK_GBP = 10.0        # risk a trade when no record says (coach.BOT_FALLBACK_RISK_GBP)
SCHEMA = 1
MAX_PROMOTIONS = 3              # 13 Oct's weekly_max_promotions (SUPERSEDED 14 Oct by demo_target; still read)
MAX_PROMOTIONS_CAP = 10
SETTINGS = "weekly.json"        # data/weekly.json: {"demo_target": 10, "demo_risk_cap_pct": 10}
LABDEMO = "labdemo"             # the key the lab-demo roster rides under in the modes dict
CONTROL_FAMILY = "CONTROL_TIME_OF_DAY"
# a bot's market for the diversity cap (each trades its own; the Runner and Band Breaker both indices)
BOT_MARKET = {"runner": "INDEX", "bandbreaker": "INDEX", "crowd": "CROWD", "ian": "IAN"}

DIR = "weekly"
STATE, LATEST, HISTORY = "state.json", "latest.json", "history.jsonl"

# plan statuses
PLANNED, APPLIED, VETOED, NONE, SKIPPED, EXPIRED = "planned", "applied", "vetoed", "none", "skipped", "expired"

# Home's chips
MAKING, LOSING, TOO_EARLY = "MAKING MONEY", "LOSING", "TOO EARLY"
NOTHING_PROVED = "None yet: nothing has proved itself"
NEEDS_LIVE_PATH = "picked, needs a live path (being built)"

# The four bots: modes name -> (verdicts' id, label, magic, the process that runs it)
BOTS = {"runner": ("momentum_runner", "Momentum Runner", 990_511, "trader"),
        "bandbreaker": ("band_breaker", "Band Breaker", 990_611, "trader"),
        "crowd": ("crowd_fader", "Crowd Fader", 990_711, "trader"),
        "ian": ("financial_ian", "Financial Ian", 990_911, "ian")}
TNB_LABEL, TNB_MAGIC, LAB_LABEL = "Trend & Breakout", 990_311, "The Lab"
# each bot's own sqlite (for its risk a trade and, when the page is down, its LIVE rows)
OWN_DB = {"runner": "runner.sqlite", "bandbreaker": "bandbreaker.sqlite", "crowd": "crowd.sqlite", "ian": "ian.sqlite"}


# ------------------------------------------------------------- time --
def uk(t: dt.datetime) -> dt.datetime:
    return to_utc(t).astimezone(TZ_LONDON)


def uk_at(d: dt.date, hm: tuple[int, int]) -> dt.datetime:
    """``hm`` UK time on UK day ``d``, as UTC (BST or GMT as that day has it)."""
    return dt.datetime(d.year, d.month, d.day, hm[0], hm[1], tzinfo=TZ_LONDON).astimezone(dt.timezone.utc)


def next_trading_day(d: dt.date) -> dt.date:
    """The first UK trading day (a weekday) after ``d``."""
    d += dt.timedelta(days=1)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d


def is_trading_day(d: dt.date) -> bool:
    return d.weekday() < 5


def last_pick_day(now: dt.datetime) -> dt.date:
    """The latest UK trading day whose pick time (22:30 UK) is at or before ``now``
    (14 Oct: every trading day; it was Fridays only)."""
    d = uk(now).date()
    if not is_trading_day(d) or uk_at(d, PICK_AT) > to_utc(now):
        d -= dt.timedelta(days=1)
        while not is_trading_day(d):
            d -= dt.timedelta(days=1)
    return d


last_pick_friday = last_pick_day             # the name it had while the pick was weekly (older callers)


def next_pick_utc(now: dt.datetime) -> dt.datetime:
    return uk_at(next_trading_day(last_pick_day(now)), PICK_AT)


def release_utc(day: dt.date) -> dt.datetime:
    """06:30 UK on the next UK trading day after the pick's day (Friday's: Monday)."""
    return uk_at(next_trading_day(day), RELEASE_AT)


def expires_utc(day: dt.date) -> dt.datetime:
    """A plan never released (HALT) lapses at the next pick."""
    return uk_at(next_trading_day(day), PICK_AT)


def iso_week(t: dt.datetime) -> str:
    y, w, _ = uk(t).date().isocalendar()
    return f"{y}-W{w:02d}"


def _iso(t: Optional[dt.datetime]) -> Optional[str]:
    return None if t is None else to_utc(t).replace(microsecond=0).isoformat()


def _when(v) -> Optional[dt.datetime]:
    from .verdicts import _when as w
    return w(v) if v else None


# ------------------------------------------------------------- files --
def wdir(data_dir) -> Path:
    return Path(data_dir) / DIR


def _read(path: Path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None


def _write_atomic(path: Path, obj) -> None:
    """Written whole under a temporary name, then renamed into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("." + path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, indent=1, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def max_promotions(data_dir) -> int:
    """13 Oct's ``weekly_max_promotions`` (0 to MAX_PROMOTIONS_CAP), else MAX_PROMOTIONS.
    SUPERSEDED on 14 Oct by ``demo_target`` (the pick fills the demo's free places
    every trading day); kept so an older data/weekly.json still reads."""
    raw = _read(Path(data_dir) / SETTINGS) if data_dir else None
    try:
        n = int(raw["weekly_max_promotions"]) if isinstance(raw, dict) else MAX_PROMOTIONS
    except (KeyError, TypeError, ValueError):
        return MAX_PROMOTIONS
    return n if 0 <= n <= MAX_PROMOTIONS_CAP else MAX_PROMOTIONS


def demo_target(data_dir) -> int:
    """How many tests may be on the demo at once: data/weekly.json's ``demo_target``
    (0 to DEMO_TARGET_MAX; Aaron, 14 Oct: 8 to 12), else DEMO_TARGET (10). A bad
    value reads as the default, never as more."""
    raw = _read(Path(data_dir) / SETTINGS) if data_dir else None
    try:
        n = int(raw["demo_target"]) if isinstance(raw, dict) and "demo_target" in raw else DEMO_TARGET
    except (TypeError, ValueError):
        return DEMO_TARGET
    return n if 0 <= n <= DEMO_TARGET_MAX else DEMO_TARGET


def plan_picks(plan: Optional[dict]) -> list[dict]:
    """A plan's picks as [{"pick", "extras"}], best first (a plan written before
    there could be several holds its one ``pick`` and ``extras``)."""
    if not isinstance(plan, dict):
        return []
    got = plan.get("picks")
    if isinstance(got, list):
        return [x for x in got if isinstance(x, dict) and isinstance(x.get("pick"), dict)]
    return [{"pick": plan["pick"], "extras": list(plan.get("extras") or ())}] if plan.get("pick") else []


def plan_path(data_dir, friday: dt.date) -> Path:
    return wdir(data_dir) / f"plan-{friday.isoformat()}.json"


def veto_path(data_dir, friday: dt.date) -> Path:
    return wdir(data_dir) / f"VETO-{friday.isoformat()}"


class Unreadable(Exception):
    """A file the weekly pick must read could not be: the pass stops, nothing is decided on a guess."""


def load_state(data_dir, strict: bool = False) -> dict:
    """data/weekly/state.json: what the weekly pick has on real money. Missing:
    nothing yet. There but unreadable: ``strict`` raises (the watchdog's pass
    stops, so a damaged file never becomes an empty one); otherwise empty, for
    the page."""
    path = wdir(data_dir) / STATE
    s: object = {}
    if path.exists():
        try:
            s = json.loads(path.read_text())
        except Exception as exc:
            if strict:
                raise Unreadable(f"{path} could not be read ({exc})") from exc
            s = {}
        if not isinstance(s, dict):
            if strict:
                raise Unreadable(f"{path} does not hold a JSON object")
            s = {}
    s.setdefault("schema", SCHEMA)
    s["promoted"] = [p for p in s.get("promoted") or [] if isinstance(p, dict) and p.get("id")]
    s["restart_pending"] = [str(x) for x in s.get("restart_pending") or []]
    return s


def history(data_dir) -> list[dict]:
    out = []
    try:
        lines = (wdir(data_dir) / HISTORY).read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if isinstance(d, dict):
            out.append(d)
    return out


def _append_history(data_dir, event: dict) -> None:
    p = wdir(data_dir) / HISTORY
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def last_demoted(hist: Sequence[dict]) -> dict:
    """{candidate id: when it was last taken back to PAPER, or last vetoed before it went live}: its evidence
    window starts again there (review, 14 Oct: a veto restarts it as a demotion does, so the next day's pick
    never takes the same test again on the same record)."""
    out: dict = {}
    for e in hist:
        if e.get("event") in ("demote", "veto") and _when(e.get("ts_utc")):
            out[str(e.get("id"))] = _when(e["ts_utc"])
    return out


def promotions_in_week(hist: Sequence[dict], week: str) -> list[dict]:
    return [e for e in hist if e.get("event") == "promote" and e.get("week") == week]


def picks_in_week(hist: Sequence[dict], week: str, other_than: str = "") -> int:
    """The week's promotions that were picks (a part that only rode along with a
    pick is not one), leaving out those of the plan ``other_than`` (a Friday)."""
    n = 0
    for e in promotions_in_week(hist, week):
        if other_than and e.get("plan") == other_than:
            continue
        if e.get("pick", not str(e.get("why") or "").startswith("proved itself too")):
            n += 1
    return n


def halted(data_dir) -> bool:
    return (Path(data_dir) / "HALT").exists()


# -------------------------------------------------------- config.json --
def read_config(config_path) -> dict:
    """config.json with ``json`` alone (never ``Config.load``, which opens the secrets file)."""
    raw = _read(Path(config_path))
    return raw if isinstance(raw, dict) else {}


def tnb_live_now(raw: dict) -> tuple[str, frozenset, frozenset]:
    """(Trend & Breakout's mode, the kinds kept LIVE, the approaches that go
    real there - every approach when the list names none) as the trader reads
    config.json."""
    from ..engine.tactics import APPROACH_NAMES
    from .modes import tnb_live_in_force, tnb_mode_of, tnb_tactics_of
    mode = tnb_mode_of(raw)
    groups, _since = tnb_live_in_force(raw)
    tactics = tnb_tactics_of(raw) or tuple(APPROACH_NAMES)
    return mode, frozenset(groups), frozenset(tactics if groups else ())


def read_config_strict(config_path) -> dict:
    """config.json as the modes tool reads it before writing: missing or broken raises."""
    from .modes import _read_config
    return _read_config(Path(config_path))


UNREADABLE = "UNREADABLE"          # a bot's own file is there but broken: its mode is not known


def strict_view(config_path) -> tuple[dict, dict]:
    """(config.json, every bot's mode), for deciding anything. config.json
    must be readable (the modes tool's own reader), else it raises - never a
    mode guessed from a broken file. Financial Ian's own file is read the same
    way, but a broken ian.json makes only Ian's mode UNREADABLE (``unknown``):
    it blocks Ian's own decisions, never another's. rider.json is not the
    weekly pick's (the Rapid Momentum Rider is not a candidate) and is never
    a reason to stop. The lab-demo roster (data/labdemo/roster.json) rides in
    the modes under LABDEMO, the ids of the lab ideas on the demo; a broken one
    makes only the lab ideas' modes UNREADABLE."""
    from .modes import FILES, _data_dir, _read_json, read_modes
    cfg = Path(config_path)
    raw = read_config_strict(cfg)
    modes = read_modes(cfg)
    try:
        _read_json(_data_dir(raw, cfg) / FILES["ian"])   # raises on a broken file (missing is PAPER)
    except ValueError:
        modes["ian"] = UNREADABLE
    modes[LABDEMO] = lab_roster_ids(_data_dir(raw, cfg), strict=True)
    modes["__data__"] = str(_data_dir(raw, cfg))
    return raw, modes


def lab_roster_ids(data_dir, strict: bool = False):
    """The lab ideas on the demo (frozenset of candidate ids), or UNREADABLE when
    ``strict`` and the roster (or its magic numbers) cannot be read."""
    from ..labdemo import roster as LR
    try:
        LR.read_magics(data_dir, strict=strict)
        return frozenset(LR.read_roster(data_dir, strict=strict))
    except LR.RosterUnreadable:
        return UNREADABLE


def unknown(cid_or_item, modes: dict) -> bool:
    """Is this candidate's mode not known (its own file broken)? Then nothing is decided for it."""
    cid = cid_or_item.get("id", "") if isinstance(cid_or_item, dict) else str(cid_or_item)
    if cid.startswith("lab:"):
        return modes.get(LABDEMO) == UNREADABLE
    return cid.startswith("bot:") and str(modes.get(cid[4:]) or "") == UNREADABLE


def bot_modes(config_path) -> dict:
    try:
        from .modes import _data_dir, read_modes
        out = read_modes(config_path)
        out[LABDEMO] = lab_roster_ids(_data_dir(read_config(config_path), Path(config_path)))
        return out
    except Exception:
        return {}


def is_live(cid: str, raw: dict, modes: dict) -> bool:
    """Is this candidate on the demo in config.json (or its own file, or the lab-demo roster) now?"""
    kind, rest = cid.split(":", 1) if ":" in cid else (cid, "")
    if kind == "bot":
        return str(modes.get(rest) or "").upper() == "LIVE"
    if kind == "tnb":
        tactic, group = rest.split(":", 1)
        mode, groups, tactics = tnb_live_now(raw)
        return mode == "LIVE" or (group in groups and tactic in tactics)
    if kind == "lab":
        ids = modes.get(LABDEMO)
        return isinstance(ids, frozenset) and rest in ids
    return False


# ------------------------------------------------------------- luck --
_BOOT_CACHE: dict = {}


def _means(nets: Sequence[float], ident: str, resamples: int = RESAMPLES) -> list:
    """``resamples`` resampled averages of the per-trade nets, sorted, the seed fixed by the id."""
    from ..lab.stats import seed_of
    n = len(nets)
    seed = seed_of(ident)
    try:
        import numpy as np
        arr = np.asarray(nets, dtype=float)
        rng = np.random.default_rng(seed)
        return sorted(arr[rng.integers(0, n, size=(resamples, n))].mean(axis=1).tolist())
    except ImportError:                                  # the same test, slower
        rnd = random.Random(seed).randrange
        return sorted(sum(nets[rnd(n)] for _ in range(n)) / n for _ in range(resamples))


def _quantile(sorted_vals: list, q: float) -> float:
    try:
        import numpy as np
        return float(np.quantile(np.asarray(sorted_vals), q))
    except ImportError:
        return sorted_vals[min(len(sorted_vals) - 1, int(q * (len(sorted_vals) - 1)))]


def bootstrap_all(nets: Sequence[float], ident: str, resamples: int = RESAMPLES) -> tuple:
    """(chance the true average is <= 0, the LCB_QUANTILE average, the UCB_QUANTILE
    average). Fewer than two trades: (1.0, None, None). Remembered."""
    nets = [float(x) for x in nets]
    if len(nets) < 2:
        return 1.0, None, None
    key = (ident, tuple(nets), resamples)
    if key in _BOOT_CACHE:
        return _BOOT_CACHE[key]
    means = _means(nets, ident, resamples)
    p = sum(1 for m in means if m <= 0) / len(means)
    out = (round(p, 4), round(_quantile(means, LCB_QUANTILE), 4), round(_quantile(means, UCB_QUANTILE), 4))
    if len(_BOOT_CACHE) > 4096:                          # ~640 tests in the pool: room for them all
        _BOOT_CACHE.clear()
    _BOOT_CACHE[key] = out
    return out


def bootstrap(nets: Sequence[float], ident: str, resamples: int = RESAMPLES) -> tuple[float, Optional[float]]:
    """(the chance the true average a trade is <= 0, the cautious average a
    trade) from ``resamples`` resamples of the per-trade nets, with the seed
    fixed by the id: the share of resampled averages at or below zero, and
    their LCB_QUANTILE. Fewer than two trades: (1.0, None). Remembered."""
    p, lcb, _ucb = bootstrap_all(nets, ident, resamples)
    return p, lcb


# ----------------------------------------------------- the early bar (14 Oct) --
def r_of(t: dict) -> float:
    """A trade's R: its net after costs / the money it risked (FALLBACK_RISK_GBP when no record says)."""
    try:
        risk = float(t.get("risk") or 0.0)
    except (TypeError, ValueError):
        risk = 0.0
    if not (math.isfinite(risk) and risk > 0):
        risk = FALLBACK_RISK_GBP
    return float(t["net"]) / risk


def trading_days_between(a: dt.date, b: dt.date) -> int:
    """UK trading days from ``a`` to ``b``, both counted (at least 1)."""
    if b < a:
        return 1
    n = sum(1 for k in range((b - a).days + 1) if is_trading_day(a + dt.timedelta(days=k)))
    return max(1, n)


def needed_trades(per_day: float) -> int:
    """Forward paper trades needed for the early bar, by how often the test trades."""
    if per_day >= FREQUENT_PER_DAY:
        return NEED_FREQUENT
    if per_day >= SLOW_PER_DAY:
        return NEED_MEDIUM
    return NEED_SLOW


def frequency_words(per_day: float) -> str:
    return "frequent" if per_day >= FREQUENT_PER_DAY else "medium" if per_day >= SLOW_PER_DAY else "slow"


def shrink(mean: float, se2: float, prior: tuple) -> float:
    """Empirical-Bayes shrinkage of a test's mean R a trade toward the pool's:
    B = se2 / (se2 + tau2); shrunk = B x pool mean + (1 - B) x its own mean. A test
    with few or noisy trades is pulled most of the way to the pool's average."""
    mu0, tau2 = prior
    b = se2 / (se2 + tau2) if (se2 + tau2) > 0 else 1.0
    return b * mu0 + (1.0 - b) * mean


def r_noise(rs: Sequence[float]) -> tuple:
    """(mean R a trade, its sampling variance for its OWN shrinkage, its sampling
    variance for the POOL's prior) of one test's R values; (None, None, None) with none.

    Both variances are the sample variance (n - 1 under it, unbiased) divided by n.
    The first takes the test's R spread as at least VAR_FLOOR_R2 (VAR_FLOOR_R2 / 1 with
    a single trade), so a short lucky streak whose results barely differ never earns
    the weight of a steady record. The second (15 Oct) has no floor: short lab records
    are lumpy (five losses in a row have almost no spread), and flooring them in the
    pool's average noise pulled tau2 down; it is None under two trades."""
    n = len(rs)
    if not n:
        return None, None, None
    mean = sum(rs) / n
    var = sum((x - mean) ** 2 for x in rs) / (n - 1) if n >= 2 else None
    own = max(VAR_FLOOR_R2 if var is None else var, VAR_FLOOR_R2) / n
    return mean, own, (None if var is None else var / n)


def trades_key(trades: Sequence[dict]) -> Optional[str]:
    """A fingerprint of a trade list: each trade's close time, market and R, in
    close order. Two tests with the same key are exact clones (the same trades);
    the pool's prior counts them once. None for no trades."""
    if not trades:
        return None
    rows = sorted((to_utc(t["closed"]).isoformat(), str(t.get("symbol") or ""), round(r_of(t), 6)) for t in trades)
    return hashlib.sha1(repr(rows).encode("utf-8")).hexdigest()[:16]


def pool_prior(cands: Sequence[dict], floor: Optional[float] = None) -> tuple:
    """(pool mean R a trade, spread of the true means tau2) by the method of
    moments over the distinct tests with at least POOL_MIN_TRADES trades: tau2 = the
    variance of their means less their average sampling variance (never under ``floor``,
    TAU2_FLOOR by default: the promotion rule in force; 0 shows the raw estimate).
    With fewer than POOL_MIN_SIZE such tests: the fixed prior (0, TAU2_DEFAULT).

    15 Oct: a test's sampling variance HERE is its own unbiased one with no
    VAR_FLOOR_R2 (``se2_pool_r``; ``se2_r`` when a record has no such figure). The
    floor stays in each test's own shrinkage (``_finish`` uses ``se2_r``). Exact clones
    (the same ``trades_key``) count once, in the mean, the spread and the pool's size."""
    pts, seen = [], set()
    for c in cands:
        if c.get("mean_r") is None or int(c["stats"]["trades"] or 0) < POOL_MIN_TRADES:
            continue
        key = c.get("trades_key")
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        v = c.get("se2_pool_r")
        pts.append((c["mean_r"], c["se2_r"] if v is None else v))
    if len(pts) < POOL_MIN_SIZE:
        return 0.0, TAU2_DEFAULT
    ms = [m for m, _ in pts]
    mu0 = sum(ms) / len(ms)
    var = sum((m - mu0) ** 2 for m in ms) / (len(ms) - 1)
    tau2 = max(TAU2_FLOOR if floor is None else float(floor), var - sum(v for _, v in pts) / len(pts))
    return round(mu0, 6), round(tau2, 6)


def control_bar(cands: Sequence[dict]) -> Optional[float]:
    """The luck benchmark: the best shrunk R a trade among the control ideas with at
    least CONTROL_MIN_TRADES trades (None when none has that many yet)."""
    vals = [c["shrunk_r"] for c in cands if c.get("control") and c.get("shrunk_r") is not None
            and int(c["stats"]["trades"] or 0) >= CONTROL_MIN_TRADES]
    return round(max(vals), 4) if vals else None


def _raw(trades: Sequence[dict], ident: str, now: dt.datetime) -> dict:
    """Everything about one test's trades that does not depend on the pool."""
    from . import verdicts as V
    s = V.stats(trades)
    trades = sorted(trades, key=lambda t: to_utc(t["closed"]))
    nets = [float(t["net"]) for t in trades]
    p, lcb, _ucb = bootstrap_all(nets, ident)
    first = to_utc(trades[0]["closed"]) if trades else None
    span = max(7.0, (to_utc(now) - first).total_seconds() / 86400.0) if first else 7.0
    per_week = round(s["trades"] * 7.0 / span, 2)
    tdays = trading_days_between(uk(first).date(), uk(now).date()) if first else 1
    per_day = round(len(trades) / tdays, 2) if trades else 0.0
    mean_r, se2, se2_pool = r_noise([r_of(t) for t in trades])
    days = len({uk(to_utc(t["closed"])).date() for t in trades})
    best = max(nets) if nets else 0.0
    wins = sum(x for x in nets if x > 0)
    losses = -sum(x for x in nets if x < 0)
    pf = (wins / losses) if losses > 0 else (math.inf if wins > 0 else 0.0)
    return {"stats": s, "verdict": V.decide(s)[0], "luck": p, "lcb": lcb, "per_week": per_week, "per_day": per_day,
            "need": needed_trades(per_day), "days_traded": days, "pf": pf,
            "net_less_best": round(sum(nets) - best, 2) if nets else 0.0,
            "mean_r": None if mean_r is None else round(mean_r, 4), "se2_r": None if se2 is None else round(se2, 6),
            "se2_pool_r": None if se2_pool is None else round(se2_pool, 8), "trades_key": trades_key(trades)}


def _finish(c: dict, prior: tuple, bar: Optional[float], weight: float = 1.0, control: bool = False) -> dict:
    """The early bar and the score, once the pool is known. Updates and returns ``c``."""
    s = c["stats"]
    n = int(s["trades"] or 0)
    shrunk = None if c.get("mean_r") is None else round(shrink(c["mean_r"], c["se2_r"], prior), 4)
    why = []
    if n < c["need"]:
        why.append(f"needs {c['need'] - n} more trade{'' if c['need'] - n == 1 else 's'} "
                   f"({c['need']} for a {frequency_words(c['per_day'])} test)")
    if c["days_traded"] < MIN_DAYS:
        k = MIN_DAYS - c["days_traded"]
        why.append(f"needs {k} more UK day{'' if k == 1 else 's'} of trades")
    if n and s["net"] <= 0:
        why.append("not positive after costs")
    elif n and c["pf"] < MIN_PF:
        why.append(f"profit factor {c['pf']:.2f}, under {MIN_PF}")
    if n and s["net"] > 0 and c["net_less_best"] <= 0:
        why.append("its profit hangs on one trade")
    if shrunk is not None and n and shrunk <= 0:
        why.append(f"shrunk toward the pool it is {shrunk:+.2f} R a trade")
    if shrunk is not None and bar is not None and shrunk < bar + CONTROL_MARGIN_R:
        why.append(f"does not clearly beat the best clock trade ({shrunk:+.2f} R vs {bar:+.2f} R)")
    if control:
        why.append("a control (the luck benchmark) is never promoted")
    passes = n > 0 and not why
    score = None if shrunk is None else round(shrunk * c["per_week"] * weight, 3)
    c.update(shrunk_r=shrunk, control_bar=bar, weight=weight, control=control, score=score, passes=passes,
             prior=list(prior),
             why=(f"passes the bar: {n} trades over {c['days_traded']} UK days, profit factor "
                  f"{min(c['pf'], 99.0):.2f}, shrunk {shrunk:+.2f} R a trade"
                  + (f" (the best clock trade {bar:+.2f} R)" if bar is not None else "")) if passes
             else "; ".join(why) or "no trades yet")
    return c


def judge(trades: Sequence[dict], ident: str, now: dt.datetime, prior: Optional[tuple] = None,
          bar: Optional[float] = None, weight: float = 1.0, control: bool = False) -> dict:
    """The early bar and the score of one candidate's trades (already the window's):
    {stats, verdict (13 Oct's words, shown), luck, lcb, per_week, per_day, need, mean_r,
    shrunk_r, control_bar, weight, score, passes, why}. On its own (no pool) it is
    shrunk toward the fixed prior and has no control benchmark; ``candidates`` judges
    the whole pool together (``score_pool``)."""
    return _finish(_raw(trades, ident, now), prior or (0.0, TAU2_DEFAULT), bar, weight, control)


def score_pool(cands: list[dict]) -> list[dict]:
    """The pool's prior and the control benchmark, then every test's bar and score."""
    prior = pool_prior(cands)
    for c in cands:                                       # the controls first, against the pool
        if c.get("control"):
            _finish(c, prior, None, c.get("weight", 1.0), True)
    bar = control_bar(cands)
    for c in cands:
        _finish(c, prior, bar, c.get("weight", 1.0), bool(c.get("control")))
    return cands


def lab_weight(tests: Sequence[dict]) -> tuple[float, str]:
    """The past and fresh lab tests as a weight on the rank (never a gate): each one
    strongly negative (20+ trades, losing, profit factor under 0.8) halves it, each
    mildly negative (losing) takes 15% off. Only the latest of each kind counts."""
    w, words = 1.0, []
    latest: dict = {}
    for t in tests:
        k = str(t.get("kind") or "")
        if k in ("past", "fresh") and (k not in latest or str(t.get("finished_utc") or "") >=
                                       str(latest[k].get("finished_utc") or "")):
            latest[k] = t
    for k, t in sorted(latest.items()):
        try:
            net, n = float(t.get("net_gbp")), int(t.get("trades") or 0)
        except (TypeError, ValueError):
            continue
        pf = t.get("pf")
        if net < 0 and n >= 20 and pf is not None and float(pf) < 0.8:
            w *= WEIGHT_STRONG_NEG
            words.append(f"its {k} test was strongly negative")
        elif net < 0:
            w *= WEIGHT_MILD_NEG
            words.append(f"its {k} test lost")
    return round(w, 4), "; ".join(words)


# ------------------------------------------------------- the candidates --
def window_start(now: dt.datetime, demoted: Optional[dt.datetime] = None) -> dt.datetime:
    start = to_utc(now) - dt.timedelta(days=LOOKBACK_DAYS)
    return max(start, to_utc(demoted)) if demoted is not None else start


def _rows(path: Path, sql: str, args: tuple = ()) -> list[dict]:
    from .attribution import _rows_ro
    return _rows_ro(Path(path), sql, args)


def _median_risk(values: Iterable) -> float:
    vals = []
    for v in values:
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f) and f > 0:
            vals.append(f)
    return round(median(vals), 2) if vals else FALLBACK_RISK_GBP


def bot_trades(data_dir, name: str, start: dt.datetime, end: dt.datetime) -> list[dict]:
    from . import verdicts as V
    return V.practice_trades(data_dir, BOTS[name][0], start, end)


def tnb_part_trades(data_dir, start: dt.datetime, end: dt.datetime, record=None) -> dict:
    """{(approach, kind): [trades]} - Trend & Breakout's practice trades
    (its paper record, ``verdicts.practice_trades``) by the approach its
    journal booked (``engine.tactics.approach_of``) and the kind of market
    (``coach.records.kind_of``). Each trade carries its ``risk``."""
    from . import verdicts as V
    from ..coach.records import kind_of
    from ..engine.tactics import approach_of
    trades = V.practice_trades(data_dir, V.TNB, start, end, record)
    if not trades:
        return {}
    j = {int(r.get("ticket") or 0): r for r in _rows(
        Path(data_dir) / "journal.sqlite", "SELECT ticket, tactic, risk_money FROM trades WHERE closed_utc >= ?",
        ((to_utc(start) - dt.timedelta(days=2)).isoformat(),))}
    out: dict = {}
    for t in trades:
        r = j.get(int(t["ticket"]))
        if r is None:
            continue                                      # its approach is not known: no part to put it in
        a = approach_of(str(r.get("tactic") or ""))
        if not a:
            continue
        out.setdefault((a, kind_of(t["symbol"])), []).append(dict(t, risk=r.get("risk_money")))
    return out


def lab_trades(data_dir, start: dt.datetime, end: dt.datetime, stages=None) -> list[tuple[dict, list]]:
    """[(the idea's row, its practice trades)] for EVERY lab idea (or only those
    at ``stages``), read-only, each row with ``short``: its one-line name built
    from its own definition (``candidate.short_name_of``). Nothing for a made-up
    (synthetic) lab. The trades are the PRACTICE rows the practice process
    wrote; an idea with none in the window comes with an empty list."""
    data = Path(data_dir)
    summ = _read(data / "lab" / "summary.json")
    if isinstance(summ, dict) and summ.get("synthetic"):
        return []
    db = data / "lab" / "lab.sqlite"
    if stages:
        stages = tuple(stages)
        q = ",".join("?" for _ in stages)
        ideas = _rows(db, f"SELECT * FROM candidates WHERE stage IN ({q})", stages)
    else:
        ideas = _rows(db, "SELECT * FROM candidates")
    ideas = [i for i in ideas if not i.get("retired_utc")]     # the weekly refresh took it off the pool
    if not ideas:
        return []
    from ..coach.records import _is_test_row
    rows = _rows(db, "SELECT * FROM practice_trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND "
                     "closed_utc < ? AND (mode IS NULL OR mode='PRACTICE')",
                 (to_utc(start).isoformat(), to_utc(end).isoformat()))
    by: dict = {}
    for r in rows:
        t = _when(r.get("closed_utc"))
        if t is None or _is_test_row(r) or r.get("net_gbp") is None:
            continue
        c = r.get("commission_gbp")
        by.setdefault(str(r.get("candidate_id")), []).append(
            {"closed": t, "net": round(float(r["net_gbp"]), 2), "costs": None if c is None else abs(float(c)),
             "symbol": str(r.get("symbol") or ""), "ticket": int(r.get("ticket") or 0), "kind": "practice",
             "risk": r.get("risk_gbp")})
    for i in ideas:
        i["short"] = lab_short_name(i)
    return [(i, by.get(str(i["id"]), [])) for i in ideas]


lab_ready_trades = lab_trades                 # the name it had while only READY FOR LIVE ideas were read


def lab_short_name(row: dict) -> str:
    """'Breakout · majors · 60 min, London · 2:1': family, markets, key settings,
    exit, from the idea's own stored definition; its long name, else its id, when
    the definition cannot be read."""
    try:
        from ..lab.candidate import short_name_of
        from ..lab.stages import cand_of
        return short_name_of(cand_of(row))
    except Exception:
        return str(row.get("name") or row.get("id") or "lab idea")


def _kind_words(kind: str) -> str:
    from .verdicts import _KIND_WORDS
    return _KIND_WORDS.get(kind, kind.lower().replace("_", " "))


def part_words(tactic: str, kind: str) -> str:
    """'momentum continuation, minor pairs'."""
    return f"{tactic.lower().replace('_', ' ')}, {_kind_words(kind)}"


def lab_direction(row: dict) -> str:
    """LONG or SHORT for an idea that trades one side only (its ``side`` setting), else BOTH."""
    try:
        from ..lab.stages import cand_of
        side = str(cand_of(row).p("side") or "").upper()
    except Exception:
        return "BOTH"
    return {"BUY": "LONG", "SELL": "SHORT", "LONG": "LONG", "SHORT": "SHORT"}.get(side, "BOTH")


def lab_tests(data_dir) -> dict:
    """{idea id: [its past and fresh test rows]} (read-only), for the rank's weight."""
    out: dict = {}
    for r in _rows(Path(data_dir) / "lab" / "lab.sqlite",
                   "SELECT candidate_id, kind, trades, net_gbp, pf, finished_utc FROM tests WHERE status='done'"):
        out.setdefault(str(r.get("candidate_id")), []).append(r)
    return out


def div_key(c: dict) -> str:
    """The diversity cap's key: its market and direction ("FX_MAJOR/BOTH")."""
    return f"{c.get('market') or '?'}/{c.get('direction') or 'BOTH'}"


def candidates(data_dir, config_path, now: dt.datetime, hist: Optional[Sequence[dict]] = None,
               lab_stages: Optional[Sequence[str]] = None, keep_empty: bool = False) -> list[dict]:
    """Every practising candidate, judged on its window, ranked: qualifying
    ones first by score, then the rest by practice GBP after costs. Every lab
    idea is its own candidate (``lab_stages`` narrows them to some stages).
    ``keep_empty``: a test with no trade in the window is kept too (Home counts
    those as waiting for a trade); the pick never needs them. The whole pool is
    judged together (``score_pool``: the shrinkage prior and the control benchmark)."""
    from ..engine.tactics import APPROACH_NAMES
    from .modes import LIVE_KINDS
    now = to_utc(now)
    end = now + dt.timedelta(seconds=1)
    dem = last_demoted(hist if hist is not None else history(data_dir))
    raw = read_config(config_path) if config_path else {}
    modes = bot_modes(config_path) if config_path else {}
    out: list[dict] = []

    def add(cid, kind, label, part_text, trades, live_path, **extra):
        if not trades and (not keep_empty or (kind == "bot" and extra.get("mode") != "PAPER")):
            return                                        # a bot not on PAPER is not waiting to practise
        j = _raw(trades, cid, now)
        c = dict({"id": cid, "kind": kind, "label": label, "part": part_text, "live_path": live_path,
                  "live": is_live(cid, raw, modes), "risk_gbp": _median_risk(t.get("risk") for t in trades)},
                 **j, **extra)
        c["div"] = div_key(c)
        out.append(c)

    oldest = window_start(now)
    for name, (_vid, label, magic, process) in BOTS.items():
        cid = f"bot:{name}"
        try:
            trades = bot_trades(data_dir, name, window_start(now, dem.get(cid)), end)
            risks = {int(r.get("ticket") or 0): r.get("risk_money") for r in _rows(
                Path(data_dir) / OWN_DB[name], "SELECT ticket, risk_money FROM trades WHERE closed_utc >= ?",
                (oldest.isoformat(),))}
            trades = [dict(t, risk=risks.get(int(t["ticket"]))) for t in trades]
            add(cid, "bot", label, "", trades, True, bot=name, magic=magic, process=process,
                mode=str(modes.get(name) or "").upper(), market=BOT_MARKET.get(name, name.upper()),
                session="ALL", direction="BOTH")
        except Exception as exc:                          # one broken record never stops the rest
            log.warning("weekly: %s: %s", name, exc)
    try:
        for (tactic, kind), trades in sorted(tnb_part_trades(data_dir, oldest, end).items()):
            cid = f"tnb:{tactic}:{kind}"
            d = dem.get(cid)
            trades = [t for t in trades if d is None or to_utc(t["closed"]) >= to_utc(d)]
            path = tactic in APPROACH_NAMES and kind in LIVE_KINDS
            add(cid, "tnb", TNB_LABEL, part_words(tactic, kind), trades, path, tactic=tactic, group=kind,
                magic=TNB_MAGIC, process="trader", mode=tnb_live_now(raw)[0], market=kind, session="ALL",
                direction="BOTH")
    except Exception as exc:
        log.warning("weekly: Trend & Breakout's parts: %s", exc)
    try:
        tests = lab_tests(data_dir)
        for idea, trades in lab_trades(data_dir, oldest, end, lab_stages):
            cid = f"lab:{idea['id']}"
            d = dem.get(cid)                              # 14 Oct: after a demotion only newer practice counts
            trades = [t for t in trades if d is None or to_utc(t["closed"]) >= to_utc(d)]
            family = str(idea.get("family") or "")
            w, wwhy = lab_weight(tests.get(str(idea["id"]), ()))
            market, session = lab_market_session(idea)
            add(cid, "lab", LAB_LABEL, str(idea.get("short") or idea.get("name") or idea["id"]),
                trades, family != CONTROL_FAMILY, stage=str(idea.get("stage") or ""), full=str(idea.get("name") or ""),
                version=int(idea.get("version") or 1), family=family, market=market, session=session,
                direction=lab_direction(idea), weight=w, weight_why=wwhy, control=family == CONTROL_FAMILY,
                process=None)
    except Exception as exc:
        log.warning("weekly: the lab: %s", exc)
    return rank(score_pool(out))


def lab_market_session(row: dict) -> tuple[str, str]:
    try:
        from ..lab.stages import cand_of
        c = cand_of(row)
        return c.markets, c.session
    except Exception:
        return "?", "?"


def rank(cands: list[dict]) -> list[dict]:
    """Qualifying first, by score (highest first); the rest by their window's GBP after costs."""
    def key(c):
        q = bool(c.get("passes")) and c.get("score") is not None
        return (0 if q else 1, -(c.get("score") or 0.0) if q else -float(c["stats"]["net"]), c["id"])
    return sorted(cands, key=key)


# ------------------------------------------------- Trend & Breakout's cross --
def tnb_add(tactic: str, kind: str, groups: frozenset, tactics: frozenset, proven: set) -> dict:
    """What adding one part means for tnb.live_groups x tnb.live_tactics (a
    cross product): {ok, groups, tactics (None: leave the list as it is),
    extras (other parts it makes live), why}. Refused when any other part it
    would make live has not passed the bar itself (``proven``: ids)."""
    from ..engine.tactics import APPROACH_NAMES
    every = frozenset(APPROACH_NAMES)
    new_g = frozenset(groups | {kind})
    new_t = frozenset((tactics if groups else frozenset()) | {tactic})
    before = {(t, g) for g in groups for t in tactics}
    extras = sorted(f"tnb:{t}:{g}" for g in new_g for t in new_t
                    if (t, g) not in before and (t, g) != (tactic, kind))
    unproven = [x for x in extras if x not in proven]
    if unproven:
        words = ", ".join(part_words(*x.split(":")[1:]) for x in unproven[:4])
        return {"ok": False, "groups": new_g, "tactics": new_t, "extras": extras,
                "why": f"it would also make {words} live, which has not proved itself - skipped this week"}
    # the list written whenever it changes (a list that now names every approach is still written: left as it
    # was, the old list would keep the new part on paper); left alone only when it is already what is wanted
    same = bool(groups) and new_t == tactics
    return {"ok": True, "groups": new_g, "tactics": None if same else new_t, "extras": extras, "why": ""}


def tnb_remove(tactic: str, kind: str, groups: frozenset, tactics: frozenset, keep: set) -> dict:
    """What taking one part back means: a cross product cannot leave a hole,
    so either its kind or its approach goes, whichever keeps more of
    ``keep`` (ids of the other promoted parts; a tie drops the kind). {groups,
    tactics (None: leave the list as it is), lost (other parts taken back with it)}."""
    from ..engine.tactics import APPROACH_NAMES
    every = frozenset(APPROACH_NAMES)
    opts = []
    for g, t in ((groups - {kind}, tactics), (groups, tactics - {tactic})):
        if not g or not t:
            g, t = frozenset(), frozenset()
        live = {f"tnb:{a}:{b}" for b in g for a in t}
        opts.append((len(live & keep), g, t))
    _n, g, t = opts[0] if opts[0][0] >= opts[1][0] else opts[1]
    lost = sorted(x for x in keep if x not in {f"tnb:{a}:{b}" for b in g for a in t})
    return {"groups": g, "tactics": None if (t == tactics or t == every or not g) else t, "lost": lost}


# ------------------------------------------------------------- the plan --
def roots(promoted: Sequence[dict]) -> list[dict]:
    """The promotions that hold a place on the demo (a part that only rode along with a pick holds none)."""
    return [p for p in promoted if not p.get("rides_with")]


def slot(p: dict) -> dict:
    """Where a test sits for the diversity cap and the spread: {div, market, session}. A Trend & Breakout part
    whose record does not say (one that rode along with a pick, known by its id alone) sits in its own kind of
    market, both directions, every session, as its candidate does."""
    cid = str(p.get("id") or "")
    if cid.startswith("tnb:") and cid.count(":") == 2 and not (p.get("div") and p.get("market")):
        g = cid.split(":")[2]
        return {"div": f"{g}/BOTH", "market": g, "session": "ALL"}
    return {"div": p.get("div") or div_key(p), "market": p.get("market"), "session": p.get("session")}


def make_plan(data_dir, config_path, now: dt.datetime, friday: Optional[dt.date] = None,
              ranking: Optional[list] = None, limit: Optional[int] = None) -> dict:
    """The day's picks (``friday``: the pick's UK trading day; the name is the one it
    had while the pick was weekly): the ranking, the best qualifiers for the demo's
    free places (``demo_target`` less the tests already on it, unless ``limit``
    says), the reasons, the release time. Never applies anything.

    The queue: every qualifier not already on the demo, best score first, each
    one's place lowered (never its figures) by SPREAD_MARKET for every test already
    on the demo or picked in its market and SPREAD_SESSION in its session; at most
    DIVERSITY_MAX on the demo per market and direction (every part riding with a
    pick counted there too, though it holds no place). Each Trend & Breakout part
    is checked against the live kinds x approaches AS THEY WILL BE after the picks
    before it: a part that would then make an unproven part live is skipped, and a
    part another pick already makes live rides with that pick instead."""
    now = to_utc(now)
    friday = friday or last_pick_day(now)
    try:
        promoted = load_state(data_dir)["promoted"] if data_dir else []
    except Exception:
        promoted = []
    target = demo_target(data_dir)
    room = max(0, target - len(roots(promoted)))
    limit = room if limit is None else int(limit)
    ranking = ranking if ranking is not None else candidates(data_dir, config_path, now)
    raw = read_config(config_path) if config_path else {}
    proven = {c["id"] for c in ranking if c.get("passes")}
    reasons: list[str] = []
    picks: list[dict] = []
    _mode, groups, tactics = tnb_live_now(raw)
    riding: set = set()                                    # parts the picks make live with them
    on_demo = {p["id"] for p in promoted}
    # every test on the demo counts toward the diversity cap and the spread, a part that rode along with a pick
    # too (review, 14 Oct: it trades there like any other; it only holds no place of the demo_target)
    held = [slot(p) for p in promoted]
    queue = []
    for c in ranking:
        if not c.get("passes") or c.get("live") or c["id"] in on_demo:
            continue                                       # not qualified, or already on the demo
        if not c.get("live_path"):
            reasons.append(f"{c['label']} {c['part']}: no demo path for this kind of market - skipped")
            continue
        if c["kind"] in ("bot", "tnb") and c.get("mode") != "PAPER":
            who = TNB_LABEL if c["kind"] == "tnb" else c["label"]
            reasons.append(f"{c['label']}{' ' + c['part'] if c['part'] else ''}: {who} is "
                           f"{c.get('mode') or 'in no known mode'}, not on PAPER - skipped")
            continue
        queue.append(c)
    if limit <= 0 and queue:
        reasons.append(f"the demo squad is full ({len(roots(promoted))} of {target}): "
                       f"{len(queue)} qualifier{'' if len(queue) == 1 else 's'} wait for a place")

    def taken_now() -> list[dict]:
        """The demo as it will be after today's picks so far: each pick and every part riding with it."""
        return held + [slot(y) for x in picks for y in [x["pick"]] + [{"id": e} for e in x["extras"]]]

    def place(c) -> float:
        taken = taken_now()
        k = sum(1 for x in taken if x.get("market") == c.get("market"))
        j = sum(1 for x in taken if x.get("session") == c.get("session") and c.get("session") not in (None, "ALL"))
        return float(c.get("score") or 0.0) * (SPREAD_MARKET ** k) * (SPREAD_SESSION ** j)

    while queue and len(picks) < limit:
        queue.sort(key=lambda c: (-place(c), c["id"]))
        c = queue.pop(0)
        if c["kind"] == "tnb" and c["id"] in riding:      # already counted, with the pick it rides with
            reasons.append(f"{c['label']} {c['part']}: goes live with another of today's picks")
            continue
        taken = taken_now()
        same = sum(1 for x in taken if x["div"] == c["div"])
        if same >= DIVERSITY_MAX:
            reasons.append(f"{c['label']} {c['part']}: {DIVERSITY_MAX} already on the demo in {c['div']} - waits")
            continue
        extras: list = []
        if c["kind"] == "tnb":
            add = tnb_add(c["tactic"], c["group"], groups, tactics, proven)
            if not add["ok"]:
                reasons.append(f"{c['label']} {c['part']}: {add['why']}")
                continue
            # the parts it makes live go on the demo with it: the cap holds for them too (review, 14 Oct)
            new = [c["div"]] + [slot({"id": e})["div"] for e in add["extras"]]
            over = sorted(k for k in set(new) if sum(1 for x in taken if x["div"] == k) + new.count(k) > DIVERSITY_MAX)
            if over:
                reasons.append(f"{c['label']} {c['part']}: with the parts it would make live too, more than "
                               f"{DIVERSITY_MAX} would be on the demo in {', '.join(over)} - waits")
                continue
            extras = add["extras"]
            groups = frozenset(add["groups"])              # the cross product after this pick ...
            tactics = frozenset(add["tactics"]) if add["tactics"] is not None else tactics
            riding.update(extras)                          # ... is what the next part is checked against
        picks.append({"pick": c, "extras": extras})
        reasons.append(f"{c['label']}{' ' + c['part'] if c['part'] else ''}: "
                       + f"pick {len(picks)} of {limit} free place{'' if limit == 1 else 's'}"
                       + f" (about {c['score']:+.2f} R a week, cautiously; {c['why']})")
    if not picks and not reasons:
        reasons.append(NOTHING_PROVED)
    rel = release_utc(friday)
    first = picks[0] if picks else None
    lab_first = next((x for x in picks if x["pick"]["kind"] == "lab"), None)
    return {"schema": SCHEMA, "friday": friday.isoformat(), "day": friday.isoformat(), "made_utc": _iso(now),
            "release_utc": _iso(rel), "expires_utc": _iso(expires_utc(friday)), "week": iso_week(rel),
            "status": PLANNED if picks else NONE, "limit": limit, "demo_target": target,
            "pick": _brief(first["pick"]) if first else None, "extras": first["extras"] if first else [],
            "picks": [{"pick": _brief(x["pick"]), "extras": x["extras"]} for x in picks],
            "lab_pick": _brief(lab_first["pick"]) if lab_first else None,
            "reasons": reasons, "ranking": [_brief(c) for c in ranking[:30]],
            # every bot and Trend & Breakout part, whatever its rank: the lab's Saturday refresh lists the losing
            # ones for the nightly review (it reads this file; the lab never imports this module)
            "bots": [_brief(c) for c in ranking if c.get("kind") in ("bot", "tnb")],
            "rule": RULE_TEXT}


RULE_TEXT = (f"Practice trades of the last {LOOKBACK_DAYS} days, after all costs: at least {NEED_FREQUENT} trades "
             f"for a frequent test (3+ a day), {NEED_MEDIUM} for a medium one, {NEED_SLOW} for a slow one (under 1 a "
             f"day), over {MIN_DAYS}+ UK days; positive, a profit factor of {MIN_PF}+, still positive without its best "
             f"trade; its R a trade shrunk toward the pool's (empirical Bayes) above zero and at least "
             f"{CONTROL_MARGIN_R} R better than the best clock trade (the luck benchmark). Ranked by shrunk R a trade x "
             f"trades a week (its past and fresh lab tests a weight). Picked every UK trading day at 22:30, released "
             f"06:30 UK the next trading day into the demo's free places (demo_target, default {DEMO_TARGET}), at most "
             f"{DIVERSITY_MAX} per market and direction. On the demo: after {JUDGE_AFTER} trades back to practice when "
             f"even the hopeful bootstrap bound of its average is below zero; after {JUDGE_NET_AFTER} when net <= 0; at "
             f"any point at a loss of {EARLY_EXIT_RISKS:g} x its risk a trade or a drawdown of {DD_GUARD_RISKS:g} x.")


def _brief(c: dict) -> dict:
    s = c.get("stats") or {}
    keep = ("id", "kind", "label", "part", "live", "live_path", "mode", "verdict", "luck", "lcb", "per_week", "score",
            "passes", "why", "risk_gbp", "bot", "magic", "process", "tactic", "group", "version", "family", "market",
            "session", "direction", "div", "per_day", "need", "mean_r", "shrunk_r", "control_bar", "weight",
            "control", "stage")
    out = {k: c[k] for k in keep if k in c}
    out.update({"trades": s.get("trades"), "net": s.get("net"), "win_rate": s.get("win_rate"),
                "profit_factor": s.get("profit_factor"), "days": s.get("days")})
    return out


# ------------------------------------------------------- live records --
def fetch_live_positions(config_path, timeout: float = 5.0) -> Optional[list]:
    """MetaTrader's closed positions from the trader's page (``/weekly-live``);
    None when the page does not answer."""
    import urllib.request
    raw = read_config(config_path)
    ops = raw.get("ops") if isinstance(raw.get("ops"), dict) else {}
    from ..config import OpsConfig
    host = str(ops.get("dashboard_host") or OpsConfig().dashboard_host)
    port = int(ops.get("dashboard_port") or OpsConfig().dashboard_port)
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/weekly-live", timeout=timeout) as r:   # noqa: S310
            body = json.loads(r.read().decode("utf-8"))
        rows = body.get("positions") if isinstance(body, dict) and body.get("ok") else None
        return rows if isinstance(rows, list) else None
    except Exception:
        return None


def live_positions_json(deals, open_tickets=None) -> dict:
    """The page's ``/weekly-live``: every closed position of the bots' magics, from MetaTrader's deal rows."""
    from .standing import positions_of
    magics = {TNB_MAGIC} | {m for _v, _l, m, _p in BOTS.values()}
    rows = [{"position": p["position"], "magic": p.get("bot_magic"), "symbol": p.get("symbol"), "net": p["net"],
             "commission": p.get("commission"), "opened": _iso(p.get("opened")), "closed": _iso(p.get("closed"))}
            for p in positions_of(deals or (), open_tickets) if p.get("is_closed") and p.get("bot_magic") in magics]
    return {"ok": True, "positions": rows}


def live_trades(item: dict, positions: Optional[list], data_dir) -> tuple[list[dict], str]:
    """(the promoted item's live trades since its promotion, their source).
    ``positions`` from the page (MetaTrader); None reads the bots' own records."""
    from ..coach.records import kind_of
    from ..engine.tactics import approach_of
    if item.get("kind") == "lab":                          # 14 Oct: its own record of the broker's deals
        return lab_live_trades(item, data_dir)
    since = _when(item.get("since_utc"))
    if since is None:
        return [], ""
    tnb = item.get("kind") == "tnb"
    journal = {}
    if tnb:
        journal = {int(r.get("ticket") or 0): approach_of(str(r.get("tactic") or "")) for r in _rows(
            Path(data_dir) / "journal.sqlite", "SELECT ticket, tactic FROM trades WHERE opened_utc >= ?",
            ((since - dt.timedelta(minutes=5)).isoformat(),))}
    out = []
    if positions is not None:
        for p in positions:
            opened, closed = _when(p.get("opened")), _when(p.get("closed"))
            if opened is None or closed is None or opened < since or int(p.get("magic") or -1) != int(item["magic"]):
                continue
            if tnb and (kind_of(str(p.get("symbol") or "")) != item.get("group")
                        or journal.get(int(p.get("position") or 0)) != item.get("tactic")):
                continue
            out.append({"closed": closed, "net": round(float(p.get("net") or 0.0), 2),
                        "costs": abs(float(p.get("commission") or 0.0)), "symbol": str(p.get("symbol") or ""),
                        "ticket": int(p.get("position") or 0), "kind": "real"})
        return out, "MetaTrader"
    # the page did not answer: the bots' own records (used only to take something back)
    if tnb:
        from .standing import TnbPaperRecord
        rec = TnbPaperRecord(data_dir)
        rows = _rows(Path(data_dir) / "journal.sqlite", "SELECT ticket, symbol, tactic, opened_utc, closed_utc, "
                     "pnl_money FROM trades WHERE closed_utc IS NOT NULL AND opened_utc >= ?", (since.isoformat(),))
        for r in rows:
            if rec.is_paper(r.get("ticket")) or kind_of(str(r.get("symbol") or "")) != item.get("group") \
                    or approach_of(str(r.get("tactic") or "")) != item.get("tactic"):
                continue
            out.append({"closed": _when(r.get("closed_utc")), "net": round(float(r.get("pnl_money") or 0.0), 2),
                        "costs": None, "ticket": int(r.get("ticket") or 0), "kind": "real"})
    elif item.get("bot") in OWN_DB:
        money = "net_pnl"
        rows = _rows(Path(data_dir) / OWN_DB[item["bot"]], "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND "
                     "opened_utc >= ?", (since.isoformat(),))
        for r in rows:
            if str(r.get("mode") or "").upper() != "LIVE":
                continue
            out.append({"closed": _when(r.get("closed_utc")), "net": round(float(r.get(money) or 0.0), 2),
                        "costs": None, "ticket": int(r.get("ticket") or 0), "kind": "real"})
    return [t for t in out if t["closed"] is not None], "its own record (MetaTrader's page did not answer)"


def lab_live_trades(item: dict, data_dir) -> tuple[list[dict], str]:
    """A lab idea's demo trades since its promotion, from data/labdemo/labdemo.sqlite: the
    lab-demo engine books every one from the broker's own closed-deal record."""
    from ..labdemo.record import read_trades
    since = _when(item.get("since_utc"))
    if since is None:
        return [], ""
    rows = read_trades(data_dir, str(item["id"])[4:], to_utc(since).isoformat())
    if rows is None:
        return [], "the lab-demo record could not be read"
    out = []
    for r in rows:
        if item.get("version") is not None and int(r.get("version") or 1) != int(item.get("version") or 1):
            continue
        closed = _when(r.get("closed_utc"))
        if closed is None:
            continue
        out.append({"closed": closed, "net": round(float(r.get("net_gbp") or 0.0), 2),
                    "costs": None, "symbol": str(r.get("symbol") or ""),                 # inside the net
                    "ticket": int(r.get("ticket") or 0), "kind": "real", "risk": r.get("risk_gbp"),
                    "r": r.get("r"), "equiv_1500": r.get("equiv_1500_gbp")})
    return out, "MetaTrader (the lab-demo record of the broker's deals)"


def live_verdict(trades: Sequence[dict], risk_gbp: float, ident: str = "") -> tuple[str, str]:
    """("demote" | "keep" | "wait", why) from a promotion's demo trades (14 Oct):

    * at any point: a demo loss of EARLY_EXIT_RISKS x its risk a trade, or a drawdown
      from its best of DD_GUARD_RISKS x, takes it back;
    * after JUDGE_AFTER trades: back when the bootstrap UCB_QUANTILE bound of its mean
      net a trade is below zero; kept when the LCB_QUANTILE bound is above zero;
    * after JUDGE_NET_AFTER trades: back when net <= 0.
    """
    trades = sorted(trades, key=lambda t: to_utc(t["closed"]))
    n = len(trades)
    nets = [float(t["net"]) for t in trades]
    net = round(sum(nets), 2)
    risk = max(float(risk_gbp or FALLBACK_RISK_GBP), 0.01)
    limit = round(EARLY_EXIT_RISKS * risk, 2)
    if net <= -limit:
        return "demote", (f"its demo loss reached £{-net:.2f}, {EARLY_EXIT_RISKS:g} x its £{risk:.2f} risk a trade "
                          f"(the early exit)")
    peak, run, dd = 0.0, 0.0, 0.0
    for x in nets:
        run += x
        peak = max(peak, run)
        dd = max(dd, peak - run)
    if dd >= DD_GUARD_RISKS * risk:
        return "demote", (f"a drawdown of £{dd:.2f} from its best, {DD_GUARD_RISKS:g} x its £{risk:.2f} risk a trade "
                          f"(the drawdown guard)")
    if n >= JUDGE_AFTER:
        _p, lo, hi = bootstrap_all(nets, ident or "demo")
        if hi is not None and hi < 0:
            return "demote", (f"{n} demo trades made £{net:+.2f} after costs; even the hopeful bound of its average, "
                              f"£{hi:+.2f} a trade, is below zero")
        if n >= JUDGE_NET_AFTER and net <= 0:
            return "demote", f"{n} demo trades made £{net:.2f} after costs (not in profit after {JUDGE_NET_AFTER})"
        if lo is not None and lo > 0:
            return "keep", (f"{n} demo trades made £{net:+.2f} after costs; even the cautious bound of its average, "
                            f"£{lo:+.2f} a trade, is above zero: it stays")
        return "wait", f"{n} demo trades, £{net:+.2f} so far: the evidence is not clear yet, it keeps going"
    return "wait", f"{n} of {JUDGE_AFTER} demo trades, £{net:+.2f} so far"


# --------------------------------------------------------- applying --
SetModes = Callable[..., dict]


def _set_modes(config_path, changes: dict, **kw) -> None:
    from .modes import set_modes
    set_modes(config_path, changes, **kw)


def promote_change(item: dict, raw: dict, modes: dict, proven: set) -> dict:
    """What switching one candidate LIVE writes, worked out without writing:
    {already, changes, kw (set_modes' keywords), extras}. Only a bot on PAPER
    is promoted (never one switched OFF), and a Trend & Breakout part only
    while Trend & Breakout is on PAPER. Raises on a refusal."""
    if item.get("kind") == "bot":
        mode = str(modes.get(item["bot"]) or "").upper()
        if mode == UNREADABLE:
            raise ValueError(f"{item['label']}'s own file could not be read: its mode is not known")
        if mode == "LIVE":
            return {"already": True, "changes": {}, "kw": {}, "extras": []}
        if mode != "PAPER":
            raise ValueError(f"{item['label']} is {mode or 'in no known mode'}, not on PAPER: only a bot that is "
                             f"practising is promoted")
        return {"already": False, "changes": {item["bot"]: "LIVE"}, "kw": {}, "extras": []}
    if item.get("kind") == "tnb":
        mode, groups, tactics = tnb_live_now(raw)
        if mode != "PAPER":
            raise ValueError("Trend & Breakout is LIVE as a whole, not on PAPER: no part of it is promoted")
        if item["group"] in groups and item["tactic"] in tactics:
            return {"already": True, "changes": {}, "kw": {}, "extras": []}
        add = tnb_add(item["tactic"], item["group"], groups, tactics, proven)
        if not add["ok"]:
            raise ValueError(add["why"])
        return {"already": False, "changes": {}, "extras": add["extras"],
                "kw": {"tnb_live_groups": sorted(add["groups"]),
                       "tnb_live_tactics": sorted(add["tactics"]) if add["tactics"] is not None else None}}
    if item.get("kind") == "lab":
        return lab_promote_change(item, modes, modes.get("__data__") or "")
    raise ValueError(NEEDS_LIVE_PATH)


def demote_change(item: dict, raw: dict, modes: dict, keep: set) -> dict:
    """What taking one promotion back to PAPER writes, without writing:
    {already (nothing of ours to take back), changes, kw, lost}."""
    if unknown(item, modes):
        raise ValueError(f"{item['label']}'s own file could not be read: its mode is not known")
    if item.get("kind") == "lab":
        return lab_demote_change(item, modes)
    if item.get("kind") == "bot":
        if not is_live(item["id"], raw, modes):
            return {"already": True, "changes": {}, "kw": {}, "lost": []}
        return {"already": False, "changes": {item["bot"]: "PAPER"}, "kw": {}, "lost": []}
    mode, groups, tactics = tnb_live_now(raw)
    if mode == "LIVE" or not (item["group"] in groups and item["tactic"] in tactics):
        return {"already": True, "changes": {}, "kw": {}, "lost": []}   # a fully LIVE Trend & Breakout: not ours
    rm = tnb_remove(item["tactic"], item["group"], groups, tactics, keep)
    return {"already": False, "changes": {}, "lost": rm["lost"],
            "kw": {"tnb_live_groups": sorted(rm["groups"]) or ["none"],
                   "tnb_live_tactics": sorted(rm["tactics"]) if rm["tactics"] is not None and rm["groups"] else None}}


def lab_promote_change(item: dict, modes: dict, data_dir) -> dict:
    """What putting a lab idea on the demo writes, worked out without writing: its
    roster entry (version, its magic number - the one it already has, else the next
    free one in the reserved range - its risk tier, since, plan)."""
    from ..labdemo import roster as LR
    ids = modes.get(LABDEMO)
    if ids == UNREADABLE:
        raise ValueError("the lab-demo roster could not be read: nothing is decided on a guess")
    lab_id = str(item["id"])[4:]
    if isinstance(ids, frozenset) and lab_id in ids:
        return {"already": True, "changes": {}, "kw": {}, "extras": []}
    if item.get("control") or item.get("family") == CONTROL_FAMILY:
        raise ValueError("a control idea (the luck benchmark) is never promoted")
    tier = float(item.get("tier_pct") or 0.0)
    if tier not in LR.TIERS:
        raise ValueError(f"no demo risk tier ({tier}) for {lab_id}")
    version = int(item.get("version") or 1)
    magic, magics = LR.magic_for(LR.read_magics(data_dir, strict=True), lab_id, version)
    entry = {"version": version, "magic": magic, "tier_pct": tier, "since_utc": item.get("since_utc"),
             "plan": item.get("plan")}
    return {"already": False, "changes": {}, "kw": {}, "extras": [], "magic": magic,
            "lab": {"add": {lab_id: entry}, "magics": magics}}


def lab_demote_change(item: dict, modes: dict) -> dict:
    ids = modes.get(LABDEMO)
    if ids == UNREADABLE:
        raise ValueError("the lab-demo roster could not be read: nothing is decided on a guess")
    lab_id = str(item["id"])[4:]
    if not (isinstance(ids, frozenset) and lab_id in ids):
        return {"already": True, "changes": {}, "kw": {}, "lost": []}
    return {"already": False, "changes": {}, "kw": {}, "lost": [], "lab": {"remove": lab_id}}


def _write_lab(data_dir, lab: dict, now: dt.datetime) -> None:
    """The roster written whole (magics.json first): one idea added or taken out."""
    from ..labdemo import roster as LR
    ideas = LR.read_roster(data_dir, strict=True)
    for k, v in (lab.get("add") or {}).items():
        ideas[k] = v
    if lab.get("remove"):
        ideas.pop(lab["remove"], None)
    LR.write_roster(data_dir, ideas, lab.get("magics"), now)


def apply_change(config_path, change: dict, now: dt.datetime, set_modes: SetModes = _set_modes,
                 must_live: Sequence[str] = (), must_not_live: Sequence[str] = ()) -> None:
    """``set_modes`` with the change, then config.json (and ian.json) read
    back: every id in ``must_live`` LIVE and none in ``must_not_live``. On ANY
    failure - set_modes raising after it wrote one file (PartialWrite, an
    OSError), a read-back that fails, or files that do not say what was asked
    - both files are put back exactly as they were (written whole, renamed
    into place) and it raises. The caller keeps its saved intent either way:
    only a later successful read decides what happened."""
    from .modes import _data_dir
    cfg = Path(config_path)
    data = _data_dir(read_config_strict(cfg), cfg)
    if change.get("lab"):                                  # a lab idea: the lab-demo roster, nothing else
        from ..labdemo import roster as LR
        files = [LR.magics_path(data), LR.roster_path(data)]
    else:
        files = [cfg, data / "ian.json"]
    before = {p: (p.read_bytes() if p.exists() else None) for p in files}
    try:
        if change.get("lab"):
            _write_lab(data, change["lab"], now)
        else:
            set_modes(config_path, change["changes"], now=now, **change["kw"])
        raw, modes = strict_view(config_path)
        wrong = ([i for i in must_live if unknown(i, modes) or not is_live(i, raw, modes)]
                 + [i for i in must_not_live if unknown(i, modes) or is_live(i, raw, modes)])
        if wrong:
            raise ValueError(f"after the switch config.json did not say what was asked for {', '.join(wrong)}; "
                             f"put back as it was")
    except Exception:
        _put_back(before)
        raise


def _put_back(before: dict) -> None:
    for p, body in before.items():
        if body is None:
            p.unlink(missing_ok=True)
        else:
            tmp = p.with_name("." + p.name + ".weekly.tmp")
            tmp.write_bytes(body)
            os.replace(tmp, p)


def promote(item: dict, config_path, now: dt.datetime, proven: set, set_modes: SetModes = _set_modes) -> dict:
    """Switch one candidate LIVE and check it is; nothing written when it
    already is. {changed, extras}. Raises with the files as they were."""
    raw, modes = strict_view(config_path)
    ch = promote_change(item, raw, modes, proven)
    if ch["already"]:
        return {"changed": False, "extras": []}
    apply_change(config_path, ch, now, set_modes, must_live=[item["id"]] + list(ch["extras"]))
    return {"changed": True, "extras": ch["extras"]}


def demote(item: dict, config_path, now: dt.datetime, keep: set, set_modes: SetModes = _set_modes) -> dict:
    """Take one promotion back to PAPER and check it is; nothing written when
    it is not live. {changed, lost (other parts taken back with it)}."""
    raw, modes = strict_view(config_path)
    ch = demote_change(item, raw, modes, keep)
    if ch["already"]:
        return {"changed": False, "lost": []}
    apply_change(config_path, ch, now, set_modes, must_not_live=[item["id"]] + list(ch["lost"]))
    return {"changed": True, "lost": ch["lost"]}


def _restart_all(data_dir, state: dict, restart: Optional[Callable[[str], tuple]], actions: list) -> None:
    """Restart each process whose bot's mode changed, then forget it; kept
    for the next check when it fails or no one can restart it."""
    if restart is None:
        return
    left = []
    for name in list(dict.fromkeys(state["restart_pending"])):
        try:
            ok, msg = restart(name)
        except Exception as exc:
            ok, msg = False, str(exc)
        actions.append(f"weekly: restarting {name} for its new mode - {msg}")
        if not ok:
            left.append(name)
    if left != state["restart_pending"]:
        state["restart_pending"] = left
        _save_state(data_dir, state)


def _save_state(data_dir, state: dict) -> None:
    _write_atomic(wdir(data_dir) / STATE, state)


def _queue_restart(state: dict, process: Optional[str]) -> bool:
    """Queue a process's restart; True when it was not queued already."""
    name = process or "trader"
    if name in state["restart_pending"]:
        return False
    state["restart_pending"] = state["restart_pending"] + [name]
    return True


def _unqueue(state: dict, process: Optional[str]) -> None:
    state["restart_pending"] = [x for x in state["restart_pending"] if x != (process or "trader")]


def _event(event: str, item: dict, now: dt.datetime, why: str, **extra) -> dict:
    clean = {k: v for k, v in extra.items() if v is not None}
    return dict({"ts_utc": _iso(now), "event": event, "id": item["id"], "label": item.get("label"),
                 "part": item.get("part") or "", "week": iso_week(now), "why": why}, **clean)


# ------------------------------------------------------------- the tick --
def tick(data_dir, config_path, now: Optional[dt.datetime] = None, *,
         positions_fn: Optional[Callable[[], Optional[list]]] = None,
         restart: Optional[Callable[[str], tuple]] = None,
         set_modes: SetModes = _set_modes) -> list[str]:
    """One check (the watchdog's, every CHECK_SECONDS): make the day's pick when
    due (every UK trading day at 22:30), release it at its time (06:30 UK the next
    trading day), judge what is on the demo, take back what failed, then restart
    the bots whose mode changed (a lab idea needs no restart: the engine re-reads
    its roster). Returns what it did, in words.

    Each step is idempotent. Before any mode is written its intent is saved
    in state.json (the item marked ``promoting`` or ``demoting``, its restart
    queued), so a crash in between is finished, not forgotten, at the next
    check. A state.json or config.json that cannot be read stops the whole
    pass: nothing is decided on a guess."""
    now = to_utc(now or utcnow())
    data = Path(data_dir)
    try:
        state = load_state(data, strict=True)
        strict_view(config_path)
    except Exception as exc:
        log.warning("weekly: %s", exc)
        return [f"weekly: {exc}; nothing was done this pass"]
    actions: list[str] = []
    hist = history(data)
    _restart_all(data, state, restart, actions)          # a restart left over from before (a crash in between)
    halt = halted(data)
    _settle_promoting(data, config_path, state, now, actions)

    # 0. an earlier pick, never released (HALT): it lapses at the next pick
    friday = last_pick_friday(now)
    old = _read(wdir(data) / LATEST)
    if isinstance(old, dict) and old.get("status") == PLANNED and old.get("friday") != friday.isoformat() \
            and now >= (_when(old.get("expires_utc")) or now):
        old["status"] = EXPIRED
        _write_atomic(plan_path(data, dt.date.fromisoformat(old["friday"])), old)
        _write_atomic(wdir(data) / LATEST, old)
        actions.append(f"weekly: the pick of {old['friday']} was never released and has lapsed")

    # 1. the day's pick (catching up until its release)
    ppath = plan_path(data, friday)
    plan = _read(ppath)
    if not isinstance(plan, dict) and now < release_utc(friday):
        try:
            plan = make_plan(data, config_path, now, friday)
            _write_atomic(ppath, plan)
            _write_atomic(wdir(data) / LATEST, plan)
            names = [f"{x['pick']['label']} {x['pick'].get('part') or ''}".strip() for x in plan_picks(plan)]
            actions.append(f"weekly: the pick of {friday.isoformat()} is "
                           + (", ".join(names) if names else "nothing (nothing proved itself)"))
        except Exception as exc:
            log.exception("weekly pick")
            actions.append(f"weekly: the pick could not be made ({exc}); trying again at the next check")
            plan = None

    # 2. the vetoes: this week's plan, and every live promotion by the plan it came from
    actions += _vetoes(data, state, plan, ppath, now)
    if isinstance(plan, dict):
        status = plan.get("status")
        if status == PLANNED and now >= _when(plan["expires_utc"]):
            plan["status"] = EXPIRED
            _save_plan(data, ppath, plan)
        elif status == PLANNED and now >= _when(plan["release_utc"]):
            if halt:
                if plan.get("waiting") != "HALT":
                    plan["waiting"] = "HALT"
                    _save_plan(data, ppath, plan)
                    actions.append("weekly: data/HALT is there - this week's pick waits, nothing is applied")
            else:
                actions += _release(data, config_path, plan, ppath, state, hist, now, set_modes)

    # 3. the live judgement (demotions only remove risk: applied at once, but never under HALT)
    if state["promoted"]:
        try:
            actions += _judge_live(data, config_path, state, now, positions_fn, set_modes, halt)
        except Exception as exc:                          # config.json unreadable mid-pass: nothing decided
            log.warning("weekly judgement: %s", exc)
            actions.append(f"weekly: the live judgement could not be made ({exc}); nothing was changed")

    _restart_all(data, state, restart, actions)
    return actions


def _vetoes(data: Path, state: dict, plan: Optional[dict], ppath: Path,
            now: Optional[dt.datetime] = None) -> list[str]:
    """Aaron's veto (a VETO-<Friday> file): this week's pick not yet released
    is cancelled; any live promotion whose own plan was vetoed - the pick and
    every part that went live with it - is marked to go back to PAPER. A
    vetoed pick that never went live gets a 'veto' line in the history, which
    restarts its evidence window (``last_demoted``)."""
    now = to_utc(now or utcnow())
    out = []
    if isinstance(plan, dict) and plan.get("status") in (PLANNED, APPLIED) \
            and veto_path(data, dt.date.fromisoformat(plan["friday"])).exists():
        plan["status"] = VETOED
        _save_plan(data, ppath, plan)
        _record_veto(data, state, plan, now)
        out.append("weekly: this week's pick was vetoed")
    marked = False
    for it in state["promoted"]:
        try:
            vetoed = bool(it.get("plan")) and veto_path(data, dt.date.fromisoformat(it["plan"])).exists()
        except ValueError:
            vetoed = False
        if vetoed and not it.get("demote_why"):
            it["demote_why"] = "Aaron vetoed this pick"
            marked = True
            p = _read(plan_path(data, dt.date.fromisoformat(it["plan"])))
            if isinstance(p, dict) and p.get("status") == APPLIED:
                p["status"] = VETOED
                _save_plan(data, plan_path(data, dt.date.fromisoformat(it["plan"])), p)
                _record_veto(data, state, p, now)
    if marked:
        _save_state(data, state)
        out.append("weekly: a vetoed pick goes back to PAPER")
    return out


def _record_veto(data: Path, state: dict, plan: dict, now: dt.datetime) -> None:
    """Once, as a plan becomes VETOED: a 'veto' line for each of its picks, and each part that would have ridden
    with one, that is not on the demo now (one on the demo is taken back, and its 'demote' line does the same).
    Review, 14 Oct: with the pick made every day a veto recorded nowhere lasted only until the next day's pick,
    which took the same test again on the same evidence."""
    on_demo = {str(p.get("id")) for p in state["promoted"]}
    by_id = {c.get("id"): c for c in plan.get("ranking") or () if isinstance(c, dict)}
    seen: set = set()
    for x in plan_picks(plan):
        for item in [x["pick"]] + [by_id.get(e) or {"id": e} for e in x.get("extras") or ()]:
            cid = str(item.get("id") or "")
            if not cid or cid in on_demo or cid in seen:
                continue
            seen.add(cid)
            if not item.get("label") and cid.startswith("tnb:") and cid.count(":") == 2:
                item = dict(item, label=TNB_LABEL, part=part_words(*cid.split(":")[1:]))
            _append_history(data, _event("veto", item, now, "Aaron vetoed this pick", plan=plan.get("friday")))


def _save_plan(data: Path, ppath: Path, plan: dict) -> None:
    _write_atomic(ppath, plan)
    latest = _read(data / DIR / LATEST)
    if not isinstance(latest, dict) or latest.get("friday") == plan.get("friday"):
        _write_atomic(data / DIR / LATEST, plan)


def _finish_promotion(data: Path, state: dict, items: list[dict], now: dt.datetime) -> None:
    """Record a promotion that config.json now holds: its history lines, its
    plan applied, the intent flag cleared."""
    pick = items[0]
    for it in items:
        it.pop("promoting", None)
        it.pop("restart_added", None)
        why = (f"the pick of {it.get('plan')}: {it.get('why') or 'passes the bar'}" if it is pick else
               f"proved itself too, and goes live with {pick['label']} {pick.get('part')}")
        _append_history(data, _event("promote", it, now, why, plan=it.get("plan"), pick=it is pick,
                                     tier_pct=it.get("tier_pct"), magic=it.get("magic") if it.get("kind") == "lab"
                                     else None, version=it.get("version")))
    _save_state(data, state)
    if pick.get("plan"):
        _applied_when_done(data, state, pick["plan"], now)


def _applied_when_done(data: Path, state: dict, friday: str, now: dt.datetime) -> None:
    """A plan is APPLIED once every pick it releases (``releasing``, else all of
    them) is on real money and recorded; until then it stays PLANNED, so a pick
    that failed is tried again at the next check."""
    pp = plan_path(data, dt.date.fromisoformat(friday))
    p = _read(pp)
    if not isinstance(p, dict) or p.get("status") != PLANNED:
        return
    want = p.get("releasing") or [x["pick"]["id"] for x in plan_picks(p)]
    done = {it["id"] for it in state["promoted"] if it.get("plan") == friday and not it.get("promoting")}
    if all(i in done for i in want):
        p["status"], p["applied_utc"] = APPLIED, _iso(now)
        p.pop("last_error", None)
        _save_plan(data, pp, p)


def _settle_promoting(data: Path, config_path, state: dict, now: dt.datetime, actions: list) -> None:
    """A promotion whose intent was saved but not finished (a crash between
    writing it and recording it): recorded when config.json holds it; else
    dropped, and its plan is tried again at its release."""
    stuck = [it for it in state["promoted"] if it.get("promoting")]
    if not stuck:
        return
    raw, modes = strict_view(config_path)
    for root in [it for it in stuck if not it.get("rides_with")] or stuck[:1]:
        group = [root] + [it for it in stuck if it.get("rides_with") == root["id"] and it.get("plan") == root.get("plan")]
        if any(unknown(it, modes) for it in group):
            continue                                      # its own file is broken: decided once it can be read
        if all(is_live(it["id"], raw, modes) for it in group):
            _finish_promotion(data, state, group, now)
            actions.append(f"weekly: {root['label']} {root.get('part') or ''} is LIVE (recorded after a restart)")
        else:
            for it in group:
                state["promoted"].remove(it)
            if root.get("restart_added"):
                _unqueue(state, root.get("process"))      # nothing changed: no restart needed
            _save_state(data, state)
            actions.append(f"weekly: {root['label']}'s switch to LIVE never happened; tried again at its release")


def _release(data: Path, config_path, plan: dict, ppath: Path, state: dict, hist: list, now: dt.datetime,
             set_modes: SetModes) -> list[str]:
    """Release this week's picks, best first, each on its own (its intent saved,
    the switch, the read back, the record): a pick that fails leaves the others
    alone and is tried again at the next check. At most ``max_promotions`` in
    the ISO week, counting other plans' picks already made in it."""
    entries = plan_picks(plan)
    if not entries:
        return []
    target = demo_target(data)
    # 14 Oct: the demo's free places (demo_target less the tests on it now), counting this plan's own picks
    # already released as places of their own (they are among ``entries``)
    others = [p for p in roots(state["promoted"]) if p.get("plan") != plan["friday"]]
    room = target - len(others)
    if room <= 0:
        plan["status"] = SKIPPED
        plan["skipped_why"] = f"the demo squad is full ({len(others)} of {target} places)"
        _save_plan(data, ppath, plan)
        return [f"weekly: {entries[0]['pick']['label']} not released - {plan['skipped_why']}"]
    entries = entries[:room]
    want = [x["pick"]["id"] for x in entries]
    if plan.get("releasing") != want:
        plan["releasing"] = want
        _save_plan(data, ppath, plan)
    # every pick of the week and every part riding with one is proven: the cross product was checked
    # against all of them together when the plan was made
    proven = set(want) | {e for x in entries for e in x.get("extras") or ()}
    out: list[str] = []
    for x in entries:
        out += _release_one(data, config_path, plan, ppath, state, x["pick"], proven, now, set_modes, hist)
    _applied_when_done(data, state, plan["friday"], now)
    return out


def _release_one(data: Path, config_path, plan: dict, ppath: Path, state: dict, pick: dict, proven: set,
                 now: dt.datetime, set_modes: SetModes, hist: Optional[list] = None) -> list[str]:
    week = iso_week(now)

    def failed(exc) -> list[str]:
        plan["last_error"] = str(exc)
        _save_plan(data, ppath, plan)
        log.warning("weekly release: %s", exc)
        return [f"weekly: {pick['label']} could not be switched LIVE ({exc}); nothing was changed, trying again"]
    if any(p["id"] == pick["id"] for p in state["promoted"]):
        return []                                          # already on real money (this plan, or before)
    # released once by this plan is released for good: a pick taken back (judged, its early exit, vetoed
    # or switched back by hand) is never switched LIVE again while the week's other picks are retried
    # (review, 13 Oct: lose, demote, re-promote every five minutes). The history is read afresh.
    if any(e.get("event") == "promote" and e.get("id") == pick["id"] and e.get("plan") == plan["friday"]
           for e in history(data)):
        return []
    if pick.get("kind") == "lab":
        # its demo risk tier, the next in turn across the lab's promotions (0.5%, 1%, 2%, ...): kept with the
        # promotion, in the roster and in its history line, so the tiers can be compared
        from ..labdemo.roster import next_tier
        n_lab = sum(1 for e in history(data) if e.get("event") == "promote" and str(e.get("id", "")).startswith("lab:"))
        pick = dict(pick, tier_pct=next_tier(n_lab), since_utc=_iso(now), plan=plan["friday"])
    try:
        raw, modes = strict_view(config_path)
        ch = promote_change(pick, raw, modes, proven)
    except Exception as exc:
        return failed(exc)
    items = [dict(pick, live=True, since_utc=_iso(now), week=week, plan=plan["friday"])]
    if ch.get("magic"):
        items[0]["magic"] = ch["magic"]
    by_id = {c["id"]: c for c in plan.get("ranking") or ()}
    for x in ch["extras"]:
        c = by_id.get(x)
        _k, tactic, group = x.split(":")
        items.append(dict(c or {"id": x, "kind": "tnb", "label": TNB_LABEL, "part": part_words(tactic, group),
                                "risk_gbp": pick.get("risk_gbp")},
                          tactic=tactic, group=group, magic=TNB_MAGIC, process="trader", live=True,
                          since_utc=_iso(now), week=week, plan=plan["friday"], rides_with=pick["id"]))
    known = {p["id"] for p in state["promoted"]}
    items = [it for it in items if it["id"] not in known]
    if not items:
        return []
    for it in items:
        it["promoting"] = True
    state["promoted"] += items
    if not ch["already"] and pick.get("kind") != "lab":        # a lab idea: the engine re-reads its roster
        items[0]["restart_added"] = _queue_restart(state, pick.get("process"))
    _save_state(data, state)                              # the intent first ...
    if not ch["already"]:
        try:                                              # ... then the switch, read back
            apply_change(config_path, ch, now, set_modes, must_live=[it["id"] for it in items])
        except Exception as exc:
            # the intent stays: a successful read (now, or at the next check) decides what happened
            try:
                _settle_promoting(data, config_path, state, now, [])
            except Exception:
                pass
            return failed(exc)
    _finish_promotion(data, state, items, now)            # ... then the record
    tier = f", risk tier {pick['tier_pct']:g}%" if pick.get("kind") == "lab" else ""
    return [f"weekly: {pick['label']} {pick.get('part') or ''} switched LIVE on the DEMO (the pick of "
            f"{plan['friday']}{tier})".replace("  ", " ")]


def _judge_live(data: Path, config_path, state: dict, now: dt.datetime, positions_fn, set_modes: SetModes,
                halt: bool) -> list[str]:
    actions = []
    positions = None
    try:
        positions = positions_fn() if positions_fn is not None else None
    except Exception:
        positions = None
    raw, modes = strict_view(config_path)                 # raises: nothing is marked gone on a failed read
    for it in list(state["promoted"]):
        if it not in state["promoted"] or it.get("promoting"):
            continue                                      # taken back with another part this pass
        if unknown(it, modes):
            actions.append(f"weekly: {it['label']}'s own file could not be read; nothing decided for it")
            continue
        if not is_live(it["id"], raw, modes):
            state["promoted"].remove(it)
            why = it.get("demoting") or it.get("demote_why")
            if why:                                       # our own demotion, written before a crash
                _append_history(data, _event("demote", it, now, why, record=it.get("record")))
                actions.append(f"weekly: {it['label']} {it.get('part') or ''} back on PAPER - {why} (recorded "
                               f"after a restart)".replace("  ", " "))
            else:                                         # switched back by hand: no longer ours to judge
                _append_history(data, _event("gone", it, now, "no longer LIVE in config.json (changed by hand)"))
            _save_state(data, state)
            continue
        trades, source = live_trades(it, positions, data)
        risk = _median_risk(t.get("risk") for t in trades) if it.get("kind") == "lab" and trades else \
            float(it.get("risk_gbp") or FALLBACK_RISK_GBP)
        verdict, why = live_verdict(trades, risk, it["id"])
        if it.get("demoting") or it.get("demote_why"):     # an intent is never dropped by an error
            verdict, why = "demote", it.get("demoting") or it["demote_why"]
        it["record"] = {"trades": len(trades), "net": round(sum(t["net"] for t in trades), 2), "source": source,
                        "checked_utc": _iso(now), "verdict": verdict, "why": why}
        if it.get("kind") == "lab":                       # beside the demo money, never in it
            it["record"]["equiv_1500"] = round(sum(float(t.get("equiv_1500") or 0.0) for t in trades), 2)
        if verdict == "keep" and not it.get("kept_utc"):
            it["kept_utc"] = _iso(now)
            _append_history(data, _event("keep", it, now, why))
        if verdict != "demote" or halt:
            continue                                      # nothing is applied under HALT
        keep = {p["id"] for p in state["promoted"] if p["id"] != it["id"] and p.get("kind") == "tnb"}
        try:
            ch = demote_change(it, raw, modes, keep)
        except Exception as exc:
            actions.append(f"weekly: {it['label']} could not be put back on PAPER ({exc}); nothing was changed")
            continue
        if ch["already"]:
            continue                                      # a fully LIVE Trend & Breakout: not ours to switch
        gone = [it] + [p for p in state["promoted"] if p["id"] in set(ch["lost"])]
        for g in gone:
            g["demoting"] = g.get("demoting") or (why if g is it else
                                                  f"taken back with {it['label']} {it.get('part')}: {why}")
        added = _queue_restart(state, it.get("process")) if it.get("kind") != "lab" else False
        _save_state(data, state)                          # the intent first ...
        try:
            apply_change(config_path, ch, now, set_modes, must_not_live=[g["id"] for g in gone])
        except Exception as exc:
            # the demoting flags stay (tried again at every check until a read shows them on PAPER); the
            # restart is dropped only when a successful read shows nothing changed
            try:
                r2, m2 = strict_view(config_path)
                if added and all(is_live(g["id"], r2, m2) for g in gone):
                    _unqueue(state, it.get("process"))
            except Exception:
                pass
            _save_state(data, state)
            log.warning("weekly demotion: %s", exc)
            actions.append(f"weekly: {it['label']} could not be put back on PAPER ({exc}); nothing was changed, "
                           f"trying again")
            continue
        for g in gone:                                    # ... then the record
            state["promoted"].remove(g)
            _append_history(data, _event("demote", g, now, g["demoting"],
                                         record=it.get("record") if g is it else None))
        _save_state(data, state)
        actions.append(f"weekly: {it['label']} {it.get('part') or ''} back on PAPER - {why}".replace("  ", " "))
        raw, modes = strict_view(config_path)
    state["updated_utc"] = _iso(now)
    _save_state(data, state)
    return actions


# ------------------------------------------------------------ the veto --
def veto(data_dir, now: Optional[dt.datetime] = None) -> str:
    """Cancel this week's pick: the latest plan's, or - when that has none to
    cancel - the newest pick still on real money. The watchdog marks it at its
    next check: not released, or taken back to PAPER with every part that went
    live with it."""
    now = to_utc(now or utcnow())
    latest = _read(wdir(data_dir) / LATEST)
    target = None
    if isinstance(latest, dict) and latest.get("pick") and latest.get("status") in (PLANNED, APPLIED):
        target = (latest["friday"], latest["pick"], latest["status"] == APPLIED)
    else:
        try:
            live = [p for p in load_state(data_dir)["promoted"] if p.get("plan") and not p.get("rides_with")]
        except Exception:
            live = []
        if live:
            p = max(live, key=lambda x: str(x.get("since_utc") or ""))
            target = (p["plan"], p, True)
    if target is None:
        return "There is no pick to cancel."
    friday, pick, applied = target
    path = veto_path(data_dir, dt.date.fromisoformat(friday))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_iso(now) + "\n")
    what = f"{pick['label']} {pick.get('part') or ''}".strip()
    return (f"Cancelled: {what} will be taken back to practice at the next check." if applied
            else f"Cancelled: {what} will not go live.")


# ---------------------------------------------------------------- Home --
_BOARD_CACHE: dict = {}


def home_board(data_dir, config_path=None, now: Optional[dt.datetime] = None, deals=None,
               open_tickets=None, open_live=None, cache_seconds: float = 60.0) -> dict:
    """Home's headline numbers, everything as ONE pool of tests (Aaron, 13 Oct:
    "they just become part of all of the other tests"):

    * ``live``: one entry per test on real money through the weekly pick -
      its short name, real GBP today and since it went live (MetaTrader's
      closed positions, ``deals``), open now (``open_live``, the broker's
      floating figures), "N of 20" live trades and a chip;
    * ``counts``: tests running, making money, losing, too early (1 to 9
      trades), waiting for their first trade, ready for real money (passes the
      bar);
    * ``all``: every test with a trade in the last LOOKBACK_DAYS days, best
      practice GBP after costs first - the bots, Trend & Breakout's parts and
      EVERY lab idea ranked together (13 Oct: about 640 of them);
    * ``top`` and ``bottom``: Home's share of it, the best HOME_TOP and the
      worst HOME_BOTTOM (all of them when there are no more than that);
    * ``waiting``: the tests with no trade yet in the window (not listed on
      Home: counted, and one folded line on the Practice page);
    * ``pick``: this week's pick.

    The pool (``candidates``, every lab idea included) is worked out at most
    once every ``cache_seconds``; the real money is worked out on every call,
    from the page's own refresh. Never raises."""
    now = to_utc(now or utcnow())
    out = {"live": [], "counts": {"running": 0, "making": 0, "losing": 0, "early": 0, "waiting": 0, "ready": 0},
           "top": [], "bottom": [], "all": [], "waiting": [], "tests": 0,
           "total": {"net": 0.0, "trades": 0, "win_rate": None}, "pick": pick_tile(None, now)}
    if not data_dir:
        return out
    config_path = config_path or Path(data_dir) / "config.json"
    try:
        pool = _pool(data_dir, config_path, now, cache_seconds)
        out["counts"] = pool_counts(pool)
        out.update(breakdown(pool))
        out["total"] = pool_total(pool)
    except Exception as exc:
        log.warning("home board: %s", exc)
    try:
        out["live"] = live_tiles(data_dir, now, deals, open_tickets, open_live)
    except Exception as exc:
        log.warning("home board live: %s", exc)
    latest = _read(wdir(data_dir) / LATEST)
    out["pick"] = pick_tile(latest if isinstance(latest, dict) else None, now)
    return out


def breakdown(pool: Sequence[dict]) -> dict:
    """The pool as rows, winners at the top: ``all`` (every test with a trade,
    best practice GBP after costs first, the id breaking ties), Home's ``top``
    and ``bottom`` (no row twice: with HOME_TOP + HOME_BOTTOM or fewer, all of
    them are the top and the bottom is empty), ``waiting`` (no trade yet, by
    name) and ``tests`` (how many in all)."""
    rows = [_small(c) for c in pool]
    traded = sorted((x for x in rows if x["trades"]), key=lambda x: (-float(x["net"] or 0.0), x["id"]))
    waiting = sorted((x for x in rows if not x["trades"]), key=lambda x: (x["name"], x["id"]))
    if len(traded) <= HOME_TOP + HOME_BOTTOM:
        top, bottom = traded, []
    else:
        top, bottom = traded[:HOME_TOP], traded[-HOME_BOTTOM:]
    return {"all": traded, "top": top, "bottom": bottom, "waiting": waiting, "tests": len(rows)}


def _pool(data_dir, config_path, now: dt.datetime, cache_seconds: float) -> list[dict]:
    key = str(data_dir)
    hit = _BOARD_CACHE.get(key)
    if hit and 0 <= (now - hit[0]).total_seconds() < cache_seconds:
        return hit[1]
    pool = candidates(data_dir, config_path, now, keep_empty=True)       # every lab idea, a trade or not
    if len(_BOARD_CACHE) > 16:
        _BOARD_CACHE.clear()
    _BOARD_CACHE[key] = (now, pool)
    return pool


def chip_of(trades: int, net: Optional[float]) -> str:
    """By the money alone (Aaron, 13 Oct: "0 making money" next to +£29 made no
    sense): MAKING MONEY above zero, LOSING below, TOO EARLY with no trade or
    exactly level. How few trades there are is said beside it (``early``)."""
    if not trades or net is None:
        return TOO_EARLY
    return MAKING if net > 0 else LOSING if net < 0 else TOO_EARLY


def pool_counts(pool: Sequence[dict]) -> dict:
    """running: every test; making / losing by the money; early: 1 to 9 trades;
    waiting: no trade yet in the window; ready: passes the bar."""
    chips = [chip_of(int(c["stats"]["trades"]), c["stats"]["net"]) for c in pool]
    n = [int(c["stats"]["trades"] or 0) for c in pool]
    return {"running": len(pool), "making": chips.count(MAKING), "losing": chips.count(LOSING),
            "early": sum(1 for k in n if 0 < k < TOO_EARLY_TRADES), "waiting": sum(1 for k in n if not k),
            "ready": sum(1 for c in pool if c.get("passes"))}


def pool_total(pool: Sequence[dict]) -> dict:
    """Every test together: practice £ after costs, trades and wins over the window."""
    net = sum(float(c["stats"]["net"] or 0.0) for c in pool)
    trades = sum(int(c["stats"]["trades"] or 0) for c in pool)
    wins = sum(int(c["stats"]["wins"]) if c["stats"].get("wins") is not None
               else int(round(float(c["stats"].get("win_rate") or 0.0) * int(c["stats"]["trades"] or 0) / 100.0))
               for c in pool)
    return {"net": round(net, 2), "trades": trades, "win_rate": (100.0 * wins / trades) if trades else None}


def short_name(c: dict) -> str:
    """A test's short name: the part for Trend & Breakout's ('T&B momentum
    continuation, minor pairs'), the idea's for the lab's, the bot's otherwise."""
    if c.get("kind") == "tnb":
        return f"T&B {c.get('part') or ''}".strip()
    if c.get("kind") == "lab":
        return str(c.get("part") or "lab idea")
    return str(c.get("label") or c.get("id"))


def _small(c: dict) -> dict:
    """One row of the breakdown: light on purpose (~640 of them). ``kind`` and
    ``ref`` say where its own detail page is (a bot's verdicts id, Trend &
    Breakout's, or the lab idea's id)."""
    s = c["stats"]
    kind = str(c.get("kind") or "")
    if kind == "bot":
        ref = BOTS.get(str(c.get("bot") or ""), ("",))[0]
    elif kind == "tnb":
        ref = "market_intelligence"               # Trend & Breakout's own page (verdicts.TNB)
    else:
        ref = str(c["id"])[4:] if str(c["id"]).startswith("lab:") else str(c["id"])
    return {"id": c["id"], "name": short_name(c), "net": s["net"], "trades": s["trades"], "win_rate": s["win_rate"],
            "chip": chip_of(int(s["trades"]), s["net"]), "ready": bool(c.get("passes")), "live": bool(c.get("live")),
            "early": int(s["trades"] or 0) < TOO_EARLY_TRADES, "kind": kind, "ref": ref}


def _int(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def open_approaches(data_dir, open_live) -> dict:
    """{ticket: approach} of Trend & Breakout's open positions, from its journal
    (the lookup ``live_trades`` uses for closed ones)."""
    from ..engine.tactics import approach_of
    tickets = sorted({_int(r.get("ticket")) for r in open_live or () if isinstance(r, dict)} - {0})
    if not tickets:
        return {}
    q = ",".join("?" for _ in tickets)
    return {_int(r.get("ticket")): approach_of(str(r.get("tactic") or "")) for r in _rows(
        Path(data_dir) / "journal.sqlite", f"SELECT ticket, tactic FROM trades WHERE ticket IN ({q})", tuple(tickets))}


def live_tiles(data_dir, now: dt.datetime, deals=None, open_tickets=None, open_live=None) -> list[dict]:
    """One entry per test on real money through the weekly pick (data/weekly/state.json)."""
    state = load_state(data_dir)
    if not state["promoted"]:
        return []
    from ..coach.records import kind_of
    from .standing import uk_date
    positions = live_positions_json(deals, open_tickets)["positions"] if deals is not None else None
    today = uk_date(now)
    approach = open_approaches(data_dir, open_live) if any(p.get("kind") == "tnb" for p in state["promoted"]) else {}
    out = []
    for it in state["promoted"]:
        trades, _src = live_trades(it, positions, data_dir) if positions is not None else ([], "")
        net = round(sum(t["net"] for t in trades), 2)
        opened = [r for r in open_live or () if isinstance(r, dict) and int(r.get("magic") or -1) == int(it["magic"])]
        if it.get("kind") == "tnb":                       # its kind of market AND its approach (the journal's)
            opened = [r for r in opened if kind_of(str(r.get("symbol") or "")) == it.get("group")
                      and approach.get(_int(r.get("ticket"))) == it.get("tactic")]
        out.append({"id": it["id"], "name": short_name(it),
                    "today": round(sum(t["net"] for t in trades if uk_date(t["closed"]) == today), 2)
                    if positions is not None else None,
                    "since": net if positions is not None else None, "since_utc": it.get("since_utc"),
                    "open": round(sum(float(r.get("profit") or 0.0) for r in opened), 2) if opened else None,
                    "trades": len(trades), "of": JUDGE_AFTER,
                    "chip": MAKING if net > 0 else LOSING if net < 0 else TOO_EARLY})
    return out


def pick_tile(latest: Optional[dict], now: dt.datetime) -> dict:
    """This week's pick in a few words: {name, part, when}."""
    nxt = next_pick_utc(now)
    when_next = f"next pick {uk(nxt).strftime('%a')} {uk(nxt).day} {uk(nxt).strftime('%b %H:%M')}"
    if latest and latest.get("pick") and latest.get("status") in (PLANNED, APPLIED):
        p = latest["pick"]
        names = [short_name(x["pick"]) for x in plan_picks(latest)] or [short_name(p)]
        rel = _when(latest.get("release_utc"))
        if latest["status"] == APPLIED:
            when = "LIVE since " + uk(_when(latest.get("applied_utc")) or now).strftime("%a %d %b")
        else:
            when = ("waiting: HALT" if latest.get("waiting") == "HALT" else
                    "LIVE from " + (uk(rel).strftime("%a %d %b %H:%M") if rel else "Monday"))
        return {"name": short_name(p), "names": names, "when": when, "none": False}
    return {"name": NOTHING_PROVED, "names": [NOTHING_PROVED], "when": when_next, "none": True}


def weekly_report(data_dir) -> Optional[dict]:
    """The pulse's reports/live/weekly.json: the latest plan and what is live
    now, whitelisted. None before the weekly pick has ever run."""
    d = wdir(data_dir)
    if not d.exists():
        return None
    latest = _read(d / LATEST)
    state = load_state(data_dir)
    keep = ("friday", "made_utc", "release_utc", "status", "pick", "picks", "lab_pick", "reasons", "week", "waiting",
            "skipped_why", "last_error")
    rep = {"note": "the squad's pick: practice candidates ranked every UK trading day, the best qualifiers to "
                   "the DEMO account's free places"}
    if isinstance(latest, dict):
        rep["plan"] = {k: latest[k] for k in keep if k in latest}
        rep["plan"]["ranking"] = (latest.get("ranking") or [])[:10]
    rep["live"] = [{k: p.get(k) for k in ("id", "label", "part", "since_utc", "record", "risk_gbp")}
                   for p in state["promoted"]]
    rep["history"] = history(data_dir)[-20:]
    return rep


# ------------------------------------------------------------------ CLI --
def _data_of(config_path) -> Path:
    from .modes import _data_dir
    return _data_dir(read_config(config_path), Path(config_path))


def _line(c: dict) -> str:
    shrunk = "-" if c.get("shrunk_r") is None else f"{c['shrunk_r']:+.2f}R"
    score = "-" if c.get("score") is None else f"{c['score']:+.2f}R/wk"
    return (f"  {('READY' if c.get('passes') else ''):5} {c['label'][:22]:22} {(c.get('part') or '')[:40]:40} "
            f"{c.get('trades') or 0:>4} trades  £{(c.get('net') or 0):+8.2f}  shrunk {shrunk:>7}  {score}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="the weekly pick: the best proven candidate to real money")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("what", nargs="?", default="show", choices=("show", "rank", "veto"))
    args = ap.parse_args(argv)
    data = _data_of(args.config)
    if args.what == "veto":
        print(veto(data))
        return 0
    if args.what == "rank":
        plan = make_plan(data, args.config, utcnow())
        print("RANKING (worked out now; nothing is applied)")
        for c in plan["ranking"]:
            print(_line(c))
        print("Pick: " + (f"{plan['pick']['label']} {plan['pick'].get('part') or ''}" if plan["pick"] else
                          NOTHING_PROVED))
        for r in plan["reasons"]:
            print("  - " + r)
        return 0
    latest = _read(wdir(data) / LATEST)
    if not isinstance(latest, dict):
        print(f"No weekly pick yet; the first is {uk(next_pick_utc(utcnow())).strftime('%a %d %b %H:%M')} UK.")
    else:
        p = latest.get("pick")
        print(f"Pick of {latest['friday']}: " + (f"{p['label']} {p.get('part') or ''}".strip() if p else NOTHING_PROVED)
              + f" [{latest.get('status')}] release {latest.get('release_utc')}")
        for r in latest.get("reasons") or ():
            print("  - " + r)
    for it in load_state(data)["promoted"]:
        lv = it.get("record") or {}
        print(f"LIVE: {it['label']} {it.get('part') or ''} since {it.get('since_utc')}: "
              f"{lv.get('why') or 'not judged yet'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
