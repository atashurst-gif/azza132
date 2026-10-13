"""The weekly pick: everything practises, and the best proven one goes to real money each week.

    python -m mintel.ops.weekly --config data/config.json            # what is picked, what is live
    python -m mintel.ops.weekly --config data/config.json rank       # this week's ranking, worked out now
    python -m mintel.ops.weekly --config data/config.json veto       # cancel this week's pick

Aaron, 13 Oct 2026: "put everything out of live and into the lab, once they
begin proving move the best each week into real money and live". Every bot
went to PAPER that day (deploy/mac/mintel_mac.sh
``apply_all_practice_2026_10_13``); this is the only way back to real money
(on IC Markets' DEMO account, the "real money" of this codebase).

THE CANDIDATES - everything that is practising:

* the Momentum Runner, Band Breaker, Crowd Fader and Financial Ian, each from
  its own PAPER record (``verdicts.practice_trades``);
* Trend & Breakout's parts: each approach (tactic) x kind of market (FX_MINOR,
  INDEX, GOLD ...), its paper record (data/tnb_paper.sqlite, the money) joined
  by ticket to its journal (data/journal.sqlite, the approach);
* the lab's ideas at READY FOR LIVE (data/lab/lab.sqlite, read-only). The lab
  has no live order path yet: when one of its ideas is the best, it is
  recorded as "picked, needs a live path (being built)" and the best candidate
  that has a live path is the one promoted.

THE PROOF BAR ("begin proving"), on practice trades of the last LOOKBACK_DAYS
(28) days only, each after its own costs (and, for a candidate taken back to
PAPER before, only those since it was taken back):

1. ``verdicts.decide`` says PROVEN: at least 30 trades over at least 5 UK
   days, positive after costs, a profit factor of at least 1.2 and more up
   days than down;
2. the luck check: 2,000 bootstrap resamples of its per-trade nets (a fixed
   seed from its id, ``lab.stats.seed_of``); the share of resampled averages
   at or below zero - the chance its true average is not positive - must be
   at most LUCK_MAX (10%).

THE RANKING: score = LCB x trades a week, where LCB is the 10th percentile of
those resampled averages (a cautious average GBP a trade after costs) and
trades a week = trades x 7 / the days since its first trade in the window (at
least 7). Roughly the GBP a week it should make, cautiously estimated.

THE CADENCE (UK time, Europe/London, BST and GMT alike):

* Friday at or after 22:30: the pick is worked out and written to
  data/weekly/plan-<Friday>.json and data/weekly/latest.json - the ranking,
  the pick, the reasons and the release time. A watchdog that was down then
  catches up until the release.
* 06:30 on the next UK trading day (normally Monday): the pick is released -
  switched LIVE through ``modes.set_modes`` and the bot that runs it
  restarted. At most ONE new promotion in an ISO week.
* ``veto`` (Aaron) cancels the week's pick; a pick already live is taken back.
* data/HALT: nothing is applied while it exists (a pick waits, and lapses at
  the next Friday's pick).

THE LIVE JUDGEMENT, on MetaTrader's own figures after commission (the
trader's page, ``/weekly-live``: positions by magic number; a Trend & Breakout
part by its approach, from the journal, and its kind of market):

* after JUDGE_AFTER (20) live trades: net <= 0 -> back to PAPER; else it stays;
* at any point, a live loss of EARLY_EXIT_RISKS (10) x its risk a trade ->
  back to PAPER at the next check.

A demotion only removes risk, so it is applied at the next check, never
waiting for Monday. When the page cannot be read, the bots' own records are
read instead - only ever to take something back, never to keep it.

Every promotion and demotion is one line of data/weekly/history.jsonl.
Everything here is idempotent: the change is recorded only after
``set_modes`` has written it, and a change already in config.json (a
restart between the two) is recorded without writing it again; a failure
leaves every file as it was and is logged. Nothing here doubles after a
loss or adds exposure because of one; every trade keeps its own broker stop
(the bots' own code, untouched). config.json is read with ``json`` alone;
the secrets file is never opened here (``set_modes`` checks a bot's magic
number on its way to LIVE through ``Config.load``, as the modes tool always
has).
"""
from __future__ import annotations

import argparse
import datetime as dt
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
LUCK_MAX = 0.10                 # the chance its true average is <= 0 must be at most this
LCB_QUANTILE = 0.10             # the cautious average a trade: this quantile of the resampled averages
RESAMPLES = 2000
JUDGE_AFTER = 20                # live trades before a promotion is judged
EARLY_EXIT_RISKS = 10.0         # a live loss of this many times its risk a trade: back to PAPER
TOO_EARLY_TRADES = 10           # Home: fewer trades than this is TOO EARLY
PICK_WEEKDAY, PICK_AT = 4, (22, 30)     # Friday 22:30 UK
RELEASE_AT = (6, 30)                    # 06:30 UK on the next UK trading day
CHECK_SECONDS = 300.0           # how often the watchdog looks
FALLBACK_RISK_GBP = 10.0        # risk a trade when no record says (coach.BOT_FALLBACK_RISK_GBP)
SCHEMA = 1

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


def last_pick_friday(now: dt.datetime) -> dt.date:
    """The latest Friday whose pick time (22:30 UK) is at or before ``now``."""
    d = uk(now).date()
    d -= dt.timedelta(days=(d.weekday() - PICK_WEEKDAY) % 7)
    if uk_at(d, PICK_AT) > to_utc(now):
        d -= dt.timedelta(days=7)
    return d


def next_pick_utc(now: dt.datetime) -> dt.datetime:
    return uk_at(last_pick_friday(now) + dt.timedelta(days=7), PICK_AT)


def release_utc(friday: dt.date) -> dt.datetime:
    """06:30 UK on the next UK trading day after the Friday (normally Monday)."""
    return uk_at(next_trading_day(friday), RELEASE_AT)


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
    """{candidate id: when it was last taken back to PAPER}."""
    out: dict = {}
    for e in hist:
        if e.get("event") == "demote" and _when(e.get("ts_utc")):
            out[str(e.get("id"))] = _when(e["ts_utc"])
    return out


def promotions_in_week(hist: Sequence[dict], week: str) -> list[dict]:
    return [e for e in hist if e.get("event") == "promote" and e.get("week") == week]


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
    a reason to stop."""
    from .modes import FILES, _data_dir, _read_json, read_modes
    cfg = Path(config_path)
    raw = read_config_strict(cfg)
    modes = read_modes(cfg)
    try:
        _read_json(_data_dir(raw, cfg) / FILES["ian"])   # raises on a broken file (missing is PAPER)
    except ValueError:
        modes["ian"] = UNREADABLE
    return raw, modes


def unknown(cid_or_item, modes: dict) -> bool:
    """Is this candidate's mode not known (its own file broken)? Then nothing is decided for it."""
    cid = cid_or_item.get("id", "") if isinstance(cid_or_item, dict) else str(cid_or_item)
    return cid.startswith("bot:") and str(modes.get(cid[4:]) or "") == UNREADABLE


def bot_modes(config_path) -> dict:
    try:
        from .modes import read_modes
        return read_modes(config_path)
    except Exception:
        return {}


def is_live(cid: str, raw: dict, modes: dict) -> bool:
    """Is this candidate on real money in config.json (or its own file) now?"""
    kind, rest = cid.split(":", 1) if ":" in cid else (cid, "")
    if kind == "bot":
        return str(modes.get(rest) or "").upper() == "LIVE"
    if kind == "tnb":
        tactic, group = rest.split(":", 1)
        mode, groups, tactics = tnb_live_now(raw)
        return mode == "LIVE" or (group in groups and tactic in tactics)
    return False                                          # the lab has no live path


# ------------------------------------------------------------- luck --
_BOOT_CACHE: dict = {}


def bootstrap(nets: Sequence[float], ident: str, resamples: int = RESAMPLES) -> tuple[float, Optional[float]]:
    """(the chance the true average a trade is <= 0, the cautious average a
    trade) from ``resamples`` resamples of the per-trade nets, with the seed
    fixed by the id: the share of resampled averages at or below zero, and
    their LCB_QUANTILE. Fewer than two trades: (1.0, None). Remembered."""
    nets = [float(x) for x in nets]
    n = len(nets)
    if n < 2:
        return 1.0, None
    key = (ident, tuple(nets), resamples)
    if key in _BOOT_CACHE:
        return _BOOT_CACHE[key]
    from ..lab.stats import seed_of
    seed = seed_of(ident)
    try:
        import numpy as np
        arr = np.asarray(nets, dtype=float)
        rng = np.random.default_rng(seed)
        means = arr[rng.integers(0, n, size=(resamples, n))].mean(axis=1)
        p = float((means <= 0).mean())
        lcb = float(np.quantile(means, LCB_QUANTILE))
    except ImportError:                                  # the same test, slower
        rnd = random.Random(seed).randrange
        means = sorted(sum(nets[rnd(n)] for _ in range(n)) / n for _ in range(resamples))
        p = sum(1 for m in means if m <= 0) / resamples
        lcb = means[min(resamples - 1, int(LCB_QUANTILE * (resamples - 1)))]
    if len(_BOOT_CACHE) > 512:
        _BOOT_CACHE.clear()
    _BOOT_CACHE[key] = (round(p, 4), round(lcb, 4))
    return _BOOT_CACHE[key]


def judge(trades: Sequence[dict], ident: str, now: dt.datetime) -> dict:
    """The proof bar and the score of one candidate's trades (already the window's):
    {stats, verdict, luck, lcb, per_week, score, passes, why}."""
    from . import verdicts as V
    s = V.stats(trades)
    code, _why = V.decide(s)
    p, lcb = bootstrap([t["net"] for t in sorted(trades, key=lambda t: t["closed"])], ident)
    first = min((to_utc(t["closed"]) for t in trades), default=None)
    span = max(7.0, (to_utc(now) - first).total_seconds() / 86400.0) if first else 7.0
    per_week = round(s["trades"] * 7.0 / span, 2)
    passes = code == V.PROVEN and p <= LUCK_MAX
    why = []
    if code != V.PROVEN:
        missing = V._missing_for_proven(s)
        if s["net"] <= 0 and s["trades"]:
            missing.append("to be positive after costs")
        why.append("not proven yet: needs " + V._join(missing) if missing else "not proven yet")
    if p > LUCK_MAX:
        why.append(f"could be luck ({round(p * 100)}% chance its true average is not positive; at most "
                   f"{round(LUCK_MAX * 100)}% allowed)")
    return {"stats": s, "verdict": code, "luck": p, "lcb": lcb, "per_week": per_week,
            "score": round(lcb * per_week, 2) if lcb is not None else None, "passes": passes,
            "why": "passes the bar" if passes else "; ".join(why)}


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


def lab_ready_trades(data_dir, start: dt.datetime, end: dt.datetime, stages=None) -> list[tuple[dict, list]]:
    """[(the idea's row, its practice trades)] for every lab idea at READY FOR
    LIVE (or ``stages``), read-only; nothing for a made-up (synthetic) lab."""
    from ..lab import READY_FOR_LIVE
    data = Path(data_dir)
    summ = _read(data / "lab" / "summary.json")
    if isinstance(summ, dict) and summ.get("synthetic"):
        return []
    db = data / "lab" / "lab.sqlite"
    stages = tuple(stages or (READY_FOR_LIVE,))
    q = ",".join("?" for _ in stages)
    ideas = _rows(db, f"SELECT id, name, family, stage FROM candidates WHERE stage IN ({q})", stages)
    if not ideas:
        return []
    from ..coach.records import _is_test_row
    rows = _rows(db, "SELECT * FROM practice_trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND "
                     "closed_utc < ?", (to_utc(start).isoformat(), to_utc(end).isoformat()))
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
    return [(i, by.get(str(i["id"]), [])) for i in ideas]


def _kind_words(kind: str) -> str:
    from .verdicts import _KIND_WORDS
    return _KIND_WORDS.get(kind, kind.lower().replace("_", " "))


def part_words(tactic: str, kind: str) -> str:
    """'momentum continuation, minor pairs'."""
    return f"{tactic.lower().replace('_', ' ')}, {_kind_words(kind)}"


def candidates(data_dir, config_path, now: dt.datetime, hist: Optional[Sequence[dict]] = None,
               lab_stages: Optional[Sequence[str]] = None) -> list[dict]:
    """Every practising candidate, judged on its window, ranked: qualifying
    ones first by score, then the rest by practice GBP after costs. The lab's
    ideas at READY FOR LIVE (``lab_stages``: Home's pool adds PRACTICE)."""
    from ..engine.tactics import APPROACH_NAMES
    from .modes import LIVE_KINDS
    now = to_utc(now)
    end = now + dt.timedelta(seconds=1)
    dem = last_demoted(hist if hist is not None else history(data_dir))
    raw = read_config(config_path) if config_path else {}
    modes = bot_modes(config_path) if config_path else {}
    out: list[dict] = []

    def add(cid, kind, label, part_text, trades, live_path, **extra):
        if not trades:
            return
        j = judge(trades, cid, now)
        out.append(dict({"id": cid, "kind": kind, "label": label, "part": part_text, "live_path": live_path,
                         "live": is_live(cid, raw, modes), "risk_gbp": _median_risk(t.get("risk") for t in trades)},
                        **j, **extra))

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
                mode=str(modes.get(name) or "").upper())
        except Exception as exc:                          # one broken record never stops the rest
            log.warning("weekly: %s: %s", name, exc)
    try:
        for (tactic, kind), trades in sorted(tnb_part_trades(data_dir, oldest, end).items()):
            cid = f"tnb:{tactic}:{kind}"
            d = dem.get(cid)
            trades = [t for t in trades if d is None or to_utc(t["closed"]) >= to_utc(d)]
            path = tactic in APPROACH_NAMES and kind in LIVE_KINDS
            add(cid, "tnb", TNB_LABEL, part_words(tactic, kind), trades, path, tactic=tactic, group=kind,
                magic=TNB_MAGIC, process="trader", mode=tnb_live_now(raw)[0])
    except Exception as exc:
        log.warning("weekly: Trend & Breakout's parts: %s", exc)
    try:
        for idea, trades in lab_ready_trades(data_dir, oldest, end, lab_stages):
            add(f"lab:{idea['id']}", "lab", LAB_LABEL, str(idea.get("name") or idea["id"]), trades, False,
                stage=str(idea.get("stage") or ""))
    except Exception as exc:
        log.warning("weekly: the lab: %s", exc)
    return rank(out)


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
def make_plan(data_dir, config_path, now: dt.datetime, friday: Optional[dt.date] = None,
              ranking: Optional[list] = None) -> dict:
    """This Friday's pick: the ranking, the best candidate with a live path
    (and the best overall, when that is a lab idea), the reasons, the release
    time. Never applies anything."""
    now = to_utc(now)
    friday = friday or last_pick_friday(now)
    ranking = ranking if ranking is not None else candidates(data_dir, config_path, now)
    raw = read_config(config_path) if config_path else {}
    proven = {c["id"] for c in ranking if c.get("passes")}
    reasons: list[str] = []
    pick, lab_pick, extras = None, None, []
    for c in ranking:
        if not c.get("passes"):
            continue
        if c.get("live"):
            continue                                       # already on real money
        if c["kind"] == "lab":
            if lab_pick is None and pick is None:
                lab_pick = c
                reasons.append(f"{c['label']} - {c['part']}: the best, {NEEDS_LIVE_PATH}")
            continue
        if not c.get("live_path"):
            reasons.append(f"{c['label']} {c['part']}: no live path for this kind of market - skipped")
            continue
        if c.get("mode") != "PAPER":
            who = TNB_LABEL if c["kind"] == "tnb" else c["label"]
            reasons.append(f"{c['label']}{' ' + c['part'] if c['part'] else ''}: {who} is "
                           f"{c.get('mode') or 'in no known mode'}, not on PAPER - skipped")
            continue
        if c["kind"] == "tnb":
            _mode, groups, tactics = tnb_live_now(raw)
            add = tnb_add(c["tactic"], c["group"], groups, tactics, proven)
            if not add["ok"]:
                reasons.append(f"{c['label']} {c['part']}: {add['why']}")
                continue
            extras = add["extras"]
        pick = c
        reasons.append(f"{c['label']}{' ' + c['part'] if c['part'] else ''}: the best with a live path "
                       f"(about £{c['score']:.2f} a week, cautiously)")
        break
    if pick is None and not reasons:
        reasons.append(NOTHING_PROVED)
    rel = release_utc(friday)
    return {"schema": SCHEMA, "friday": friday.isoformat(), "made_utc": _iso(now), "release_utc": _iso(rel),
            "expires_utc": _iso(uk_at(friday + dt.timedelta(days=7), PICK_AT)), "week": iso_week(rel),
            "status": PLANNED if pick else NONE, "pick": _brief(pick) if pick else None,
            "extras": extras, "lab_pick": _brief(lab_pick) if lab_pick else None,
            "reasons": reasons, "ranking": [_brief(c) for c in ranking[:30]],
            "rule": RULE_TEXT}


RULE_TEXT = (f"Practice trades of the last {LOOKBACK_DAYS} days, after costs: proven by the bots' rule (30+ trades "
             f"over 5+ UK days, positive, £1.20+ won for every £1 lost, more up days than down) and at most a "
             f"{round(LUCK_MAX * 100)}% chance its true average is not positive ({RESAMPLES:,} resamples). Ranked "
             f"by a cautious average a trade times trades a week. One promotion a week, released 06:30 UK on the "
             f"next trading day; after {JUDGE_AFTER} live trades net <= 0 goes back to practice, and a live loss "
             f"of {EARLY_EXIT_RISKS:g} x its risk a trade at any point does too.")


def _brief(c: dict) -> dict:
    s = c.get("stats") or {}
    keep = ("id", "kind", "label", "part", "live", "live_path", "mode", "verdict", "luck", "lcb", "per_week", "score",
            "passes", "why", "risk_gbp", "bot", "magic", "process", "tactic", "group")
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


def live_verdict(trades: Sequence[dict], risk_gbp: float) -> tuple[str, str]:
    """("demote" | "keep" | "wait", why) from a promotion's live trades."""
    n = len(trades)
    net = round(sum(float(t["net"]) for t in trades), 2)
    limit = round(EARLY_EXIT_RISKS * max(float(risk_gbp or FALLBACK_RISK_GBP), 0.01), 2)
    if net <= -limit:
        return "demote", f"its live loss reached £{-net:.2f}, {EARLY_EXIT_RISKS:g} x its £{risk_gbp:.2f} risk a trade"
    if n >= JUDGE_AFTER:
        if net <= 0:
            return "demote", f"{n} live trades made £{net:.2f} after costs"
        return "keep", f"{n} live trades made £{net:+.2f} after costs: it stays"
    return "wait", f"{n} of {JUDGE_AFTER} live trades, £{net:+.2f} so far"


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
    raise ValueError(NEEDS_LIVE_PATH)


def demote_change(item: dict, raw: dict, modes: dict, keep: set) -> dict:
    """What taking one promotion back to PAPER writes, without writing:
    {already (nothing of ours to take back), changes, kw, lost}."""
    if unknown(item, modes):
        raise ValueError(f"{item['label']}'s own file could not be read: its mode is not known")
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
    files = [cfg, _data_dir(read_config_strict(cfg), cfg) / "ian.json"]
    before = {p: (p.read_bytes() if p.exists() else None) for p in files}
    try:
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
    """One check (the watchdog's, every CHECK_SECONDS): make Friday's pick when
    due, release it at its time, judge what is live, take back what failed,
    then restart the bots whose mode changed. Returns what it did, in words.

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

    # 0. last week's pick, never released (HALT all week): it lapses at this week's pick
    friday = last_pick_friday(now)
    old = _read(wdir(data) / LATEST)
    if isinstance(old, dict) and old.get("status") == PLANNED and old.get("friday") != friday.isoformat() \
            and now >= (_when(old.get("expires_utc")) or now):
        old["status"] = EXPIRED
        _write_atomic(plan_path(data, dt.date.fromisoformat(old["friday"])), old)
        _write_atomic(wdir(data) / LATEST, old)
        actions.append(f"weekly: the pick of {old['friday']} was never released and has lapsed")

    # 1. Friday's pick (catching up until its release)
    ppath = plan_path(data, friday)
    plan = _read(ppath)
    if not isinstance(plan, dict) and now < release_utc(friday):
        try:
            plan = make_plan(data, config_path, now, friday)
            _write_atomic(ppath, plan)
            _write_atomic(wdir(data) / LATEST, plan)
            p = plan.get("pick")
            actions.append(f"weekly: the pick for {friday.isoformat()} is "
                           + (f"{p['label']} {p.get('part') or ''}".strip() if p else "nothing (nothing proved itself)"))
        except Exception as exc:
            log.exception("weekly pick")
            actions.append(f"weekly: the pick could not be made ({exc}); trying again at the next check")
            plan = None

    # 2. the vetoes: this week's plan, and every live promotion by the plan it came from
    actions += _vetoes(data, state, plan, ppath)
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


def _vetoes(data: Path, state: dict, plan: Optional[dict], ppath: Path) -> list[str]:
    """Aaron's veto (a VETO-<Friday> file): this week's pick not yet released
    is cancelled; any live promotion whose own plan was vetoed - the pick and
    every part that went live with it - is marked to go back to PAPER."""
    out = []
    if isinstance(plan, dict) and plan.get("status") in (PLANNED, APPLIED) \
            and veto_path(data, dt.date.fromisoformat(plan["friday"])).exists():
        plan["status"] = VETOED
        _save_plan(data, ppath, plan)
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
    if marked:
        _save_state(data, state)
        out.append("weekly: a vetoed pick goes back to PAPER")
    return out


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
        _append_history(data, _event("promote", it, now, "this week's pick" if it is pick else
                                     f"proved itself too, and goes live with {pick['label']} {pick.get('part')}",
                                     plan=it.get("plan")))
    _save_state(data, state)
    if pick.get("plan"):
        pp = plan_path(data, dt.date.fromisoformat(pick["plan"]))
        p = _read(pp)
        if isinstance(p, dict) and p.get("status") == PLANNED:
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
    pick = plan["pick"]
    week = iso_week(now)
    if promotions_in_week(hist, week):
        plan["status"] = SKIPPED
        plan["skipped_why"] = f"a promotion was already made in {week} (one a week)"
        _save_plan(data, ppath, plan)
        return [f"weekly: {pick['label']} not released - {plan['skipped_why']}"]
    proven = {pick["id"]} | set(plan.get("extras") or ())

    def failed(exc) -> list[str]:
        plan["last_error"] = str(exc)
        _save_plan(data, ppath, plan)
        log.warning("weekly release: %s", exc)
        return [f"weekly: {pick['label']} could not be switched LIVE ({exc}); nothing was changed, trying again"]
    try:
        raw, modes = strict_view(config_path)
        ch = promote_change(pick, raw, modes, proven)
    except Exception as exc:
        return failed(exc)
    items = [dict(pick, live=True, since_utc=_iso(now), week=week, plan=plan["friday"])]
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
    if not ch["already"]:
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
    return [f"weekly: {pick['label']} {pick.get('part') or ''} switched LIVE (this week's pick)".replace("  ", " ")]


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
        verdict, why = live_verdict(trades, float(it.get("risk_gbp") or FALLBACK_RISK_GBP))
        if it.get("demoting") or it.get("demote_why"):     # an intent is never dropped by an error
            verdict, why = "demote", it.get("demoting") or it["demote_why"]
        it["record"] = {"trades": len(trades), "net": round(sum(t["net"] for t in trades), 2), "source": source,
                        "checked_utc": _iso(now), "verdict": verdict, "why": why}
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
        added = _queue_restart(state, it.get("process"))
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
    * ``counts``: tests running, making money, losing, too early, ready for
      real money (passes the bar);
    * ``top``: the three best tests by practice GBP after costs over the last
      LOOKBACK_DAYS days - the bots, Trend & Breakout's parts and the lab's
      ideas ranked together;
    * ``pick``: this week's pick.

    The pool (``candidates``, with the lab's PRACTICE ideas too) is worked out
    at most once every ``cache_seconds``; the real money is worked out on every
    call, from the page's own refresh. Never raises."""
    now = to_utc(now or utcnow())
    out = {"live": [], "counts": {"running": 0, "making": 0, "losing": 0, "early": 0, "ready": 0}, "top": [],
           "pick": pick_tile(None, now)}
    if not data_dir:
        return out
    config_path = config_path or Path(data_dir) / "config.json"
    try:
        pool = _pool(data_dir, config_path, now, cache_seconds)
        out["counts"] = pool_counts(pool)
        out["top"] = [_small(c) for c in sorted(pool, key=lambda c: -float(c["stats"]["net"]))[:3]]
    except Exception as exc:
        log.warning("home board: %s", exc)
    try:
        out["live"] = live_tiles(data_dir, now, deals, open_tickets, open_live)
    except Exception as exc:
        log.warning("home board live: %s", exc)
    latest = _read(wdir(data_dir) / LATEST)
    out["pick"] = pick_tile(latest if isinstance(latest, dict) else None, now)
    return out


def _pool(data_dir, config_path, now: dt.datetime, cache_seconds: float) -> list[dict]:
    from ..lab import PRACTICE, READY_FOR_LIVE
    key = str(data_dir)
    hit = _BOARD_CACHE.get(key)
    if hit and 0 <= (now - hit[0]).total_seconds() < cache_seconds:
        return hit[1]
    pool = candidates(data_dir, config_path, now, lab_stages=(PRACTICE, READY_FOR_LIVE))
    if len(_BOARD_CACHE) > 16:
        _BOARD_CACHE.clear()
    _BOARD_CACHE[key] = (now, pool)
    return pool


def chip_of(trades: int, net: Optional[float]) -> str:
    if trades < TOO_EARLY_TRADES or net is None:
        return TOO_EARLY
    return MAKING if net > 0 else LOSING if net < 0 else TOO_EARLY


def pool_counts(pool: Sequence[dict]) -> dict:
    chips = [chip_of(int(c["stats"]["trades"]), c["stats"]["net"]) for c in pool]
    return {"running": len(pool), "making": chips.count(MAKING), "losing": chips.count(LOSING),
            "early": chips.count(TOO_EARLY), "ready": sum(1 for c in pool if c.get("passes"))}


def short_name(c: dict) -> str:
    """A test's short name: the part for Trend & Breakout's ('T&B momentum
    continuation, minor pairs'), the idea's for the lab's, the bot's otherwise."""
    if c.get("kind") == "tnb":
        return f"T&B {c.get('part') or ''}".strip()
    if c.get("kind") == "lab":
        return str(c.get("part") or "lab idea")
    return str(c.get("label") or c.get("id"))


def _small(c: dict) -> dict:
    s = c["stats"]
    return {"id": c["id"], "name": short_name(c), "net": s["net"], "trades": s["trades"], "win_rate": s["win_rate"],
            "chip": chip_of(int(s["trades"]), s["net"]), "ready": bool(c.get("passes")), "live": bool(c.get("live"))}


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
        rel = _when(latest.get("release_utc"))
        if latest["status"] == APPLIED:
            when = "LIVE since " + uk(_when(latest.get("applied_utc")) or now).strftime("%a %d %b")
        else:
            when = ("waiting: HALT" if latest.get("waiting") == "HALT" else
                    "LIVE from " + (uk(rel).strftime("%a %d %b %H:%M") if rel else "Monday"))
        return {"name": short_name(p), "when": when, "none": False}
    return {"name": NOTHING_PROVED, "when": when_next, "none": True}


def weekly_report(data_dir) -> Optional[dict]:
    """The pulse's reports/live/weekly.json: the latest plan and what is live
    now, whitelisted. None before the weekly pick has ever run."""
    d = wdir(data_dir)
    if not d.exists():
        return None
    latest = _read(d / LATEST)
    state = load_state(data_dir)
    keep = ("friday", "made_utc", "release_utc", "status", "pick", "lab_pick", "reasons", "week", "waiting",
            "skipped_why", "last_error")
    rep = {"note": "the weekly pick: practice candidates ranked, one to real money a week"}
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
    luck = "-" if c.get("luck") is None else f"{round(c['luck'] * 100)}%"
    lcb = "-" if c.get("lcb") is None else f"£{c['lcb']:+.2f}"
    score = "-" if c.get("score") is None else f"£{c['score']:+.2f}/wk"
    return (f"  {('READY' if c.get('passes') else ''):5} {c['label'][:22]:22} {(c.get('part') or '')[:40]:40} "
            f"{c.get('trades') or 0:>4} trades  £{(c.get('net') or 0):+8.2f}  luck {luck:>4}  lcb {lcb:>7}  {score}")


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
