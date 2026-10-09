"""Runs the whole research on ticks already in the store.

    coverage -> bars cache -> event study (per day) + replays (per variant
    and day, each with its cost and latency stresses) -> walk-forward ->
    registry -> report

Every job is cached on disk under its inputs (the variant's settings, the
cost model, the tick files it reads and a fingerprint of the rider code), so
an interrupted run picks up where it stopped and a re-run after a code
change recomputes only what changed. Jobs run in parallel processes.
"""
from __future__ import annotations

import concurrent.futures as cf
import dataclasses
import datetime as dt
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ...broker.base import CalendarEvent
from ...contracts import SymbolSpec
from ..config import RiderConfig
from . import SYNTHETIC_LABEL
from .costs import CostModel
from .eventstudy import combine, study_day
from .labeler import DEFAULT_SETTINGS, RunSetting
from .registry import Registry
from .replay import day_bars, load_day, replay_day, seed_days
from .stats import short_line, summarise
from .tickstore import TickStore, add_days
from .variants import Variant, VARIANTS
from .walkforward import walk_forward

RIDER_DIR = Path(__file__).resolve().parents[1]


# research modules that only summarise finished jobs: changing them never invalidates the job cache
POST_PROCESSING = {"report.py", "publish.py", "__main__.py", "registry.py", "walkforward.py", "stats.py"}


def code_stamp() -> str:
    """A fingerprint of the rider code that decides a job's result (the core
    and the research modules that replay, label and measure)."""
    h = hashlib.sha256()
    for p in sorted(RIDER_DIR.rglob("*.py")):
        if p.parent.name == "research" and p.name in POST_PROCESSING:
            continue
        h.update(str(p.relative_to(RIDER_DIR)).encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


# ------------------------------------------------------------ (de)serialise --

def spec_to_dict(s: SymbolSpec) -> dict:
    d = dataclasses.asdict(s)
    d["filling_modes"] = list(d.get("filling_modes") or ())
    return d


def spec_from_dict(d: dict) -> SymbolSpec:
    d = dict(d)
    d["filling_modes"] = tuple(d.get("filling_modes") or ("IOC", "FOK"))
    names = {f.name for f in dataclasses.fields(SymbolSpec)}
    return SymbolSpec(**{k: v for k, v in d.items() if k in names})


def event_to_dict(e: CalendarEvent) -> dict:
    d = dataclasses.asdict(e)
    d["time_utc"] = e.time_utc.isoformat()
    return d


def event_from_dict(d: dict) -> CalendarEvent:
    d = dict(d)
    t = dt.datetime.fromisoformat(d["time_utc"])
    d["time_utc"] = t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
    names = {f.name for f in dataclasses.fields(CalendarEvent)}
    return CalendarEvent(**{k: v for k, v in d.items() if k in names})


# ------------------------------------------------------------------ plan --

@dataclass
class Plan:
    root: Path                                   # research dir: ticks/, cache/, registry.sqlite
    out_dir: Path                                # report.md, summary.json, trades.csv
    symbols: list
    days: list                                   # the replay days asked for (oldest first)
    data_source: str
    synthetic: bool = False
    rider_cfg: RiderConfig = field(default_factory=RiderConfig)
    variants: list = field(default_factory=lambda: list(VARIANTS))
    workers: int = 1
    cost_mults: tuple = (1.5, 2.0)
    latency_stress_ms: tuple = (1000.0, 2000.0)
    settings: tuple = DEFAULT_SETTINGS
    primary: int = 1
    wf_is: int = 4
    wf_oos: int = 2
    wf_gap: int = 1
    wf_min_trades: int = 20
    min_days: int = 5
    min_ticks: int = 5000
    max_gap_minutes: float = 30.0
    min_trades: int = 30
    n_baseline: int = 120
    seed: int = 11
    account_currency: str = "GBP"
    use_cache: bool = True
    ticks_dir: Optional[Path] = None

    @property
    def ticks_root(self) -> Path:
        return Path(self.ticks_dir) if self.ticks_dir else Path(self.root) / "ticks"

    @property
    def cache_dir(self) -> Path:
        return Path(self.root) / "cache"

    def cost(self) -> CostModel:
        c = self.rider_cfg
        return CostModel(slippage_pips=c.slippage_pips, latency_ms=c.latency_ms)

    def stresses(self) -> list[CostModel]:
        base = self.cost()
        return ([base.stressed(m) for m in self.cost_mults] +
                [base.delayed(L) for L in self.latency_stress_ms])


# ------------------------------------------------------------------ jobs --

def _file_stamp(store: TickStore, symbols: Sequence[str], days: Sequence[str]) -> list:
    out = []
    for d in days:
        for s in symbols:
            p = store.day_path(s, d)
            if p.exists():
                st = p.stat()
                out.append([s, d, st.st_size, int(st.st_mtime)])
    return out


def _job_key(args: dict) -> str:
    blob = json.dumps({k: v for k, v in args.items() if k not in ("cache_dir",)}, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode()).hexdigest()[:20]


def same_settings(variants: Sequence[Variant], base: RiderConfig) -> dict[str, str]:
    """METHOD_ID -> the METHOD_ID whose replay it shares: variants with
    identical settings (RMR-GUARDS-ON and RMR-BASE with the shipped defaults)
    are replayed once and reported under each name."""
    first: dict[str, str] = {}
    out: dict[str, str] = {}
    for v in variants:
        key = json.dumps(v.config(base).to_dict(), sort_keys=True, default=str)
        out[v.method_id] = first.setdefault(key, v.method_id)
    return out


def run_job(args: dict) -> dict:
    """One unit of work (in a worker process). Cached on disk by its inputs."""
    cache = Path(args["cache_dir"]) / "jobs" / args["kind"] / f"{_job_key(args)}.json"
    if args.get("use_cache", True):
        try:
            res = json.loads(cache.read_text())
            res["_cached"] = True
            return res
        except Exception:
            pass
    t0 = time.monotonic()
    store = TickStore(args["ticks_root"])
    specs = {k: spec_from_dict(v) for k, v in args["specs"].items()}
    calendar = [event_from_dict(e) for e in args.get("calendar", [])]
    cfg = RiderConfig.from_dict(args["cfg"])
    kind = args["kind"]
    if kind == "bars":
        day_bars(store, args["symbol"], args["day"], Path(args["cache_dir"]))
        res: dict = {}
    elif kind == "study":
        settings = [RunSetting(**s) for s in args["settings"]]
        res = study_day(store, args["day"], args["symbols"], specs, calendar, cfg, settings, args["primary"],
                        n_baseline=args["n_baseline"], seed=args["seed"], slippage_pips=cfg.slippage_pips,
                        cache_dir=Path(args["cache_dir"]), account_currency=args["account_currency"])
    elif kind == "replay":
        data = load_day(store, args["day"], args["symbols"], specs, calendar, cache_dir=Path(args["cache_dir"]),
                        account_currency=args["account_currency"])
        cost = CostModel(**args["cost"])
        stresses = [CostModel(**c) for c in args["stresses"]]
        res = replay_day(cfg, data, cost, args["variant"], stresses)
        res["_ticks"] = data.tick_count()
    else:
        raise ValueError(kind)
    res["_seconds"] = round(time.monotonic() - t0, 1)
    if args.get("use_cache", True) and kind != "bars":
        cache.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache.with_name(cache.name + ".tmp")
        tmp.write_text(json.dumps(res, default=str))
        tmp.replace(cache)
    return res


def _run_all(jobs: list[dict], workers: int, progress: Callable[[str], None], what: str) -> list[dict]:
    out: list[Optional[dict]] = [None] * len(jobs)
    t0 = time.monotonic()
    if workers <= 1 or len(jobs) <= 1:
        for i, j in enumerate(jobs):
            out[i] = run_job(j)
            progress(f"  {what}: {i + 1} of {len(jobs)} done ({_label(j)}, {out[i].get('_seconds', 0)} s)")
        return out                                            # type: ignore[return-value]
    with cf.ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_job, j): i for i, j in enumerate(jobs)}
        done = 0
        for f in cf.as_completed(futs):
            i = futs[f]
            out[i] = f.result()
            done += 1
            el = time.monotonic() - t0
            eta = el / done * (len(jobs) - done)
            progress(f"  {what}: {done} of {len(jobs)} done ({_label(jobs[i])}, {out[i].get('_seconds', 0)} s; "
                     f"about {eta / 60:.0f} min left)")
    return out                                                # type: ignore[return-value]


def _label(j: dict) -> str:
    if j["kind"] == "replay":
        return f"{j['variant']} {j['day']}"
    if j["kind"] == "study":
        return f"event study {j['day']}"
    return f"{j.get('symbol', '')} {j.get('day', '')}"


# ------------------------------------------------------------------- run --

def run(plan: Plan, specs: dict, calendar: Sequence[CalendarEvent], progress: Callable[[str], None] = print,
        fetch_report: Optional[dict] = None, stamp: Optional[str] = None) -> dict:
    from .report import build_summary, write_outputs
    store = TickStore(plan.ticks_root)
    stamp = stamp or code_stamp()
    symbols = [s for s in plan.symbols if s in specs]
    cov = store.coverage(symbols, plan.days, plan.min_days, plan.min_ticks, plan.max_gap_minutes)
    days = list(cov.usable_days)
    progress(f"Data: {cov.statement()}")
    base_args = {"ticks_root": str(plan.ticks_root), "cache_dir": str(plan.cache_dir), "use_cache": plan.use_cache,
                 "specs": {s: spec_to_dict(specs[s]) for s in symbols},
                 "calendar": [event_to_dict(e) for e in calendar], "symbols": symbols,
                 "account_currency": plan.account_currency, "code": stamp}
    # bars for every day a replay seeds from
    need = sorted({d for day in days for d in seed_days(store, symbols, day)} | set(days))
    bar_jobs = [{**base_args, "kind": "bars", "symbol": s, "day": d, "cfg": {}, "use_cache": False,
                 "calendar": [], "specs": {}} for d in need for s in symbols if store.day_path(s, d).exists()]
    if days:
        progress(f"Preparing one- and five-minute bars ({len(bar_jobs)} market-days)...")
        _run_all(bar_jobs, plan.workers, lambda m: None, "bars")
    variants: list[Variant] = list(plan.variants)
    cost = plan.cost()
    stresses = plan.stresses()
    shared = same_settings(variants, plan.rider_cfg)
    jobs: list[dict] = []
    for v in variants:
        if shared[v.method_id] != v.method_id:
            continue                                          # the same settings as an earlier variant: replayed once
        cfg = v.config(plan.rider_cfg)
        for d in days:
            jobs.append({**base_args, "kind": "replay", "variant": v.method_id, "day": d, "cfg": cfg.to_dict(),
                         "cost": cost.to_dict(), "stresses": [c.to_dict() for c in stresses],
                         "files": _file_stamp(store, symbols, seed_days(store, symbols, d) + [d, add_days(d, 1)])})
    for d in days:
        jobs.append({**base_args, "kind": "study", "day": d, "cfg": plan.rider_cfg.to_dict(),
                     "settings": [dataclasses.asdict(s) for s in plan.settings], "primary": plan.primary,
                     "n_baseline": plan.n_baseline, "seed": plan.seed,
                     "files": _file_stamp(store, symbols, seed_days(store, symbols, d) + [d, add_days(d, 1)])})
    if jobs:
        progress(f"Replaying {len(variants)} variant(s) over {len(days)} day(s) and running the event study "
                 f"({len(jobs)} jobs, {plan.workers} worker(s)). This is the slow part.")
    results = _run_all(jobs, plan.workers, progress, "research") if jobs else []
    trades: dict[str, dict[str, list]] = {v.method_id: {} for v in variants}
    counters: dict[str, dict] = {v.method_id: {} for v in variants}
    notes: list[str] = []
    study_days: list[dict] = []
    seconds = 0.0
    ticks_replayed = 0
    reused = 0
    for j, r in zip(jobs, results):
        if r.get("_cached"):
            reused += 1
        else:
            seconds += float(r.get("_seconds", 0) or 0)
        if j["kind"] == "study":
            study_days.append(r)
            continue
        vid = j["variant"]
        if vid == variants[0].method_id:
            ticks_replayed += int(r.get("_ticks", 0) or 0)
        for label, block in r.items():
            if label.startswith("_"):
                continue
            trades[vid].setdefault(label, []).extend(block["trades"])
            acc = counters[vid].setdefault(label, {})
            for k, x in block["counters"].items():
                acc[k] = acc.get(k, 0) + x
        if vid == variants[0].method_id:
            notes += r.get("_notes", [])
    for v in variants:
        src = shared[v.method_id]
        if src != v.method_id:
            trades[v.method_id] = {label: [dict(t, variant=v.method_id) for t in ts]
                                   for label, ts in trades[src].items()}
            counters[v.method_id] = json.loads(json.dumps(counters[src]))
            notes.append(f"{v.method_id} has exactly the settings of {src}: replayed once, reported under both")
    es = combine(study_days) if study_days else None
    normal = cost.label
    grid = [v for v in variants if v.grid]
    wf = walk_forward({v.method_id: trades[v.method_id].get(normal, []) for v in variants}, grid, days,
                      is_min=plan.wf_is, oos=plan.wf_oos, gap=plan.wf_gap, min_trades=plan.wf_min_trades)
    summary = build_summary(plan, symbols, days, cov, trades, counters, wf, es, stresses, cost, notes, stamp,
                            fetch_report, seconds, ticks_replayed)
    summary["compute"]["jobs"] = len(jobs)
    summary["compute"]["jobs_from_cache"] = reused
    record_registry(plan, summary, variants, stamp)
    write_outputs(plan.out_dir, summary, trades.get(variants[0].method_id, {}).get(normal, []))
    progress(f"Report written: {Path(plan.out_dir) / 'report.md'}")
    return summary


def record_registry(plan: Plan, summary: dict, variants: Sequence[Variant], stamp: str) -> None:
    """Every variant run goes into the registry, with its data source."""
    reg = Registry(Path(plan.root) / "registry.sqlite")
    try:
        methods = [{"method_id": v.method_id, "description": v.description, "hypothesis": v.hypothesis,
                    "params": v.params(plan.rider_cfg)} for v in variants]
        methods.append({"method_id": "RMR-WF-SELECT",
                        "description": "Walk-forward: in each fold choose the grid setting with the best in-sample "
                                       "expectancy averaged with its neighbours; trade it on the next (purged) days.",
                        "hypothesis": "Choosing settings on past days and trading them on later days still makes money "
                                      "after costs (the honest test of any tuning).",
                        "params": summary.get("walk_forward", {}).get("rules", {})})
        reg.seed(methods)
        src = "SYNTHETIC" if plan.synthetic else "REAL"
        days = summary.get("days_replayed", [])
        period = f"{days[0]} to {days[-1]} ({len(days)} days)" if days else "no usable days"
        markets = ",".join(summary.get("markets", []))
        rows = []
        for row in summary.get("variants", []):
            n = row["normal"]
            rid = reg.record({
                "method_id": row["method_id"], "data_source": src, "markets": markets, "period": period,
                "trades": n["trades"], "win_rate": n.get("win_rate"), "expectancy": n.get("expectancy_pips"),
                "profit_factor": n.get("profit_factor"), "drawdown": n.get("max_drawdown_pips"),
                "mfe_capture": n.get("mfe_capture"), "cost_sensitivity": row.get("cost_sensitivity"),
                "oos_result": row.get("oos_line"), "net_pips": n.get("net_pips"), "net_money": n.get("net_money"),
                "fragile": row.get("fragile"), "report_dir": str(plan.out_dir), "code_stamp": stamp,
                "notes": SYNTHETIC_LABEL if plan.synthetic else ""})
            rows.append(rid)
        wf = summary.get("walk_forward", {})
        o = wf.get("oos_result") or summarise([])
        rid = reg.record({
            "method_id": "RMR-WF-SELECT", "data_source": src, "markets": markets, "period": period,
            "trades": o.get("trades", 0), "win_rate": o.get("win_rate"), "expectancy": o.get("expectancy_pips"),
            "profit_factor": o.get("profit_factor"), "drawdown": o.get("max_drawdown_pips"),
            "mfe_capture": o.get("mfe_capture"), "cost_sensitivity": "",
            "oos_result": (short_line(o) if wf.get("enough_days") else wf.get("statement", "")),
            "net_pips": o.get("net_pips"), "net_money": o.get("net_money"), "fragile": False,
            "report_dir": str(plan.out_dir), "code_stamp": stamp,
            "notes": (SYNTHETIC_LABEL + "; " if plan.synthetic else "") + "out-of-sample days only"})
        rows.append(rid)
        summary["registry"] = {"path": str(Path(plan.root) / "registry.sqlite"), "run_ids": rows,
                               "runs_in_registry": reg.run_count()}
    finally:
        reg.close()
    # the summary on disk carries the registry ids too
    try:
        p = Path(plan.out_dir) / "summary.json"
        if p.exists():
            d = json.loads(p.read_text())
            d["registry"] = summary["registry"]
            p.write_text(json.dumps(d, indent=1, default=str))
    except Exception:
        pass


def default_workers() -> int:
    return max(1, (os.cpu_count() or 2) - 1)
