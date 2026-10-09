"""Rapid Momentum Rider research - one command.

    python -m mintel.rider.research --config data/config.json [--days 10]
        [--symbols auto|all|EURUSD,GBPUSD,...] [--upload] [--synthetic]

REAL mode (the machine with MetaTrader - the Mac through the bridge):
connects the way the trader does (mintel/run.py build_broker), fetches the
broker's own tick history for the last ``--days`` complete weekdays (plus a
seed day before them) into data/rider-research/ticks/ - resuming anything
already there - with the specs and the MetaTrader calendar, disconnects, and
runs the research offline. Results: data/rider-research/<date>/report.md,
summary.json and trades.csv; ``--upload`` publishes them to the reports
branch at reports/rider-research/<date>/.

SYNTHETIC mode (``--synthetic``): no broker; a seeded generator makes up the
ticks and the same pipeline runs. Everything is written under
data/rider-research/SYNTHETIC/ and data/rider-research/<date>-SYNTHETIC/ and
labelled SYNTHETIC - NOT PERFORMANCE.

Other options: --workers N (parallel processes), --variants
default|all|base|ID,ID, --no-fetch (use stored ticks only), --max-gb (tick
store budget), --out DIR (research directory).

PROBE (``--probe``): one quick check that past ticks can be read. It
connects, fetches one past hour of EURUSD (10:00-11:00 UTC on the last
complete weekday, plus the last complete hour to compare) exactly as the
research would, prints how many ticks MetaTrader returned and kept, with
the first and last times, and disconnects. Exit code 0: past ticks are
reachable; 5: they are not.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from ...contracts import FX_CURRENCIES, classify_fx
from . import REAL_SOURCE, SYNTHETIC_LABEL, SYNTHETIC_SOURCE

UTC = dt.timezone.utc
AUTO_SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "EURJPY", "GBPJPY")


def resolve_symbols(broker_names: Sequence[str], want: str) -> list[str]:
    """Broker names for the wanted pairs (the shortest name per pair, so a
    plain EURUSD beats EURUSD.x). ``want``: auto, all, or a comma list."""
    by_pair: dict[tuple[str, str], str] = {}
    for name in broker_names:
        b, q, _ = classify_fx(name)
        if b in FX_CURRENCIES and q in FX_CURRENCIES and b != q:
            cur = by_pair.get((b, q))
            if cur is None or len(name) < len(cur) or (len(name) == len(cur) and name < cur):
                by_pair[(b, q)] = name
    w = (want or "auto").strip()
    if w.lower() == "all":
        return sorted(by_pair.values())
    pairs = AUTO_SYMBOLS if w.lower() == "auto" else [x.strip().upper() for x in w.split(",") if x.strip()]
    out = []
    for p in pairs:
        b, q, _ = classify_fx(p)
        name = by_pair.get((b, q))
        if name and name not in out:
            out.append(name)
    return out


def _say(msg: str) -> None:
    print(msg, flush=True)


def _connect(broker, wait_s: float = 90.0) -> bool:
    t0 = time.monotonic()
    while True:
        try:
            if broker.connect():
                return True
        except Exception as exc:
            _say(f"MetaTrader did not answer yet ({exc})")
        if time.monotonic() - t0 > wait_s:
            return False
        time.sleep(5.0)


def fetch_real(args, root: Path, store, days: list[str]) -> tuple[list[str], dict, list, dict, str]:
    """Connect, fetch ticks/specs/calendar, disconnect."""
    from ...config import Config
    from ...run import build_broker
    from .pipeline import event_from_dict, event_to_dict, spec_to_dict
    from .tickstore import add_days, fetch_ticks, save_json
    cfg = Config.load(args.config)
    broker = build_broker(cfg)
    _say("Connecting to MetaTrader...")
    if not _connect(broker):
        raise SystemExit("Could not connect to MetaTrader (is the bot's bridge running?). Nothing was fetched.")
    try:
        acct = broker.account()
        ccy = str(getattr(acct, "currency", "") or "GBP").upper()
        names = list(broker.symbols())
        symbols = resolve_symbols(names, args.symbols)
        specs = {}
        for s in symbols:
            try:
                sp = broker.spec(s)
            except Exception:
                sp = None
            if sp is not None and sp.is_tradable():
                specs[s] = sp
            else:
                _say(f"{s}: no usable contract specification - left out")
        symbols = [s for s in symbols if s in specs]
        if not symbols:
            raise SystemExit("None of the requested FX markets is available at this broker.")
        _say(f"Markets: {', '.join(symbols)}; account currency {ccy}")
        first, last = days[0], days[-1]
        span = []
        d = dt.date.fromisoformat(add_days(first, -4))
        while d <= dt.date.fromisoformat(add_days(last, 1)):
            span.append(d.isoformat())
            d += dt.timedelta(days=1)
        _say(f"Fetching every tick for {len(symbols)} markets over {len(span)} calendar days "
             f"(resumes what is already stored)...")
        rep = fetch_ticks(broker, store, symbols, span, chunk_minutes=int(args.chunk_minutes),
                          max_bytes=float(args.max_gb) * 1e9, progress=_say)
        for n in rep.notes[-5:]:
            _say(n)
        cal_path = root / "calendar.json"
        try:
            old = {e["event_id"]: e for e in json.loads(cal_path.read_text())}
        except Exception:
            old = {}
        try:
            start = dt.datetime.fromisoformat(span[0]).replace(tzinfo=UTC)
            end = dt.datetime.fromisoformat(span[-1]).replace(tzinfo=UTC) + dt.timedelta(days=1)
            evs = broker.calendar(start, end, list(FX_CURRENCIES)) or []
            for e in evs:
                old[str(e.event_id)] = event_to_dict(e)
            _say(f"Calendar: {len(evs)} events from MetaTrader")
        except Exception as exc:
            _say(f"Could not read the MetaTrader calendar ({exc}); news is replayed from what was stored before"
                 if old else f"Could not read the MetaTrader calendar ({exc}); news cannot be replayed")
        save_json(cal_path, sorted(old.values(), key=lambda e: e["time_utc"]))
        save_json(root / "specs.json", {"account_currency": ccy, "specs": {s: spec_to_dict(specs[s]) for s in symbols}})
        calendar = [event_from_dict(e) for e in old.values()]
        return symbols, specs, calendar, rep.to_dict(), ccy
    finally:
        try:
            broker.disconnect()                 # leave the bridge (and the trader) running
        except Exception:
            pass


def run_probe(args) -> int:
    """--probe: can past ticks be read from here? (tickstore.probe)"""
    from ...config import Config
    from ...run import build_broker
    from .tickstore import probe
    broker = build_broker(Config.load(args.config))
    _say("Connecting to MetaTrader...")
    if not _connect(broker):
        _say("Could not connect to MetaTrader (is the bot's bridge running?). Nothing was asked.")
        return 3
    try:
        try:
            names = list(broker.symbols())
        except Exception:
            names = []
        sym = (resolve_symbols(names, "EURUSD") or ["EURUSD"])[0]
        _say(f"Probe: can past ticks of {sym} be read? (each empty answer is asked again for up to about 15 s)")
        res = probe(broker, sym)
        for line in res["lines"]:
            _say(line)
        return 0 if res["reachable"] else 5
    finally:
        try:
            broker.disconnect()                 # leave the bridge (and the trader) running
        except Exception:
            pass


def load_stored(root: Path) -> tuple[list[str], dict, list, str]:
    from .pipeline import event_from_dict, spec_from_dict
    try:
        d = json.loads((root / "specs.json").read_text())
    except Exception:
        raise SystemExit(f"No stored contract specifications in {root} - run once without --no-fetch.")
    specs = {k: spec_from_dict(v) for k, v in d.get("specs", {}).items()}
    try:
        cal = [event_from_dict(e) for e in json.loads((root / "calendar.json").read_text())]
    except Exception:
        cal = []
    return sorted(specs), specs, cal, str(d.get("account_currency") or "GBP")


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.rider.research",
                                 description="Rapid Momentum Rider research on real (or synthetic) ticks")
    ap.add_argument("--config", required=True, help="the bot's config.json (its folder is the data folder)")
    ap.add_argument("--days", type=int, default=10, help="complete weekdays to replay (default 10)")
    ap.add_argument("--symbols", default="auto", help="auto (8 liquid pairs), all, or EURUSD,GBPUSD,...")
    ap.add_argument("--upload", action="store_true", help="publish to the reports branch on GitHub")
    ap.add_argument("--synthetic", action="store_true", help=f"made-up ticks: {SYNTHETIC_LABEL}")
    ap.add_argument("--workers", type=int, default=0, help="parallel processes (default: CPUs - 1)")
    ap.add_argument("--variants", default="default", help="default | all | base | METHOD_ID,METHOD_ID")
    ap.add_argument("--no-fetch", action="store_true", help="use the ticks already stored")
    ap.add_argument("--max-gb", type=float, default=3.0, help="size budget of the tick store (GB)")
    ap.add_argument("--chunk-minutes", type=int, default=60, help="minutes of ticks asked for at a time")
    ap.add_argument("--out", default="", help="research directory (default: <data>/rider-research)")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--synthetic-hours", default="7-17", help="synthetic market hours UTC, e.g. 0-24")
    ap.add_argument("--synthetic-tick-scale", type=float, default=0.6)
    ap.add_argument("--no-cache", action="store_true", help="recompute every job")
    ap.add_argument("--probe", action="store_true",
                    help="only check that past ticks can be read: one past hour of EURUSD, its tick count and times")
    a = ap.parse_args(argv)
    if a.probe:
        return run_probe(a)
    from ..config import RiderConfig
    from .pipeline import Plan, default_workers, run
    from .publish import publish
    from .tickstore import TickStore, weekdays_back
    from .variants import choose
    data_dir = Path(a.config).resolve().parent
    root = Path(a.out).resolve() if a.out else data_dir / "rider-research"
    today = dt.datetime.now(UTC).date()
    days = weekdays_back(today - dt.timedelta(days=1), max(1, a.days))
    rider_cfg = RiderConfig.load(RiderConfig.default_path(data_dir))
    workers = a.workers or default_workers()
    try:
        variants = choose(a.variants)
    except ValueError as exc:
        _say(str(exc))
        return 2
    if a.synthetic:
        from . import synthetic
        from .tickstore import save_json
        sroot = root / "SYNTHETIC"
        store = TickStore(sroot / "ticks")
        if a.symbols.lower() in ("auto", "all"):
            symbols = list(synthetic.DEFAULT_SYMBOLS)
        else:
            symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()]
        h0, h1 = (int(x) for x in a.synthetic_hours.split("-"))
        gen_days = weekdays_back(dt.date.fromisoformat(days[0]) - dt.timedelta(days=1), 1) + days
        params = {"days": gen_days, "symbols": symbols, "seed": a.seed, "hours": [h0, h1],
                  "tick_scale": a.synthetic_tick_scale, "v": 1}
        ppath = sroot / "synthetic.json"
        try:
            same = json.loads(ppath.read_text()).get("params") == params
        except Exception:
            same = False
        if same and all(store.complete(s, d) for s in symbols for d in gen_days):
            from .. import synth as _synth
            specs = {s: _synth.make_spec(s) for s in symbols}
            from .pipeline import event_from_dict
            calendar = [event_from_dict(e) for e in json.loads(ppath.read_text()).get("calendar", [])]
            _say(f"{SYNTHETIC_LABEL}: reusing the made-up ticks already generated")
        else:
            _say(f"{SYNTHETIC_LABEL}: generating made-up ticks for {len(symbols)} markets over {len(gen_days)} days...")
            specs, calendar = synthetic.generate(store, gen_days, symbols, seed=a.seed,
                                                 tick_scale=a.synthetic_tick_scale, hours=(h0, h1), progress=_say)
            from .pipeline import event_to_dict
            save_json(ppath, {"params": params, "calendar": [event_to_dict(e) for e in calendar],
                              "label": SYNTHETIC_LABEL})
        folder = f"{today.isoformat()}-SYNTHETIC"
        plan = Plan(root=sroot, out_dir=root / folder, symbols=symbols, days=days, data_source=SYNTHETIC_SOURCE,
                    synthetic=True, rider_cfg=rider_cfg, variants=variants, workers=workers, min_ticks=1000,
                    max_gap_minutes=24 * 60.0,
                    seed=a.seed, use_cache=not a.no_cache)
        fetch_report = None
    else:
        store = TickStore(root / "ticks")
        if a.no_fetch:
            symbols, specs, calendar, ccy = load_stored(root)
            if a.symbols.lower() not in ("auto", "all"):
                symbols = resolve_symbols(symbols, a.symbols)
            elif a.symbols.lower() == "auto":
                symbols = resolve_symbols(symbols, "auto") or symbols
            fetch_report = {"note": "--no-fetch: used the ticks already stored"}
        else:
            symbols, specs, calendar, fetch_report, ccy = fetch_real(a, root, store, days)
        folder = today.isoformat()
        plan = Plan(root=root, out_dir=root / folder, symbols=symbols, days=days, data_source=REAL_SOURCE,
                    synthetic=False, rider_cfg=rider_cfg, variants=variants, workers=workers, seed=a.seed,
                    account_currency=ccy, use_cache=not a.no_cache)
    summary = run(plan, specs, calendar, _say, fetch_report)
    _say("")
    _say("VERDICT: " + summary["verdict"]["text"])
    _say(f"Report: {plan.out_dir / 'report.md'}")
    if a.upload:
        files = {n: (plan.out_dir / n).read_text() for n in ("report.md", "summary.json", "trades.csv")}
        try:
            paths = publish(files, a.config, folder)
            _say(f"Published {len(paths)} files to reports/rider-research/{folder}/ on the reports branch.")
        except Exception as exc:
            _say(f"Could not publish the report: {exc}")
            return 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
