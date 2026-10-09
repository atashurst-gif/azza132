"""Test Financial Ian on REAL PAST CME data before paying for the live feed.

Aaron's decision: "I want lower risk with previous data first". Before the live CME feed is bought, Ian
is run over real past CME futures order-book data bought from Databento's historical service, which
is priced per request - and the price is always shown, and agreed, BEFORE anything is bought.

    python -m mintel.ian.research.historical --config <data>/config.json
        [--days 5 | --start YYYY-MM-DD --end YYYY-MM-DD] [--hours london_ny|all] [--schema mbp-10]
        [--no-trades] [--max-cost USD] [--yes] [--max-gb GB] [--offline] [--download-only]
        [--upload] [--set-key]

Defaults: the last 5 complete weekdays, London and New York hours only (07:00-21:00 UTC), the front
month of each of the 7 FX futures (6E 6B 6A 6N 6C 6J 6S), schemas mbp-10 and trades. A cheaper first
look: --days 2.

1. COST FIRST. The dataset's available range is printed and dates outside it are refused. Then
   Databento is ASKED what exactly this request costs (``metadata.get_cost``, per schema and day) and how
   big it is (``metadata.get_billable_size``) - asking is free and downloads nothing. Nothing is bought
   without an explicit "y" typed at the terminal, or ``--yes`` together with a ``--max-cost`` at or above
   the quote; over ``--max-cost`` it stops and says so. Databento's API does not report account credit,
   so none is assumed.
2. DOWNLOAD in day chunks with ``timeseries.get_range`` (``stype_in="raw_symbol"``): each day asks for the
   contract Ian's live code would have traded that day - :func:`mintel.ian.mapping.front_month`, rolling
   8 days before the last trading day - not Databento's continuous symbols, so the replay sees exactly the
   contracts live Ian subscribes to. Each file is saved compressed as
   ``data/ian-research/history/<YYYY-MM-DD>/<schema>.dbn.zst`` with a ``manifest.json`` recording its hours
   and contracts. A file appears under its final name only once it is complete. What is on disk is never
   downloaded, nor paid for, again: a day already there for the hours and contracts asked for is skipped,
   and when MORE is asked for (``--hours all`` after London and New York, or contracts added in ian.json)
   only the missing hours and contracts are quoted and bought, as extra files for that day
   (``<schema>.2.dbn.zst`` ...), which never overlap.
3. BACKTEST with the real IanEngine (:mod:`mintel.ian.research.backtest`).
4. REPORT ``data/ian-research/<date>/report.md``, ``summary.json``, ``trades.csv``; ``--upload`` publishes them
   to the reports branch at ``reports/ian-research/<date>/`` (mintel.ops.report_upload).

The API key is read from secrets.json (``"databento_api_key"``) exactly as the live feed reads it; it is
never printed, logged or written anywhere else. ``--set-key`` asks for it with hidden input.

Exit codes: 0 done; 10 nothing was bought in this run (refused before buying, or the answer was not "y") -
nothing else returns 10, so the launchers say "Nothing was bought." for it alone; 4 stopped (Ctrl+C) after
buying had started; 2 a setup or Databento problem; 3 no data. Python itself exits 1 on a crash.
"""
from __future__ import annotations

import argparse
import datetime as dt
import getpass
import json
import math
import os
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..engine import IanConfig
from ..feeds.databento import DATASET, read_api_key, save_api_key
from ..mapping import CONTRACTS, front_month
from . import backtest as bt

UTC = dt.timezone.utc
HOURS = {"london_ny": (7, 21), "all": (0, 24)}
HOURS_TEXT = {"london_ny": "London and New York hours only (07:00-21:00 UTC)", "all": "the whole day (00:00-24:00 UTC)"}
DEFAULT_DAYS = 5
DEFAULT_MAX_GB = 25.0
DISK_SHARE = 0.6                  # on-disk (compressed) size assumed at most this share of the billable size
TOOL = "mintel.ian.research.historical"

# DECLINED means NOTHING WAS BOUGHT in this run, and nothing else returns it (Python exits 1 on a crash and
# argparse 2 on a bad option): the launchers print "Nothing was bought." for this code only.
OK, SETUP_ERROR, DATA_ERROR, STOPPED, DECLINED = 0, 2, 3, 4, 10


class Refused(RuntimeError):
    """A plain-English reason to stop, with the exit code."""

    def __init__(self, message: str, code: int = DECLINED):
        super().__init__(message)
        self.code = code


@dataclass
class _Run:
    """What this run has done that the exit code must not hide."""
    buying: bool = False                           # a download (a purchase) has been started


# --------------------------------------------------------------- errors --

def explain(exc: BaseException) -> str:
    """What a Databento (or network) failure means, in plain English. Never contains the key."""
    status = getattr(exc, "http_status", None)
    body = getattr(exc, "json_body", None)
    case = msg = ""
    try:
        case = str(body["detail"]["case"])
        msg = str(body["detail"]["message"])
    except Exception:
        msg = str(getattr(exc, "message", "") or "")
    text = f"{case} {msg}".lower()
    said = f" (Databento said: {status} {case or msg})" if status else ""
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None
    if status == 401 or "api_key" in text or "auth_" in text or "invalid api key" in str(exc).lower():
        return ("Databento did not accept the API key: it may be mistyped, revoked or from another account. "
                "Run this again with --set-key to enter it again." + said)
    if status == 402 or any(w in text for w in ("payment", "billing", "credit card", "insufficient", "funds")):
        return ("Databento will not bill this request: the account has no payment method (or not enough credit) "
                "for historical data. Add one in the Databento portal (Billing); nothing was charged." + said)
    if status == 403 or any(w in text for w in ("license", "licence", "entitle", "subscription", "permission",
                                                "not authorized", "forbidden")):
        return ("The Databento account is not allowed this data (GLBX.MDP3 historical): check the dataset's "
                "licence and subscription in the Databento portal; nothing was charged." + said)
    if "dataset" in text and (status in (404, 422) or "not found" in text or "unavailable" in text):
        return ("Databento says the dataset is not available to this account (GLBX.MDP3, CME Globex MDP 3.0)."
                + said)
    if status == 422 and any(w in text for w in ("start", "end", "range", "date", "time")):
        return "Databento refused the dates: they are outside what it has for this dataset." + said
    if status == 422 and "symbol" in text:
        return "Databento could not find one of the contracts asked for." + said
    if status == 429:
        return "Databento says too many requests right now: wait a minute and run this again." + said
    if status is not None and status >= 500:
        return "Databento had a problem on its side: try again later. Nothing on this computer is wrong." + said
    name = type(exc).__name__.lower()
    if isinstance(exc, (ConnectionError, TimeoutError)) or any(w in name for w in ("connection", "timeout", "ssl")):
        return "Databento could not be reached: is the internet connection working?"
    return f"Databento request failed: {type(exc).__name__}{said or (': ' + msg if msg else '')}"


# ------------------------------------------------------------- planning --

def parse_ts(v) -> Optional[dt.datetime]:
    """'2026-10-09T00:00:00.000000000Z', '2026-10-09', a date or a datetime -> an aware UTC datetime."""
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    if isinstance(v, dt.date):
        return dt.datetime(v.year, v.month, v.day, tzinfo=UTC)
    s = str(v).strip().replace("Z", "+00:00")
    if "." in s:                                  # nanoseconds: keep microseconds
        head, rest = s.split(".", 1)
        frac = "".join(ch for ch in rest if ch.isdigit())
        tz = rest[len(frac):]
        s = f"{head}.{frac[:6]}{tz}"
    t = dt.datetime.fromisoformat(s)
    return t if t.tzinfo else t.replace(tzinfo=UTC)


def dataset_range(client, schemas: Sequence[str]) -> tuple[dt.datetime, dt.datetime]:
    """The span Databento has for the dataset (the tightest over the schemas asked for)."""
    raw = client.metadata.get_dataset_range(dataset=DATASET)
    raw = raw if isinstance(raw, dict) else {}
    start = parse_ts(raw.get("start") or raw.get("start_date"))
    end = parse_ts(raw.get("end") or raw.get("end_date"))
    per = raw.get("schema") if isinstance(raw.get("schema"), dict) else {}
    for s in schemas:
        r = per.get(s) if isinstance(per, dict) else None
        if isinstance(r, dict):
            s0, s1 = parse_ts(r.get("start")), parse_ts(r.get("end"))
            start = max(start, s0) if (start and s0) else (start or s0)
            end = min(end, s1) if (end and s1) else (end or s1)
    if start is None or end is None:
        raise Refused("Databento did not say what dates it has for GLBX.MDP3; try again later.", SETUP_ERROR)
    return start, end


def window(day: dt.date, hours: str) -> tuple[dt.datetime, dt.datetime]:
    h0, h1 = HOURS[hours]
    d0 = dt.datetime(day.year, day.month, day.day, tzinfo=UTC)
    return d0 + dt.timedelta(hours=h0), d0 + dt.timedelta(hours=h1)


def last_complete_weekdays(n: int, hours: str, now: dt.datetime, latest: Optional[dt.datetime] = None) -> list[dt.date]:
    """The last ``n`` weekdays whose test window has fully ended (and that Databento already has)."""
    out: list[dt.date] = []
    d = now.date()
    for _ in range(400):
        if len(out) >= n:
            break
        if d.weekday() < 5:
            s, e = window(d, hours)
            if e <= now and (latest is None or e <= latest):
                out.append(d)
        d -= dt.timedelta(days=1)
    return sorted(out)


def weekdays_between(a: dt.date, b: dt.date) -> list[dt.date]:
    out, d = [], a
    while d <= b:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


@dataclass
class Piece:
    """One request to Databento (and one file on disk): a schema, a time range and the contracts."""
    schema: str
    start: dt.datetime
    end: dt.datetime
    symbols: tuple                                 # raw symbols
    cost: float = 0.0                              # Databento's quote, USD
    size: int = 0                                  # Databento's billable size, bytes


@dataclass
class DayPlan:
    day: dt.date
    start: dt.datetime
    end: dt.datetime
    symbols: dict                                  # root -> raw symbol (the front month that day)
    buy: list = field(default_factory=list)        # the Pieces still to buy (not on disk), once quoted


def history_dir(data_dir: Path) -> Path:
    return Path(data_dir) / "ian-research" / "history"


def day_dir(data_dir: Path, day: dt.date) -> Path:
    return history_dir(data_dir) / day.isoformat()


def read_manifest(d: Path) -> dict:
    try:
        m = json.loads((Path(d) / "manifest.json").read_text())
        return m if isinstance(m, dict) else {}
    except Exception:
        return {}


def write_manifest(d: Path, m: dict) -> None:
    p = Path(d) / "manifest.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(m, indent=2, default=str))
    tmp.replace(p)


def _pieces(data_dir: Path, day: dt.date, schema: str) -> Optional[list[tuple]]:
    """What is on disk for ``schema`` that day: (start, end, contracts or None for "all") per complete file.
    None when a file is there without a record of what it holds (copied in by hand?): it is never bought over."""
    d = day_dir(data_dir, day)
    files = [f for f in bt.day_files(d, schema) if f.stat().st_size > 0]
    if not files:
        return []
    rec = read_manifest(d).get("files") or {}
    out = []
    for f in files:
        e = rec.get(f.name)
        if e is None and f.name == f"{schema}.dbn.zst":
            e = rec.get(schema)                    # the first layout: one file a schema, recorded under its name
        if not isinstance(e, dict):
            return None
        s0, s1 = parse_ts(e.get("start")), parse_ts(e.get("end"))
        if s0 is None or s1 is None:
            return None
        syms = e.get("symbols")
        out.append((s0, s1, {str(x) for x in syms} if isinstance(syms, (list, tuple)) else None))
    return out


def _minus(start: dt.datetime, end: dt.datetime, covered: Sequence[tuple]) -> list[tuple]:
    """[start, end) less the covered spans."""
    out, t = [], start
    for a, b in sorted(covered):
        if b <= t:
            continue
        if a >= end:
            break
        if a > t:
            out.append((t, a))
        t = max(t, b)
        if t >= end:
            break
    if t < end:
        out.append((t, end))
    return out


def missing(data_dir: Path, day: dt.date, schema: str, start: dt.datetime, end: dt.datetime,
            symbols) -> list[tuple]:
    """What is NOT on disk yet of ``schema`` for ``symbols`` over [start, end): (start, end, contracts) per
    piece to buy - only the hours and the contracts still missing, never what is already there."""
    pieces = _pieces(data_dir, day, schema)
    if pieces is None:
        return []
    groups: dict[tuple, list[str]] = {}
    for sym in sorted({str(x) for x in symbols}):
        gaps = tuple(_minus(start, end, [(a, b) for a, b, ss in pieces if ss is None or sym in ss]))
        if gaps:
            groups.setdefault(gaps, []).append(sym)
    out = [(a, b, tuple(syms)) for gaps, syms in groups.items() for a, b in gaps]
    return sorted(out, key=lambda x: (x[0], x[2]))


def on_disk(data_dir: Path, day: dt.date, schema: str, start: dt.datetime, end: dt.datetime, symbols) -> bool:
    """Everything asked for is there: complete files (a file only gets its final name once complete) that
    cover the window for EVERY contract in ``symbols``."""
    pieces = _pieces(data_dir, day, schema)
    if pieces is None:
        return True                                # no record of what it holds: never bought twice
    return bool(pieces) and not missing(data_dir, day, schema, start, end, symbols)


def piece_files(data_dir: Path, day: dt.date, schema: str, start: dt.datetime, end: dt.datetime) -> list[Path]:
    """The day's files of ``schema`` that hold some of [start, end) (all of them when one has no record)."""
    d = day_dir(data_dir, day)
    files = [f for f in bt.day_files(d, schema) if f.stat().st_size > 0]
    rec = read_manifest(d).get("files") or {}
    out = []
    for f in files:
        e = rec.get(f.name)
        if e is None and f.name == f"{schema}.dbn.zst":
            e = rec.get(schema)
        s0 = parse_ts(e.get("start")) if isinstance(e, dict) else None
        s1 = parse_ts(e.get("end")) if isinstance(e, dict) else None
        if s0 is None or s1 is None or (s0 < end and s1 > start):
            out.append(f)
    return out


def _hhmm(day: dt.date, t: dt.datetime) -> str:
    d0 = dt.datetime(day.year, day.month, day.day, tzinfo=UTC)
    return "24:00" if t == d0 + dt.timedelta(days=1) else f"{t.astimezone(UTC):%H:%M}"


def describe(day: dt.date, start: dt.datetime, end: dt.datetime, symbols, all_symbols) -> str:
    """'07:00-21:00 UTC' or '00:00-07:00 UTC for 6AZ6 6CZ6' (when not every contract of the day)."""
    span = f"{_hhmm(day, start)}-{_hhmm(day, end)} UTC"
    syms = sorted({str(x) for x in symbols})
    return span if set(syms) >= {str(x) for x in all_symbols} else f"{span} for {' '.join(syms)}"


def fmt_bytes(n: float) -> str:
    n = float(n or 0)
    if n >= 1e9:
        return f"{n / 1e9:.1f} GB"
    if n >= 1e6:
        return f"{n / 1e6:.1f} MB"
    return f"{n / 1e3:.0f} KB"


def _api_time(t: dt.datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")       # Databento reads a time without a zone as UTC


# ---------------------------------------------------------------- client --

def make_client(key: str, factory: Optional[Callable] = None):
    if factory is not None:
        return factory(key=key)
    try:
        import databento as db
    except Exception:
        raise Refused("The databento package is not installed in this Python. Install it with:\n"
                      f"  {sys.executable} -m pip install databento", SETUP_ERROR) from None
    try:
        return db.Historical(key=key)
    except Exception as exc:
        raise Refused(explain(exc), SETUP_ERROR) from None


def quote(client, piece: Piece) -> tuple[float, int]:
    kw = dict(dataset=DATASET, start=_api_time(piece.start), end=_api_time(piece.end),
              symbols=list(piece.symbols), schema=piece.schema, stype_in="raw_symbol")
    cost = float(client.metadata.get_cost(**kw))
    size = float(client.metadata.get_billable_size(**kw))
    if not (math.isfinite(cost) and cost >= 0 and math.isfinite(size) and size >= 0):
        raise Refused(f"Databento's quote could not be read (cost {cost!r}, size {size!r}), so nothing was "
                      "bought. Try again later.", SETUP_ERROR)
    return cost, int(size)


class _Watch(threading.Thread):
    """Shows a download growing on disk while ``get_range`` streams it."""

    def __init__(self, path: Path, say: Callable[[str], None], every: float = 10.0):
        super().__init__(daemon=True)
        self.path, self.say, self.every = path, say, every
        self.halt = threading.Event()

    def run(self) -> None:
        while not self.halt.wait(self.every):
            try:
                self.say(f"      ... {fmt_bytes(self.path.stat().st_size)} received so far")
            except OSError:
                pass

    def stop(self) -> None:
        self.halt.set()
        self.join(timeout=2)


def _free_name(d: Path, schema: str) -> Path:
    """``<schema>.dbn.zst`` for a day's first file of a schema, then ``<schema>.2.dbn.zst`` ..."""
    final = d / f"{schema}.dbn.zst"
    n = 2
    while final.exists():
        final = d / f"{schema}.{n}.dbn.zst"
        n += 1
    return final


def download(client, data_dir: Path, plan: DayPlan, piece: Piece, hours: str, say: Callable[[str], None],
             watch_every: float = 10.0) -> Path:
    """One piece (a schema, its hours and contracts of one day): streamed to ``<file>.part`` and renamed only
    once complete. A day's later pieces go into new files beside the first; nothing on disk is replaced."""
    schema = piece.schema
    d = day_dir(data_dir, plan.day)
    d.mkdir(parents=True, exist_ok=True)
    final = _free_name(d, schema)
    part = final.with_name(final.name + ".part")
    if part.exists():
        part.unlink()                              # left by an interrupted run: it was never complete
    w = _Watch(part, say, watch_every)
    w.start()
    try:
        store = client.timeseries.get_range(dataset=DATASET, start=_api_time(piece.start), end=_api_time(piece.end),
                                            symbols=list(piece.symbols), schema=schema,
                                            stype_in="raw_symbol", path=str(part))
    except BaseException:
        w.stop()
        try:
            part.unlink()
        except OSError:
            pass
        raise
    w.stop()
    if not part.exists() or part.stat().st_size == 0:
        raise Refused(f"Databento sent nothing for {plan.day} {schema}.", DATA_ERROR)
    md = getattr(store, "metadata", None)
    not_found = [str(s) for s in (getattr(md, "not_found", None) or [])]
    try:
        import databento
        version = getattr(databento, "__version__", "")
    except Exception:
        version = ""
    m = read_manifest(d)
    # a test client that serves fixture files says so: its data is never recorded as real
    fixture = bool(getattr(client, "fixture", False))
    m.update({"day": plan.day.isoformat(), "dataset": DATASET, "source": "fixture" if fixture else "databento",
              "synthetic": fixture or bool(m.get("synthetic", False)), "tool": TOOL, "stype_in": "raw_symbol",
              "contracts": sorted({str(x) for x in (m.get("contracts") or [])} | set(piece.symbols))})
    m.pop("symbols", None)
    m.setdefault("files", {})[final.name] = {
        "schema": schema, "file": final.name, "bytes": part.stat().st_size, "start": piece.start.isoformat(),
        "end": piece.end.isoformat(), "hours": hours, "symbols": list(piece.symbols), "quoted_cost_usd": piece.cost,
        "quoted_billable_bytes": piece.size, "downloaded_utc": dt.datetime.now(UTC).isoformat(timespec="seconds"),
        "not_found": not_found, "databento_package": version}
    write_manifest(d, m)                           # before the rename: a final file always has its record
    os.replace(part, final)
    if not_found:
        say(f"      Databento did not find: {', '.join(not_found)} on {plan.day} (that future is missing that day)")
    return final


# ------------------------------------------------------------- the flow --

def _say(*a) -> None:
    print(*a, flush=True)


def _scrubbed(say: Callable[..., None], secret: str) -> Callable[..., None]:
    """Whatever is printed, the API key never is - not even inside a message from Databento's server."""
    def out(*a) -> None:
        say(*(str(x).replace(secret, "<API key hidden>") for x in a))
    return out


def _set_key(data_dir: Path, say, getpass_fn=getpass.getpass) -> int:
    try:
        key = getpass_fn("Databento API key (starts with db-; nothing is shown as you type): ").strip()
    except (EOFError, KeyboardInterrupt):
        key = ""
    if not key:
        say("Nothing saved.")
        return DECLINED
    if not key.startswith("db-"):
        say("That does not look like a Databento key (they start with db-). Saved anyway.")
    p = save_api_key(data_dir, key)
    say(f"Key saved in {p} (only you can read it; it is never printed or put in the code).")
    say("Saving it does NOT switch Financial Ian's live feed on: that stays as it is in ian.json.")
    return OK


def _confirm(total: float, a, ask, isatty, say) -> bool:
    # written so that anything not a plain number (NaN) FAILS: no limit is ever skipped by a comparison
    if a.max_cost is not None and not (total <= a.max_cost + 1e-9):
        raise Refused(f"STOPPED: Databento's quote, ${total:,.2f}, is over your limit (--max-cost ${a.max_cost:,.2f}). "
                      "Nothing was downloaded and nothing was charged.", DECLINED)
    if a.yes:
        say(f"--yes with --max-cost ${a.max_cost:,.2f}: the quote (${total:,.2f}) is within your limit, so going ahead.")
        return True
    if not isatty():
        raise Refused("There is no terminal here to ask you, so nothing was bought. Run it in a terminal, or add "
                      "--yes together with --max-cost <the most you allow, in dollars>.", DECLINED)
    try:
        ans = ask("Go ahead? (y/N) ")
    except (EOFError, KeyboardInterrupt):
        ans = ""
    if str(ans).strip().lower() in ("y", "yes"):
        return True
    say("Not bought: nothing was downloaded and nothing was charged.")
    return False


def _days_with_files(data_dir: Path, schema: str) -> list[dt.date]:
    """Every day with at least one downloaded file of ``schema``, oldest first."""
    out = []
    root = history_dir(data_dir)
    if not root.exists():
        return out
    for d in sorted(root.iterdir()):
        try:
            day = dt.date.fromisoformat(d.name)
        except ValueError:
            continue
        if any(f.stat().st_size > 0 for f in bt.day_files(d, schema)):
            out.append(day)
    return out


def _lacks(data_dir: Path, plan: DayPlan, schema: str) -> str:
    """What the files on disk lack of ``schema`` for this plan, in words ('' when nothing)."""
    gaps = missing(data_dir, plan.day, schema, plan.start, plan.end, plan.symbols.values())
    return "; ".join(f"{schema} {describe(plan.day, a, b, syms, plan.symbols.values())}" for a, b, syms in gaps)


def _calendar(config: Path, data_dir: Path, start: dt.datetime, end: dt.datetime):
    cands = []
    try:
        from ...config import Config
        cfg = Config.load(str(config)) if config.exists() else None
        if cfg is not None:
            # as the trader finds it: an absolute store path, else the name inside the data folder (only an
            # absolute data folder is trusted here - a relative one would depend on where this was started)
            sp = Path(cfg.news.store_path)
            if sp.is_absolute():
                cands.append(sp)
            elif Path(cfg.ops.data_dir).is_absolute():
                cands.append(Path(cfg.ops.data_dir) / sp.name)
    except Exception:
        pass
    cands.append(Path(data_dir) / "calendar.sqlite")
    path = next((c for c in cands if c.exists()), cands[-1])
    return bt.load_calendar(path, start, end)


def _costs(a) -> bt.CostModel:
    spreads = dict(bt.DEFAULT_SPREAD_PIPS)
    for item in a.spread or []:
        try:
            pair, pips = item.split("=", 1)
            spreads[pair.strip().upper()] = float(pips)
        except ValueError:
            raise Refused(f"--spread {item!r}: write it as PAIR=PIPS, for example EURUSD=0.2", DECLINED) from None
    return bt.CostModel(latency_ms=a.latency_ms, slippage_pips=a.slippage_pips,
                        commission_usd_per_lot_side=a.commission_usd, spread_pips=spreads)


def run_test(data_dir: Path, config: Path, plans: Sequence[DayPlan], schemas: Sequence[str], hours: str,
             costs: bt.CostModel, ian_cfg: IanConfig, run_date: str, say, progress_every_s: float = 60.0) -> tuple[dict, dict]:
    """Replay what is on disk for ``plans`` through Ian; write the report. Returns (summary, paths)."""
    days = [p for p in plans if on_disk(data_dir, p.day, "mbp-10", p.start, p.end, p.symbols.values())]
    if not days:
        raise Refused("No downloaded data on disk for these days (every contract, the whole window): nothing to "
                      "test.", DATA_ERROR)
    manifests = {p.day: read_manifest(day_dir(data_dir, p.day)) for p in days}
    real = all(m.get("source") == "databento" and m.get("synthetic") is False for m in manifests.values())
    stats: list[bt.DayStats] = []
    use_trades = "trades" in schemas
    # the trades file is used only for a day it covers completely; otherwise the trades inside mbp-10 are
    trades_days = {p.day for p in days
                   if use_trades and on_disk(data_dir, p.day, "trades", p.start, p.end, p.symbols.values())}

    def batches():
        for p in days:
            st = bt.DayStats(p.day.isoformat())
            stats.append(st)
            here = p.day in trades_days
            files = {"mbp-10": piece_files(data_dir, p.day, "mbp-10", p.start, p.end),
                     "trades": piece_files(data_dir, p.day, "trades", p.start, p.end) if here else []}
            yield from bt.dbn_day_batches(day_dir(data_dir, p.day), p.start, p.end, st, use_trades=here,
                                          files=files, symbols=p.symbols.values())

    windows = [bt.Window(p.start, p.end, p.day.isoformat(), dict(p.symbols)) for p in days]
    cal, cal_info = _calendar(config, data_dir, days[0].start, days[-1].end)
    out_dir = Path(data_dir) / "ian-research" / run_date
    t_first, t_last = days[0].day, days[-1].day
    say("")
    say(f"Replaying {len(days)} day(s) through Financial Ian's own engine (no MetaTrader, no orders).")
    say("  Ian decides every half second for 7 futures, so this is slow: roughly 30 to 60 minutes for each day of "
        "London and New York data. Progress and the time left are shown as it goes; leave this window open. "
        "It runs at low priority, so the bots come first.")
    started = time.monotonic()

    def progress(p: dict) -> None:
        eta = p.get("eta_s")
        say(f"  {p['time']:%a %d %b %H:%M} UTC  {p['fraction']:.0%} done, {p['events']:,} events, "
            f"{p['trades']} trades so far" + (f", about {eta / 60:.0f} min to go" if eta else ""))

    res = bt.run_backtest(batches(), windows, ian_cfg, costs, out_dir / "engine", synthetic=not real,
                          sequence_scope="channel", calendar=cal, progress=progress, progress_every_s=progress_every_s)
    records = sum(s.mbp_records + s.trade_records for s in stats)
    extra = []
    if trades_days:
        dup = sum(s.mbp_trade_records for s in stats)
        trd = sum(s.trade_records for s in stats)
        extra.append(f"Trades came from the trades file ({trd:,} records); the {dup:,} trade records the mbp-10 file "
                     "also carries were not counted a second time (live Ian, subscribed to both, does the same).")
    no_trd = [p.day.isoformat() for p in days if p.day not in trades_days]
    if use_trades and no_trd:
        extra.append(f"On {', '.join(no_trd)} the trades file was not complete, so the trades inside mbp-10 were used.")
    empty = [s.day for s in stats if s.events == 0]
    if empty:
        extra.append(f"No data at all on {', '.join(empty)} (an exchange holiday?).")
    missing = sorted({n for s in stats for n in s.not_found})
    if missing:
        extra.append(f"Databento did not find {', '.join(missing)} on some days: those futures were missing then.")
    if not real:
        extra.append("Not every file on disk is recorded as a real Databento download by this tool (a test fixture, or "
                     "files copied in by hand), so the data's source cannot be confirmed: the whole run is labelled "
                     "SYNTHETIC - NOT PERFORMANCE.")
    meta = {"generated_utc": dt.datetime.now(UTC).isoformat(timespec="seconds"),
            "period": {"first": t_first.isoformat(), "last": t_last.isoformat()},
            "hours": hours, "hours_text": HOURS_TEXT[hours], "schemas": list(schemas), "records": records,
            "day_stats": [s.to_dict() for s in stats], "calendar": cal_info, "extra_caveats": extra,
            "data_files": str(history_dir(data_dir)),
            "downloads": {p.day.isoformat(): manifests[p.day].get("files") for p in days}}
    if real:
        meta["source"] = (f"Databento {DATASET} (CME Globex MDP 3.0), schemas {' + '.join(schemas)}, "
                          f"{t_first.isoformat()} to {t_last.isoformat()}, downloaded by this tool")
    summary = bt.summarise(res, meta)
    paths = bt.write_report(out_dir, summary, res.trades)
    say(f"  replay finished in {(time.monotonic() - started) / 60:.1f} min")
    return summary, paths


def upload_report(config: Path, run_date: str, paths: dict, say, uploader=None) -> bool:
    from ...ops import report_upload as ru
    token = ru.read_token(str(config))
    if not token:
        say("Not uploaded: no GitHub token saved. Run: python -m mintel.ops.report_upload "
            f"--config {config} --set-token")
        return False
    try:
        from ...config import Config
        cfg = Config.load(str(config)) if Path(config).exists() else Config()
        repo, branch = cfg.report_repo, cfg.report_branch
    except Exception:
        from ...config import Config
        repo, branch = Config().report_repo, Config().report_branch
    files = {f"reports/ian-research/{run_date}/{name}": Path(p).read_text(encoding="utf-8") for name, p in paths.items()}
    try:
        (uploader or ru.upload)(files, repo, branch, token, day=run_date)
    except Exception as exc:
        say(f"UPLOAD FAILED ({exc}); the report is still on this computer.")
        return False
    say(f"Uploaded to {repo}, branch {branch}: reports/ian-research/{run_date}/ (report.md, summary.json, trades.csv)")
    return True


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog=f"python -m {TOOL}",
                                 description="Test Financial Ian on real past CME order-book data from Databento. "
                                             "The cost is always shown, and agreed, before anything is bought.")
    ap.add_argument("--config", required=True, help="the bot's config.json (its folder holds secrets.json)")
    ap.add_argument("--days", type=int, default=None, help=f"the last N complete weekdays (default {DEFAULT_DAYS}); "
                                                           "--days 2 is a cheaper first look")
    ap.add_argument("--start", default="", help="first day, YYYY-MM-DD (with --end)")
    ap.add_argument("--end", default="", help="last day, YYYY-MM-DD (inclusive)")
    ap.add_argument("--hours", choices=tuple(HOURS), default="london_ny",
                    help="london_ny = 07:00-21:00 UTC (default); all = the whole day")
    ap.add_argument("--schema", choices=("mbp-10",), default="mbp-10", help="the order-book schema (mbp-10)")
    ap.add_argument("--no-trades", action="store_true",
                    help="do not buy the trades schema (the trades inside mbp-10 are used instead)")
    ap.add_argument("--max-cost", type=float, default=None, help="the most you allow Databento to charge, in USD")
    ap.add_argument("--yes", action="store_true", help="buy without asking - only together with --max-cost")
    ap.add_argument("--max-gb", type=float, default=DEFAULT_MAX_GB, help="size budget, GB (Databento's billable size)")
    ap.add_argument("--offline", action="store_true", help="never contact Databento: test what is already on disk")
    ap.add_argument("--download-only", action="store_true", help="download (after the cost question), do not test")
    ap.add_argument("--upload", action="store_true", help="publish the report to the reports branch")
    ap.add_argument("--set-key", action="store_true", help="enter the Databento API key (hidden) and save it")
    ap.add_argument("--latency-ms", type=float, default=250.0, help="decision to fill (default 250)")
    ap.add_argument("--slippage-pips", type=float, default=0.1, help="per market or stop fill (default 0.1)")
    ap.add_argument("--commission-usd", type=float, default=3.50, help="per lot per side (default 3.50)")
    ap.add_argument("--spread", action="append", default=[], metavar="PAIR=PIPS",
                    help="override a pair's typical spot spread, e.g. EURUSD=0.2 (repeatable)")
    return ap


def main(argv=None, *, client_factory: Optional[Callable] = None, ask: Callable[[str], str] = input,
         isatty: Optional[Callable[[], bool]] = None, now: Optional[dt.datetime] = None,
         say: Callable[..., None] = _say, getpass_fn=getpass.getpass, uploader=None,
         progress_every_s: float = 60.0) -> int:
    a = build_parser().parse_args(argv)
    isatty = isatty or (lambda: bool(getattr(sys.stdin, "isatty", lambda: False)()))
    config = Path(a.config).expanduser()
    data_dir = config.parent
    if a.set_key:
        return _set_key(data_dir, say, getpass_fn)
    secret = read_api_key(data_dir)
    if secret:
        say = _scrubbed(say, secret)
    run = _Run()
    try:
        return _main(a, config, data_dir, client_factory, ask, isatty, now, say, uploader, progress_every_s, run)
    except Refused as exc:
        if str(exc):
            say("")
            say(str(exc))
        # DECLINED promises that nothing was bought in this run: never once a download has been started
        return STOPPED if (exc.code == DECLINED and run.buying) else exc.code
    except KeyboardInterrupt:
        say("")
        if run.buying:
            say("Stopped. What was already downloaded was paid for and stays on disk; run it again to carry on "
                "(nothing is bought twice).")
            return STOPPED
        say("Stopped before anything was bought in this run: nothing was downloaded or charged.")
        return DECLINED


def _main(a, config: Path, data_dir: Path, client_factory, ask, isatty, now, say, uploader, progress_every_s,
          run: Optional[_Run] = None) -> int:
    run = run if run is not None else _Run()
    # a limit that is not a plain number (nan, inf) would let every comparison through: refused outright
    if a.max_cost is not None and not math.isfinite(a.max_cost):
        raise Refused("--max-cost must be a number of US dollars, for example --max-cost 20. Nothing was asked of "
                      "Databento.")
    if not (math.isfinite(a.max_gb) and a.max_gb > 0):
        raise Refused("--max-gb must be a number of gigabytes above 0, for example --max-gb 25. Nothing was asked of "
                      "Databento.")
    if a.yes and a.max_cost is None:
        raise Refused("--yes needs --max-cost (the most you allow Databento to charge, in US dollars), so nothing is "
                      "ever bought without a limit you set. Nothing was asked of Databento.")
    if a.max_cost is not None and a.max_cost < 0:
        raise Refused("--max-cost cannot be negative.")
    if a.offline and a.download_only:
        raise Refused("--offline never contacts Databento, so --download-only would do nothing.")
    if (a.start or a.end) and a.days is not None:
        raise Refused("Use either --days or --start/--end, not both.")
    if bool(a.start) != bool(a.end):
        raise Refused("--start and --end go together (both YYYY-MM-DD).")
    if a.days is not None and not (1 <= a.days <= 60):
        raise Refused("--days must be between 1 and 60.")
    explicit: Optional[list[dt.date]] = None
    if a.start:
        try:
            d0, d1 = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
        except ValueError:
            raise Refused("--start and --end must be dates written YYYY-MM-DD.") from None
        if d1 < d0:
            raise Refused("--end is before --start.")
        explicit = weekdays_between(d0, d1)
        if not explicit:
            raise Refused("There are no weekdays between --start and --end (CME FX futures do not trade at weekends).")
    n_days = a.days or DEFAULT_DAYS
    now = now or dt.datetime.now(UTC)
    schemas = (a.schema,) if a.no_trades else (a.schema, "trades")
    ian_cfg = IanConfig.load(IanConfig.default_path(data_dir))
    roots = [r for r in ian_cfg.instruments if r in CONTRACTS] or list(CONTRACTS)
    costs = _costs(a)
    say("")
    say("FINANCIAL IAN - TEST ON PAST CME DATA (lower risk first: real past data before any live feed)")

    def plan_for(day: dt.date) -> DayPlan:
        s, e = window(day, a.hours)
        return DayPlan(day, s, e, {r: front_month(r, day, ian_cfg.roll_days).raw_symbol for r in roots})

    def complete(p: DayPlan) -> bool:
        return on_disk(data_dir, p.day, a.schema, p.start, p.end, p.symbols.values())

    if a.offline:
        if explicit is not None:
            plans = [plan_for(d) for d in explicit]
        else:
            plans = [plan_for(d) for d in _days_with_files(data_dir, a.schema)]
        have = [p for p in plans if complete(p)]
        if explicit is None:
            have = have[-n_days:]
        for p in plans:
            lacks = "" if complete(p) else _lacks(data_dir, p, a.schema)
            if lacks and (explicit is not None or not have or p.day > have[0].day):
                say(f"Not tested: {p.day} is on disk without {lacks}. Run without --offline to buy only that "
                    "(the cost is shown first).")
        if not have:
            raise Refused("--offline: none of those days is on disk (every contract, the whole window) yet. Run "
                          "without --offline to download them (the cost is shown first).", DATA_ERROR)
        say(f"Offline: testing {len(have)} day(s) already on disk; Databento is not contacted.")
        plans = have
    else:
        key = read_api_key(data_dir)
        if not key:
            raise Refused("No Databento API key is saved yet. Run this with --set-key first (the key is asked for "
                          "with hidden input and kept in secrets.json).", SETUP_ERROR)
        client = make_client(key, client_factory)
        try:
            first, last = dataset_range(client, schemas)
        except Refused:
            raise
        except Exception as exc:
            raise Refused(explain(exc), SETUP_ERROR) from None
        say(f"Databento has CME Globex ({DATASET}) data from {first:%d %b %Y} to {last:%d %b %Y %H:%M} UTC.")
        if explicit is not None:
            outside = [d for d in explicit if not (first <= window(d, a.hours)[0] and window(d, a.hours)[1] <= last)]
            if outside:
                raise Refused(f"Refused: {', '.join(d.isoformat() for d in outside)} "
                              f"{'is' if len(outside) == 1 else 'are'} outside what Databento has "
                              f"({first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC). Nothing was asked for or bought.")
            days = explicit
        else:
            days = last_complete_weekdays(n_days, a.hours, now, last)
            days = [d for d in days if window(d, a.hours)[0] >= first]
            if not days:
                raise Refused("Databento has no complete weekday in its range yet.", DATA_ERROR)
        plans = [plan_for(d) for d in days]
        say(f"Test period: {len(plans)} weekday(s), {plans[0].day:%a %d %b} to {plans[-1].day:%a %d %b %Y}, "
            f"{HOURS_TEXT[a.hours]}.")
        contracts = sorted({s for p in plans for s in p.symbols.values()})
        say(f"Contracts: {' '.join(contracts)} (the front month each day by Ian's own roll rule: 8 days before the "
            "last trading day)")
        say(f"Schemas: {' + '.join(schemas)}")
        # only what is NOT on disk is quoted and bought: the missing hours and the missing contracts
        todo = []
        for p in plans:
            for s in schemas:
                for t0, t1, syms in missing(data_dir, p.day, s, p.start, p.end, p.symbols.values()):
                    pc = Piece(s, t0, t1, syms)
                    p.buy.append(pc)
                    todo.append((p, pc))
        done_days = [p.day for p in plans if not p.buy]
        if done_days:
            say(f"Already on disk (not downloaded or paid for again): {', '.join(d.isoformat() for d in done_days)}")
        for p in plans:
            if p.buy and any(_pieces(data_dir, p.day, s) for s in schemas):
                say(f"Part of {p.day} is already on disk (not bought again); only what is missing is bought: "
                    + "; ".join(f"{pc.schema} {describe(p.day, pc.start, pc.end, pc.symbols, p.symbols.values())}"
                                for pc in p.buy))
        if todo:
            _buy(client, data_dir, plans, todo, schemas, a, ask, isatty, say, run, explicit is not None)
        else:
            say("Every day is already on disk: nothing to download, nothing to pay.")
    if a.download_only:
        say("Downloaded. Run again without --download-only (or with --offline) to test Ian on it.")
        return OK
    run_date = (now or dt.datetime.now(UTC)).date().isoformat()
    summary, paths = run_test(data_dir, config, plans, schemas, a.hours, costs, ian_cfg, run_date, say,
                              progress_every_s)
    st = summary["stats"]
    say("")
    say("=" * 70)
    say(f"  VERDICT: {summary['verdict']}")
    for r in summary["verdict_reasons"]:
        say(f"    - {r}")
    say(f"  {st['trades']} trades, net {bt._num(st['net_pips'])} pips / {bt._money(st['net_gbp'])} after costs "
        f"({summary['label']})")
    say(f"  {summary['out_of_sample']}.")
    say("=" * 70)
    say(f"Report: {paths['report.md']}")
    say(f"        {paths['summary.json'].name}, {paths['trades.csv'].name} in the same folder")
    if a.upload:
        upload_report(config, run_date, paths, say, uploader)
    return OK


def _buy(client, data_dir: Path, plans: Sequence[DayPlan], todo, schemas, a, ask, isatty, say,
         run: Optional[_Run] = None, explicit_days: bool = False) -> None:
    run = run if run is not None else _Run()
    say("")
    say("Asking Databento what this costs (asking is free: nothing is downloaded or charged yet) ...")
    total_cost, total_bytes = 0.0, 0
    per_day: dict[dt.date, float] = {}
    for p, pc in todo:
        try:
            pc.cost, pc.size = quote(client, pc)
        except Refused:
            raise
        except Exception as exc:
            raise Refused(explain(exc), SETUP_ERROR) from None
        total_cost += pc.cost
        total_bytes += pc.size
        per_day[p.day] = per_day.get(p.day, 0.0) + pc.cost
    for p in plans:
        if not p.buy:
            continue
        cells = []
        for s in schemas:
            mine = [pc for pc in p.buy if pc.schema == s]
            if mine:
                cells.append(f"{s:7s} {fmt_bytes(sum(pc.size for pc in mine)):>9s}  ${sum(pc.cost for pc in mine):,.2f}")
        say(f"  {p.day:%a %d %b %Y}   {'   '.join(cells)}")
    n_buy = len(per_day)
    already = len(plans) - n_buy
    say("")
    say(f"This test will download about {fmt_bytes(total_bytes)} of CME order-book data for {n_buy} "
        f"day{'s' if n_buy != 1 else ''} and Databento will charge about ${total_cost:,.2f}."
        + (f" ({already} more day{'s are' if already != 1 else ' is'} already on disk and cost nothing.)" if already else ""))
    say("  (That is Databento's own quote for exactly this request, including any flat-rate plan discount. Its API "
        "does not report account credit, so none is assumed; any free credit is taken off on Databento's bill - "
        "see the Billing page in the Databento portal.)")
    say(f"  On disk it is stored compressed (smaller than {fmt_bytes(total_bytes)}).")
    if len(plans) > 2:
        # exactly the days the suggestion would buy - the last 2 of this period - priced as they would be
        # bought now (nothing for what is on disk); with --start/--end, --days would pick other days
        last2 = [p.day for p in plans[-2:]]
        cheap = sum(per_day.get(d, 0.0) for d in last2)
        how = f"--start {last2[0].isoformat()} --end {last2[1].isoformat()}" if explicit_days else "--days 2"
        if cheap < total_cost - 0.005:
            say(f"  A cheaper first look: {how} (the last 2 of these days) would cost "
                + ("nothing - they are already on disk." if cheap < 0.005 else f"about ${cheap:,.2f}."))
    if not (total_bytes <= a.max_gb * 1e9):
        raise Refused(f"STOPPED: that is {fmt_bytes(total_bytes)}, over the size budget of {a.max_gb:g} GB (--max-gb). "
                      "Try fewer days. Nothing was downloaded and nothing was charged.")
    try:
        free = shutil.disk_usage(history_dir(data_dir).parent if history_dir(data_dir).parent.exists()
                                 else data_dir).free
    except OSError:
        free = None
    if free is not None and free < DISK_SHARE * total_bytes + 1e9:
        raise Refused(f"STOPPED: only {fmt_bytes(free)} free on this disk; about "
                      f"{fmt_bytes(DISK_SHARE * total_bytes + 1e9)} is needed to be safe. Nothing was downloaded "
                      "and nothing was charged.")
    if not _confirm(total_cost, a, ask, isatty, say):
        raise Refused("", DECLINED)
    say("")
    for i, (p, pc) in enumerate(todo, 1):
        part = "" if describe(p.day, pc.start, pc.end, pc.symbols, p.symbols.values()) == \
            describe(p.day, p.start, p.end, p.symbols.values(), p.symbols.values()) else \
            f" ({describe(p.day, pc.start, pc.end, pc.symbols, p.symbols.values())})"
        say(f"[{i}/{len(todo)}] {p.day:%a %d %b %Y} {pc.schema}{part}: downloading about {fmt_bytes(pc.size)} "
            f"(${pc.cost:,.2f}) ...")
        t0 = time.monotonic()
        run.buying = True                          # from here on, money may have been spent
        try:
            f = download(client, data_dir, p, pc, a.hours, say)
        except Refused:
            raise
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            raise Refused(explain(exc) + "\nWhat was already downloaded was paid for, stays on disk and is not bought "
                          "again; run it again to carry on (the cost of what is left is shown first).",
                          SETUP_ERROR) from None
        say(f"      done: {fmt_bytes(f.stat().st_size)} on disk in {time.monotonic() - t0:.0f} s")


if __name__ == "__main__":
    sys.exit(main())
