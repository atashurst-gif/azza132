"""What the coach reads each tick, read only (spec sections 4.1, 8 and 14).

* the page's ``/api`` (``report_upload.status_json(cfg)``): the ``standing``
  block - the account's ``today`` (closed since 00:00 UK, every bot, after
  commission), ``open`` (floating), ``equity``, ``balance`` and each bot's
  row - and, once the page has it, ``coach_progress`` (per-day real rows);
* ``data/coach/weather.json`` (the trader's market board and the broker's
  demo-account check);
* ``data/coach/approvals.json`` (Aaron's approve and veto, his CLI only);
* ``data/coach.json`` (Aaron's envelope);
* ``config.json``, ``data/ian.json``: Aaron's own settings, read with
  ``json`` alone (``records.baseline_config``), never through
  ``Config.load``, which opens the secrets file beside it;
* the open real risk of every bot, from each bot's own record, read only.

Nothing here writes anything. The coach never opens the secrets file.
"""
from __future__ import annotations

import datetime as dt
import json
import math
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from . import Paths, as_utc
from . import envelope as envelope_mod
from . import records as R
from . import weather as W

MAX_APPROVALS_BYTES = 256 * 1024


def _fin(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(float(x))


def _read_json(path: Path, limit: int = 2_000_000) -> tuple[Optional[Any], str]:
    try:
        with open(path, "rb") as fh:
            raw = fh.read(limit + 1)
    except FileNotFoundError:
        return None, "missing"
    except OSError as exc:
        return None, f"unreadable ({exc})"
    if len(raw) > limit:
        return None, "too large"
    try:
        return json.loads(raw.decode("utf-8")), ""
    except Exception as exc:
        return None, f"not JSON ({exc})"


def own_mode(path: Path, default: str = "PAPER") -> str:
    """A bot's own settings file's mode (OFF, PAPER or LIVE), as the bot reads it; its default otherwise."""
    obj, _ = _read_json(path, 256 * 1024)
    if isinstance(obj, dict):
        m = str(obj.get("mode") or "").strip().upper()
        if m in ("OFF", "PAPER", "LIVE"):
            return m
    return default


@dataclass
class Page:
    """The page's ``/api``: the standing block and the progress rows, or why they could not be read."""
    ok: bool
    why: str = ""
    standing: dict = field(default_factory=dict)
    progress: Optional[dict] = None

    @property
    def today(self) -> Optional[float]:
        v = self.standing.get("today")
        return float(v) if self.ok and _fin(v) else None

    @property
    def open(self) -> Optional[float]:
        v = self.standing.get("open")
        return float(v) if self.ok and _fin(v) else None

    @property
    def equity(self) -> Optional[float]:
        v = self.standing.get("equity")
        return float(v) if self.ok and _fin(v) else None

    @property
    def real_today(self) -> Optional[float]:
        """Closed today plus floating, every bot, after commission (B1, B2)."""
        if self.today is None or self.open is None:
            return None
        return round(self.today + self.open, 2)


def read_page(cfg: Any, fetch: Optional[Callable[[str], str]] = None) -> Page:
    """``report_upload.status_json(cfg)`` (the page's own data). Never raises."""
    try:
        from ..ops.report_upload import status_json
        body = status_json(cfg, fetch)
        obj = json.loads(body)
    except Exception as exc:
        return Page(False, f"the page could not be read ({exc})")
    if not isinstance(obj, dict) or "unavailable" in obj:
        why = obj.get("unavailable") if isinstance(obj, dict) else "not a table"
        return Page(False, f"the page could not be read ({why})")
    sd = obj.get("standing")
    if not isinstance(sd, dict):
        return Page(False, "the page has no standing block")
    if sd.get("error"):
        return Page(False, f"MetaTrader's figures are incomplete ({sd.get('error')})", sd,
                    obj.get("coach_progress") if isinstance(obj.get("coach_progress"), dict) else None)
    return Page(True, "", sd, obj.get("coach_progress") if isinstance(obj.get("coach_progress"), dict) else None)


def read_approvals(paths: Paths) -> list[dict]:
    """Aaron's approvals and vetoes (``data/coach/approvals.json``; his CLI is the only writer)."""
    obj, _ = _read_json(paths.approvals, MAX_APPROVALS_BYTES)
    if isinstance(obj, dict):
        obj = obj.get("items")
    if not isinstance(obj, list):
        return []
    out = []
    for a in obj:
        if isinstance(a, dict) and a.get("by") == "aaron" and a.get("action") in ("approve", "veto") and a.get("id"):
            out.append({"id": str(a["id"]), "action": a["action"], "plan": str(a.get("plan") or ""),
                        "ts": str(a.get("ts") or "")})
    return out


def open_real_risk(data_dir: "str | Path", cfg: Any) -> tuple[Optional[float], str]:
    """Money at the stops of every open REAL position, every bot, from each bot's own record (read only):
    Trend & Breakout's journal (real by its paper book), the Momentum Runner, Band Breaker, Crowd Fader and
    Financial Ian (``mode`` LIVE). (None, why) when a record that exists cannot be read."""
    data = Path(data_dir)
    total = 0.0
    j = data / "journal.sqlite"
    if j.exists():
        check = R.SourceCheck("journal", str(j))
        paper = R._paper_book(data, cfg, check)                 # read-only (records.py imports PaperBroker)
        if paper is None:
            return None, "Trend & Breakout's paper record could not be read"
        try:
            with closing(R.connect_ro(j)) as con:
                cols = R.table_columns(con, "trades")
                if "risk_money" not in cols:
                    return None, "the journal has no risk_money column"
                for r in con.execute("SELECT ticket, risk_money FROM trades WHERE closed_utc IS NULL OR closed_utc = ''"):
                    if not paper.is_paper(int(r["ticket"] or 0)):
                        total += max(0.0, float(r["risk_money"] or 0.0))
        except Exception as exc:
            return None, f"the journal could not be read ({exc})"
        finally:
            try:
                paper.close_db()
            except Exception:
                pass
    for name, fallback in (("runner.sqlite", R.RUNNER_FALLBACK_RISK_GBP), ("bandbreaker.sqlite", R.BOT_FALLBACK_RISK_GBP),
                           ("crowd.sqlite", R.BOT_FALLBACK_RISK_GBP), ("ian.sqlite", R.BOT_FALLBACK_RISK_GBP)):
        p = data / name
        if not p.exists():
            continue
        try:
            with closing(R.connect_ro(p)) as con:
                cols = R.table_columns(con, "trades")
                if not cols or "mode" not in cols:
                    continue
                risk_col = "risk_money" if "risk_money" in cols else None
                q = (f"SELECT {risk_col or '0'} AS risk FROM trades WHERE (closed_utc IS NULL OR closed_utc = '') "
                     f"AND upper(mode) = 'LIVE'")
                for r in con.execute(q):
                    v = r["risk"]
                    total += max(0.0, float(v)) if risk_col and v is not None else fallback
        except Exception as exc:
            return None, f"{name} could not be read ({exc})"
    return round(total, 2), ""


@dataclass
class Feed:
    """One tick's reading of the world."""
    now: dt.datetime
    cfg: Any
    envelope: envelope_mod.EnvelopeState
    page: Page
    weather: W.Weather
    approvals: list
    ian_mode: str
    open_risk: Optional[float]
    open_risk_why: str = ""

    @property
    def config_stop_pct(self) -> float:
        return float(self.cfg.risk.max_daily_loss_pct)


def read(data_dir: "str | Path", now: dt.datetime, *, cfg: Any = None,
         fetch: Optional[Callable[[str], str]] = None) -> Feed:
    """Everything one tick reads. ``cfg``: Aaron's settings (``records.baseline_config`` when not given:
    config.json read with json alone). Never raises."""
    now = as_utc(now)
    data = Path(data_dir)
    paths = Paths.of(data)
    if cfg is None:
        cfg = R.baseline_config(data)
    env = envelope_mod.read(paths.envelope, float(cfg.risk.max_risk_pct), float(cfg.risk.max_daily_loss_pct))
    page = read_page(cfg, fetch)
    weather = W.read(paths, now)
    approvals = read_approvals(paths)
    ian_mode = own_mode(data / "ian.json")
    try:
        risk, why = open_real_risk(data, cfg)
    except Exception as exc:
        risk, why = None, f"open risk could not be read ({exc})"
    return Feed(now, cfg, env, page, weather, approvals, ian_mode, risk, why)
