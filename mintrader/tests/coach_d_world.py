"""Builder D's test helpers (the coach's decisions and run loop).

EVERY RECORD MADE HERE IS MADE UP for the tests - synthetic trades, synthetic
money, synthetic pages. None of it is a result, and none of it is ever shown
as one. The one real record the coach's tests use is the reports-branch copy
in tests/fixtures/coach_reports_trades.csv (test_coach_replay.py).
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import sqlite3
from pathlib import Path
from typing import Optional, Sequence

from mintel.coach import (BASE_STEP, F1_KEY, STATE_LIVE, STATE_LIVE_TRIAL, STATE_PINNED, STATE_PRACTICE, UTC, Envelope,
                          Paths)
from mintel.coach import decide as D
from mintel.coach import estimate as ES
from mintel.coach import policy as P
from mintel.coach import records as R
from mintel.coach.brakes import BrakeState
from mintel.coach.ledger import Segment
from mintel.config import Config

NOW = dt.datetime(2026, 11, 10, 10, 0, tzinfo=UTC)           # a Tuesday, 10:00 UK (GMT), past every epoch
EQ = 1865.61
GOOD_KEY = "tnb|SESSION_EXPANSION|INDEX|TREND"                # a made-up winner (no pin, no Runner, not C2)
OTHER_KEY = "tnb|BREAKOUT_ACCEPTANCE|GOLD|HIGH_VOL"
MINOR_KEY = "tnb|TREND_PULLBACK|FX_MINOR|TREND"               # C2: never granted while Formula 1 is pinned
SYMBOL = {"INDEX": "US500", "FX_MINOR": "EURGBP", "FX_MAJOR": "EURUSD", "GOLD": "XAUUSD", "CRYPTO": "BTCUSD"}
SYMBOLS = {"INDEX": ("US500", "JP225", "UK100", "DE40", "AUS200"), "FX_MINOR": ("EURGBP", "AUDNZD", "EURCHF"),
           "GOLD": ("XAUUSD", "XAUEUR"), "FX_MAJOR": ("EURUSD", "GBPUSD"), "CRYPTO": ("BTCUSD",)}

_tickets = itertools.count(1_990_000_001)


def trade(key: str, x: float, closed: dt.datetime, *, real: bool = True, risk: float = 10.0, symbol: str = "",
          side: str = "BUY", minutes: int = 20, commission: float = 0.6, score: float = 80.0, rr: float = 2.5,
          ticket: Optional[int] = None, coach_route: str = "") -> R.Trade:
    """One made-up closed trade of ``key`` with outcome ``x`` R after costs."""
    bot, approach, kind, cond = key.split("|")
    source = "journal" if bot == "tnb" else bot
    sym = symbol or SYMBOL.get(kind, "US500")
    return R.Trade(source, bot, ticket or next(_tickets), approach, kind, cond, sym, side,
                   closed - dt.timedelta(minutes=minutes), closed, real, round(x * risk, 2), commission, risk, score, rr,
                   coach_route=coach_route)


def trading_days(end: dt.date, n: int) -> list[dt.date]:
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return sorted(out)


def series(key: str, pattern: Sequence[float], days: Sequence[dt.date], *, real: bool = True, risk: float = 10.0,
           first_hour: int = 1, gap_minutes: int = 0, symbol: str = "", **kw) -> list:
    """``pattern`` repeated on every day, trades spread through the day (two hours apart when they fit, so each
    is its own bet), on the kind's symbols in turn (or ``symbol``)."""
    kind = key.split("|")[2]
    syms = (symbol,) if symbol else SYMBOLS.get(kind, (SYMBOL.get(kind, "US500"),))
    gap = gap_minutes or min(120, (21 * 60) // max(1, len(pattern)))
    out = []
    for d in days:
        start = dt.datetime(d.year, d.month, d.day, first_hour, 30, tzinfo=UTC)
        for i, x in enumerate(pattern):
            closed = start + dt.timedelta(minutes=gap * i)
            out.append(trade(key, x, closed, real=real, risk=risk, symbol=syms[i % len(syms)], **kw))
    return out


def clustered(trades) -> list:
    return R.assign_clusters(trades)


def cfg() -> Config:
    """Aaron's settings as on the Mac since 10 Oct: Trend & Breakout on PAPER with Formula 1 (momentum
    continuation on minor pairs) real."""
    c = Config()
    c.tnb.mode = "PAPER"
    c.tnb.live_groups = ("FX_MINOR",)
    c.tnb.live_tactics = ("MOMENTUM_CONTINUATION",)
    c.tnb.live_since_utc = "2026-10-12T07:58:00+00:00"
    return c


def env(**kw) -> Envelope:
    kw.setdefault("mode", "PILOT")
    return Envelope(**kw)


def seg(key: str, state: str = STATE_PRACTICE, step: Optional[str] = None, *, registered: dt.datetime = None,
        since: dt.datetime = None, live_since: dt.datetime = None, **kw) -> Segment:
    registered = registered or NOW - dt.timedelta(days=60)
    return Segment(key, registered_utc=registered.isoformat(), state=state, step=step,
                   since_utc=(since or registered).isoformat(),
                   live_since_utc=(live_since.isoformat() if live_since else
                                   ((since or registered).isoformat() if state in (STATE_LIVE, STATE_LIVE_TRIAL) else "")),
                   **kw)


def world(trades, segs: dict, *, now: dt.datetime = NOW, demo: bool = False, mode: str = "PILOT",
          brakes: Optional[BrakeState] = None, equity: Optional[float] = EQ, c: Optional[Config] = None,
          e: Optional[Envelope] = None, judged: Optional[dict] = None, stop: Optional[float] = None,
          data_ok: bool = True, ian_mode: str = "PAPER", data_dir=None) -> D.World:
    c = c or cfg()
    e = e or env()
    trades = clustered(list(trades))
    registered = {k: R.parse_utc(s.registered_utc) for k, s in segs.items() if s.registered_utc}
    live = {k for k, s in segs.items() if s.state in (STATE_LIVE, STATE_LIVE_TRIAL)}
    fit = ES.fit(trades, now, registered=registered, live_keys=live, pins=P.active_pins(e, judged or {}))
    return D.World(now, c, e, mode, trades, data_ok=data_ok, fit=fit, brakes=brakes or BrakeState(), demo=demo,
                   equity=equity, judged=dict(judged or {}), stop_pct=stop, ian_mode=ian_mode, data_dir=data_dir)


def winner(days: int = 16, end: dt.date = None, key: str = GOOD_KEY, pattern=(1.4, 1.2, -0.3), **kw) -> list:
    """A made-up key with a clear edge, every trade after its registration (forward evidence)."""
    end = end or (NOW.date() - dt.timedelta(days=1))
    return series(key, pattern, trading_days(end, days), **kw)


# ------------------------------------------------------------ a data folder --
def write_rows(data: Path, rows: Sequence[dict]) -> None:
    """Made-up closed rows into Trend & Breakout's journal (created by the bot's own Journal class)."""
    from mintel.engine.journal import Journal
    path = data / "journal.sqlite"
    if not path.exists():
        Journal(path).close()
    con = sqlite3.connect(path)
    for r in rows:
        cols = ",".join(r)
        con.execute(f"INSERT INTO trades ({cols}) VALUES ({','.join('?' * len(r))})", tuple(r.values()))
    con.commit()
    con.close()


def paper_book(data: Path) -> None:
    from mintel.broker import paper as PB
    path = data / PB.DB_NAME
    if path.exists():
        return
    con = sqlite3.connect(path)
    con.executescript(PB.SCHEMA)
    con.commit()
    con.close()


def journal_row(key: str, x: float, closed: dt.datetime, *, risk: float = 10.0, ticket: Optional[int] = None,
                symbol: str = "") -> dict:
    """A made-up real journal row (MADE-UP TEST RECORD) with ``x`` R after the whole commission."""
    _bot, approach, kind, cond = key.split("|")
    commission = 0.6
    return {"ticket": ticket or next(_tickets), "intent_key": "MADE-UP TEST RECORD", "symbol": symbol or SYMBOL[kind],
            "side": "BUY", "volume": 0.1, "entry": 1.0, "stop": 0.99, "target": 1.02,
            "opened_utc": (closed - dt.timedelta(minutes=20)).isoformat(), "closed_utc": closed.isoformat(),
            "exit_price": 1.01, "pnl_money": round(x * risk + commission / 2, 2), "realised_r": 9.99,
            "regime": cond, "tactic": approach, "opportunity": 80.0, "risk_money": risk, "reward_risk": 2.5,
            "entry_state_json": json.dumps({"note": "MADE-UP TEST RECORD"}), "exit_reason": "MADE-UP",
            "entry_commission": -commission / 2, "commission_sides": 2}


class FakePage:
    """The page's /api, made up: the standing block the coach reads. ``today`` follows the made-up real rows
    closed on the UK day unless set."""

    def __init__(self, equity: float = EQ, today: float = 0.0, open_: float = 0.0, progress=None):
        self.equity, self.today, self.open, self.progress = equity, today, open_, progress
        self.up = True
        self.calls = 0

    def __call__(self, url: str) -> str:
        self.calls += 1
        if not self.up:
            raise OSError("the page is down (made up)")
        body = {"standing": {"today": self.today, "open": self.open, "equity": self.equity, "balance": self.equity,
                             "bots": [], "error": ""}}
        if self.progress is not None:
            body["coach_progress"] = self.progress
        return json.dumps(body)


def data_folder(tmp_path: Path, *, mode: str = "PILOT", config: Optional[dict] = None, **env_kw) -> tuple[Path, Config, str]:
    """A data folder with config.json, Aaron's envelope, an empty journal and an empty paper book."""
    from mintel.coach import envelope as envelope_mod
    c = cfg()
    data = Path(tmp_path) / "data"
    data.mkdir(parents=True, exist_ok=True)
    c.ops.data_dir = str(data)
    (data / "config.json").write_text(json.dumps(config or {
        "mode": "DEMO", "tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": ["MOMENTUM_CONTINUATION"],
                                "live_since_utc": "2026-10-12T07:58:00+00:00"}}))
    e = Envelope(mode=mode, **env_kw)
    (data / "coach.json").write_text(json.dumps(e.to_dict(), indent=1))
    sha = envelope_mod.envelope_sha(json.loads((data / "coach.json").read_text()))
    write_rows(data, [])
    paper_book(data)
    return data, c, sha


def weather(data: Path, now: dt.datetime, *, demo: bool = True, login: int = 0, symbols=()) -> None:
    """A made-up weather.json as the trader writes it (section 21's account check)."""
    doc = {"schema": 1, "bot": "tnb", "as_of_utc": now.isoformat(), "account_demo": demo, "account_is_demo": demo,
           "account_server": "ICMarketsSC-Demo" if demo else "ICMarketsSC-Live", "account_login": login,
           "config_mode": "DEMO" if demo else "LIVE", "tnb_mode": "", "scanned": len(symbols), "symbols": list(symbols)}
    p = Paths.of(data).weather
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc))
