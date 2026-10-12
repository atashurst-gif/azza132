"""The coach's evidence store (spec section 4): every source read-only, R
after costs, keys, clusters, weights, the haircut and the data checks.

EVERY RECORD IN THIS FILE IS MADE UP for the test (tickets, prices, money),
except the 1 Oct Swiss franc trades, which are Trend & Breakout's real trades
copied from the reports branch (tests/fixtures/coach_reports_trades.csv).
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path

import pytest

from mintel.broker import paper as P
from mintel.coach import records as R
from mintel.engine.journal import Journal
from mintel.runner import shadow as RUNNER_SHADOW
from mintel.bandbreaker import engine as BB_ENGINE
from mintel.crowd import engine as CROWD_ENGINE
from mintel.ian import journal as IAN_JOURNAL

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 12, 15, 0, tzinfo=UTC)          # a Monday afternoon, UK
FIXTURE = Path(__file__).parent / "fixtures" / "coach_reports_trades.csv"
MADE_UP = "MADE-UP TEST RECORD"

REAL_TICKET = 1_993_000_001             # about 1.99e9, as IC Markets' real tickets are
PAPER_TICKET = 960_000_003              # on the paper book
UNBOOKED_HIGH = 960_000_777             # above the paper start, NOT on the book: real


def iso(t: dt.datetime) -> str:
    return t.isoformat()


# ----------------------------------------------------------- made-up stores --
def journal_row(ticket, symbol="EURGBP", side="BUY", tactic="MOMENTUM_CONTINUATION", regime="HIGH_VOL",
                opened=None, closed=None, pnl=5.0, risk=10.0, volume=0.10, sides=2, entry_commission=-0.30,
                opportunity=75.0, rr=2.1, state=None):
    closed = closed or NOW - dt.timedelta(hours=2)
    opened = opened or closed - dt.timedelta(minutes=20)
    return {"ticket": ticket, "intent_key": MADE_UP, "symbol": symbol, "side": side, "volume": volume,
            "entry": 1.0, "stop": 0.99, "target": 1.02, "opened_utc": iso(opened), "closed_utc": iso(closed),
            "exit_price": 1.01, "pnl_money": pnl, "realised_r": 9.99,      # price-R: never used
            "regime": regime, "tactic": tactic, "opportunity": opportunity, "risk_money": risk,
            "reward_risk": rr, "entry_state_json": json.dumps(state or {"note": MADE_UP}),
            "exit_reason": MADE_UP, "entry_commission": entry_commission, "commission_sides": sides}


def write_journal(data: Path, rows) -> Path:
    path = data / "journal.sqlite"
    j = Journal(path)
    j.close()
    con = sqlite3.connect(path)
    for r in rows:
        cols = ",".join(r)
        con.execute(f"INSERT INTO trades ({cols}) VALUES ({','.join('?' * len(r))})", tuple(r.values()))
    con.commit()
    con.close()
    return path


def write_paper_book(data: Path, tickets) -> Path:
    path = data / P.DB_NAME
    con = sqlite3.connect(path)
    con.executescript(P.SCHEMA)
    for i, t in enumerate(tickets):
        con.execute("INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (t + 10_000 + i, t, 990_311, "EURGBP", "BUY", 0.1, 1.0, 1.0, 0.0, -0.3, 0.0, 1,
                     iso(NOW - dt.timedelta(hours=3)), MADE_UP))
    con.commit()
    con.close()
    return path


def write_table(path: Path, schema: str, table: str, rows) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(schema)
    for r in rows:
        cols = ",".join(r)
        con.execute(f"INSERT INTO {table} ({cols}) VALUES ({','.join('?' * len(r))})", tuple(r.values()))
    con.commit()
    con.close()
    return path


def bot_row(ticket, symbol="US500", side="BUY", net=8.0, mode="PAPER", closed=None, **extra):
    closed = closed or NOW - dt.timedelta(hours=1)
    row = {"ticket": ticket, "symbol": symbol, "side": side, "volume": 1.0, "entry": 100.0,
           "initial_stop": 99.0, "opened_utc": iso(closed - dt.timedelta(minutes=30)), "closed_utc": iso(closed),
           "net_pnl": net, "commission": -0.5, "exit_reason": MADE_UP, "mode": mode}
    row.update(extra)
    return row


LAB_SCHEMA = """CREATE TABLE IF NOT EXISTS practice_trades(
    ticket INTEGER PRIMARY KEY, candidate_id TEXT, symbol TEXT, side INT,
    volume REAL, entry REAL, stop REAL, target REAL, opened_utc TEXT,
    closed_utc TEXT, exit_price REAL, exit_reason TEXT, gross_gbp REAL,
    commission_gbp REAL, spread_points INT, slippage_points INT, net_gbp REAL,
    risk_gbp REAL, r REAL, signal_bar_utc TEXT, tick_at_signal_json TEXT,
    fill_source TEXT, mode TEXT DEFAULT 'PRACTICE', source TEXT DEFAULT 'bridge');"""


@pytest.fixture
def data(tmp_path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "config.json").write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"]},
                                               "runner": {"max_risk_money": 10.0},
                                               "bandbreaker": {"risk_money": 10.0}}))
    return d


def full_store(data: Path) -> None:
    """One made-up record of every kind."""
    write_journal(data, [
        journal_row(REAL_TICKET, pnl=6.5),
        journal_row(PAPER_TICKET, pnl=-10.3, symbol="GBPJPY"),
        journal_row(UNBOOKED_HIGH, pnl=2.0, symbol="EURCHF"),
        journal_row(1_993_000_009, pnl=4.0, state={"runner_feed": True, "note": MADE_UP}, symbol="US30",
                    tactic="SESSION_EXPANSION"),
    ])
    write_paper_book(data, [PAPER_TICKET])
    write_table(data / "runner.sqlite", RUNNER_SHADOW.SCHEMA, "trades",
                [bot_row(55, tactic="SESSION_EXPANSION", mode="LIVE"), bot_row(56, mode="PAPER", tactic="")])
    write_table(data / "bandbreaker.sqlite", BB_ENGINE.SCHEMA, "trades", [bot_row(70, symbol="USTEC")])
    write_table(data / "crowd.sqlite", CROWD_ENGINE.SCHEMA, "trades",
                [bot_row(80, symbol="XAUUSD", risk_money=None), bot_row(81, symbol="US500", risk_money=12.0)])
    ian = [dict(ticket=90, instrument="BTCUSD", spot_symbol="BTCUSDT", side="BUY", opened_utc=iso(NOW - dt.timedelta(hours=5)),
                closed_utc=iso(NOW - dt.timedelta(hours=4)), net_pnl=-3.0, regime="TREND", score=71.0, mode="LIVE",
                data_source="binance", risk_money=10.0, exit_reason=MADE_UP),
           dict(ticket=91, instrument="ETHUSD", spot_symbol="ETHUSDT", side="SELL", opened_utc=iso(NOW - dt.timedelta(hours=5)),
                closed_utc=iso(NOW - dt.timedelta(hours=4)), net_pnl=99.0, regime="TREND", score=71.0, mode="PAPER",
                data_source="SYNTHETIC", risk_money=10.0, exit_reason=MADE_UP)]
    write_table(data / "ian.sqlite", IAN_JOURNAL.SCHEMA, "trades", ian)
    write_table(data / "lab" / "lab.sqlite", LAB_SCHEMA, "practice_trades",
                [dict(ticket=1, candidate_id="lab-0123456789", symbol="EURUSD", side=-1, opened_utc=iso(NOW - dt.timedelta(hours=3)),
                      closed_utc=iso(NOW - dt.timedelta(hours=2)), net_gbp=-13.6, commission_gbp=0.55, risk_gbp=13.0,
                      exit_reason=MADE_UP)])


def by_source(ev, source):
    return {t.ticket: t for t in ev.trades if t.source == source}


# ------------------------------------------------------- real versus paper --
def test_real_versus_practice_is_decided_by_the_paper_book_never_the_ticket_number(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    tnb = by_source(ev, R.JOURNAL)
    assert tnb[REAL_TICKET].real is True                 # about 1.99e9: real
    assert tnb[PAPER_TICKET].real is False               # on the paper book: practice
    assert tnb[UNBOOKED_HIGH].real is True               # above 960,000,001 but not on the book: real
    assert ev.checks[R.JOURNAL].ok


def test_runner_feed_rows_are_left_out_of_trend_and_breakout(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    assert 1_993_000_009 not in by_source(ev, R.JOURNAL)
    assert ev.checks[R.JOURNAL].skipped == 1


def test_keys_follow_the_spec_for_every_bot(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    keys = {t.source: set() for t in ev.trades}
    for t in ev.trades:
        keys[t.source].add(t.key)
    assert "tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL" in keys[R.JOURNAL]
    assert keys[R.RUNNER] == {"runner|SESSION_EXPANSION|INDEX|*", "runner|*|INDEX|*"}
    assert keys[R.BANDBREAKER] == {"bandbreaker|*|INDEX|*"}             # USTEC: an index (infer_group misses it)
    assert keys[R.CROWD] == {"crowd|*|GOLD|*", "crowd|*|INDEX|*"}
    assert keys[R.IAN] == {"ian|*|CRYPTO|TREND"}
    assert keys[R.LAB] == {"lab|LAB_0123456789|FX_MAJOR|*"}
    import mintel.coach as C
    for t in ev.trades:                                     # every key is one the policy's Key accepts
        assert str(C.Key.parse(t.key)) == t.key
    lab = by_source(ev, R.LAB)[1]
    assert lab.real is False and lab.side == "SELL"


def test_ian_synthetic_rows_are_tests_not_results(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    assert set(by_source(ev, R.IAN)) == {90}
    assert ev.checks[R.IAN].skipped == 1
    assert by_source(ev, R.IAN)[90].real is True


# --------------------------------------------------------------- the unit --
def test_r_is_after_costs_on_planned_risk_and_clipped(data):
    write_journal(data, [journal_row(REAL_TICKET, pnl=6.5, risk=13.0),
                         journal_row(REAL_TICKET + 1, pnl=-80.0, risk=10.0),
                         journal_row(REAL_TICKET + 2, pnl=120.0, risk=10.0)])
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    t = by_source(ev, R.JOURNAL)
    assert t[REAL_TICKET].x == pytest.approx(0.5)                    # 6.50 / 13.00, never realised_r (9.99)
    assert t[REAL_TICKET + 1].x == -3.0 and t[REAL_TICKET + 1].x_raw == -8.0
    assert t[REAL_TICKET + 2].x == 5.0
    # commission x 1.5: x - 0.5 x commission / risk (whole commission = 2 x the opening half)
    assert t[REAL_TICKET].commission_gbp == pytest.approx(0.60)
    assert t[REAL_TICKET].x_cost15 == pytest.approx(0.5 - 0.5 * 0.60 / 13.0)


def test_commission_before_9_october_is_estimated_with_the_traders_own_estimator(data):
    old = NOW - dt.timedelta(days=6)
    write_journal(data, [journal_row(REAL_TICKET, pnl=5.0, volume=0.10, sides=None, entry_commission=None, closed=old),
                         journal_row(REAL_TICKET + 1, pnl=5.0, volume=0.10, sides=2, entry_commission=-0.30)])
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    t = by_source(ev, R.JOURNAL)
    # RiskManager.row_net: one missing side at commission_fallback_per_lot / 2 (3.00 a lot) x 0.10 lots
    from mintel.config import Config
    from mintel.engine.risk import RiskManager
    expect, est = RiskManager(Config()).row_net({"pnl_money": 5.0, "commission_sides": None, "volume": 0.10,
                                                 "symbol": "EURGBP"})
    assert est == pytest.approx(0.30) and t[REAL_TICKET].net_gbp == pytest.approx(expect) == pytest.approx(4.70)
    assert R.F_COMMISSION_ESTIMATED in t[REAL_TICKET].flags
    assert t[REAL_TICKET + 1].net_gbp == 5.0 and R.F_COMMISSION_ESTIMATED not in t[REAL_TICKET + 1].flags
    assert ev.checks[R.JOURNAL].flags[R.F_COMMISSION_ESTIMATED] == 1


def test_the_fallback_risk_is_used_and_flagged_when_a_record_has_no_risk(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    runner = by_source(ev, R.RUNNER)
    bb = by_source(ev, R.BANDBREAKER)
    crowd = by_source(ev, R.CROWD)
    assert runner[55].risk_gbp == 10.0 and R.F_RISK_ESTIMATED in runner[55].flags      # no risk_money column
    assert bb[70].risk_gbp == 10.0 and R.F_RISK_ESTIMATED in bb[70].flags              # no risk_money column
    assert crowd[80].risk_gbp == 10.0 and R.F_RISK_ESTIMATED in crowd[80].flags        # the column, but NULL
    assert crowd[81].risk_gbp == 12.0 and R.F_RISK_ESTIMATED not in crowd[81].flags
    assert R.F_COMMISSION_UNKNOWN in by_source(ev, R.IAN)[90].flags                    # Ian records no commission


def test_a_missing_journal_column_makes_the_source_unreadable_never_a_guess(data):
    con = sqlite3.connect(data / "journal.sqlite")
    con.execute("CREATE TABLE trades (ticket INTEGER PRIMARY KEY, symbol TEXT, side TEXT, closed_utc TEXT, "
                "pnl_money REAL, tactic TEXT, regime TEXT)")                         # no risk_money
    con.execute("INSERT INTO trades VALUES (1, 'EURGBP', 'BUY', ?, 1.0, 'MOMENTUM_CONTINUATION', 'TREND')",
                (iso(NOW),))
    con.commit()
    con.close()
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    assert ev.checks[R.JOURNAL].status == R.UNREADABLE and not ev.data_ok
    assert "risk_money" in ev.checks[R.JOURNAL].failed[0]


# --------------------------------------------------------------- clusters --
def test_legs_follow_the_table():
    assert R.legs("EURGBP", "BUY") == ("EUR+", "GBP-")
    assert R.legs("EURGBP", "SELL") == ("EUR-", "GBP+")
    assert R.legs("US500", "BUY") == R.legs("US30", "BUY") == R.legs("USTEC", "BUY") == ("USEQ+",)
    assert R.legs("DE40", "SELL") == ("EUEQ-",) and R.legs("UK100", "BUY") == ("UKEQ+",)
    assert R.legs("JP225", "BUY") == ("JPEQ+",) and R.legs("AUS200", "BUY") == ("AUS200+",)
    assert R.legs("XAUJPY", "BUY") == ("XAU+",) and R.legs("XAGUSD", "BUY") == ("XAG+",)
    assert R.legs("BTCUSD", "BUY") == ("BTC+",) and R.legs("ETHUSD", "BUY") == ("ETH+",)


def _trade(ticket, symbol, side, opened, bot="tnb", minutes=10):
    return R.Trade("journal", bot, ticket, "MOMENTUM_CONTINUATION", R.kind_of(symbol), "HIGH_VOL", symbol, side,
                   opened, opened + dt.timedelta(minutes=minutes), True, 1.0, 0.1, 10.0)


def test_a_cluster_is_one_bot_one_direction_a_shared_leg_within_60_minutes():
    t0 = dt.datetime(2026, 10, 1, 7, 0, tzinfo=UTC)
    trades = R.assign_clusters([
        _trade(1, "EURGBP", "BUY", t0),                                   # EUR+ GBP-
        _trade(2, "EURUSD", "BUY", t0 + dt.timedelta(minutes=20)),       # shares EUR+: same move
        _trade(3, "GBPCHF", "BUY", t0 + dt.timedelta(minutes=25)),       # GBP+ : the opposite bet on GBP
        _trade(4, "AUDUSD", "SELL", t0 + dt.timedelta(minutes=30)),      # a SELL: another direction
        _trade(5, "EURJPY", "BUY", t0 + dt.timedelta(minutes=61)),       # EUR+ but 61 min after the first
        _trade(6, "EURGBP", "BUY", t0 + dt.timedelta(minutes=5), bot="runner"),   # another bot
    ])
    c = {t.ticket: t.cluster for t in trades}
    assert c[1] == c[2]
    assert len({c[1], c[3], c[4], c[5]}) == 4
    assert c[6].startswith("runner:")


def test_the_1_october_swiss_franc_trades_form_a_few_clusters():
    """REAL trades (reports branch): Formula 1's 1 Oct trades on the Swiss franc."""
    rows = [r for r in csv.DictReader(l for l in FIXTURE.open() if not l.startswith("#"))
            if r["report_day"] == "2026-10-01" and "CHF" in r["symbol"] and r["tactic"] == "MOMENTUM_CONTINUATION"]
    trades = R.assign_clusters([R.Trade("journal", "tnb", int(r["ticket"]), "MOMENTUM_CONTINUATION",
                                        R.kind_of(r["symbol"]), r["regime"], r["symbol"], r["side"],
                                        R.parse_utc(r["opened_utc"]), R.parse_utc(r["closed_utc"]), True,
                                        float(r["net_money"]), abs(float(r["commission"] or 0)), 6.09)
                                for r in rows])
    assert len(trades) == 16
    clusters = {t.cluster for t in trades}
    assert len(clusters) == 5                                              # a few bets, not 16
    by = {t.ticket: t.cluster for t in trades}
    morning = (1978567885, 1978585628, 1978597670, 1978602717, 1978646284)   # 07:43-08:08 sells on CHF
    afternoon = (1979745973, 1979752676, 1979769904, 1979786635, 1979892937, 1979898357, 1979932217, 1979937972)
    assert len({by[t] for t in morning}) == 1 and len({by[t] for t in afternoon}) == 1
    assert by[morning[0]] != by[afternoon[0]]                              # 6 hours apart: two moves
    assert by[1978614110] != by[morning[0]]                                # the 07:59 CHFJPY BUY: its own bet
    assert by[1978468717] != by[morning[0]]                                # XAUCHF: the XAU leg alone


# ---------------------------------------------------------- the data checks --
def test_a_source_with_5_percent_or_more_unusable_rows_fails(data):
    rows = [journal_row(REAL_TICKET + i, pnl=1.0) for i in range(19)] + [journal_row(REAL_TICKET + 99, risk=0.0)]
    write_journal(data, rows)
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    c = ev.checks[R.JOURNAL]
    assert c.excluded == 1 and c.rows == 20 and c.status == R.UNREADABLE
    assert not ev.data_ok
    rows = [journal_row(REAL_TICKET + i, pnl=1.0) for i in range(20)] + [journal_row(REAL_TICKET + 99, risk=None)]
    (data / "journal.sqlite").unlink()
    write_journal(data, rows)
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    assert ev.checks[R.JOURNAL].excluded == 1 and ev.checks[R.JOURNAL].ok      # 1 of 21: under 5%


def test_a_shrunk_record_and_a_closing_time_in_the_future_are_stale(data):
    write_journal(data, [journal_row(REAL_TICKET), journal_row(REAL_TICKET + 1,
                                                               closed=NOW + dt.timedelta(minutes=30))])
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,), previous_counts={R.JOURNAL: 5})
    c = ev.checks[R.JOURNAL]
    assert c.status == R.STALE and len(c.failed) == 2
    assert ev.counts() == {R.JOURNAL: 2}


def test_tickets_are_counted_once(data):
    write_table(data / "bandbreaker.sqlite", BB_ENGINE.SCHEMA.replace("ticket INTEGER PRIMARY KEY", "ticket INTEGER"),
                "trades", [bot_row(70), bot_row(70), bot_row(71)])
    ev = R.read_evidence(data, now=NOW, sources=(R.BANDBREAKER,))
    assert ev.checks[R.BANDBREAKER].duplicates == 1
    assert sorted(by_source(ev, R.BANDBREAKER)) == [70, 71] and ev.checks[R.BANDBREAKER].ok


def test_missing_files(data):
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW)
    assert ev.checks[R.JOURNAL].status == R.UNREADABLE              # Trend & Breakout's journal is always there
    assert all(ev.checks[s].ok and not ev.checks[s].present for s in (R.RUNNER, R.LAB, R.IAN))
    assert ev.unreadable_bots() == ["tnb"]


def test_on_paper_without_its_paper_record_real_and_practice_cannot_be_told_apart(data):
    write_journal(data, [journal_row(REAL_TICKET)])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    assert ev.checks[R.JOURNAL].status == R.UNREADABLE and not ev.trades


def _files(data: Path) -> dict:
    """Every file but sqlite's own -wal/-shm side files (a read-only reader of a
    WAL database may create them; the database itself is never written)."""
    return {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in data.rglob("*")
            if p.is_file() and not p.name.endswith(("-wal", "-shm"))}


def _rows(path: Path, table: str) -> list:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return con.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    finally:
        con.close()


def test_every_source_is_opened_read_only_and_nothing_is_written(data):
    full_store(data)
    before = _files(data)
    rows_before = {n: _rows(data / n, "trades") for n in ("journal.sqlite", "runner.sqlite", "crowd.sqlite")}
    R.read_evidence(data, now=NOW)
    R.read_evidence(data, now=NOW)
    assert _files(data) == before                                   # no file changed, none created
    assert {n: _rows(data / n, "trades") for n in rows_before} == rows_before
    assert not (data / "coach").exists()                            # reading writes nothing, not even its own folder


def test_secrets_json_is_never_opened(data, monkeypatch):
    full_store(data)
    (data / "secrets.json").write_text(json.dumps({"account_password": "made-up"}))
    opened = []
    real_open, real_read = open, Path.read_text

    def spy_open(file, *a, **k):
        opened.append(str(file))
        return real_open(file, *a, **k)

    def spy_read(self, *a, **k):
        opened.append(str(self))
        return real_read(self, *a, **k)

    monkeypatch.setattr("builtins.open", spy_open)
    monkeypatch.setattr(Path, "read_text", spy_read)
    R.read_evidence(data, now=NOW)
    R.baseline_config(data)
    assert opened and not any(Path(p).name == "secrets.json" for p in opened)
    assert any(Path(p).name == "config.json" for p in opened)


# --------------------------------------------------- the broker reconciliation --
def test_the_records_must_match_the_pages_today_figure(data):
    full_store(data)
    ev = R.read_evidence(data, now=NOW)
    # real today: Trend & Breakout 6.50 + 2.00, the Runner's LIVE ride 8.00, Ian -3.00 (practice never counts)
    assert ev.real_today(NOW) == pytest.approx(13.50)
    assert R.reconcile(ev.trades, 13.50, NOW) == {"ok": True, "records_today": 13.5, "page_today": 13.5,
                                                  "tolerance": 3.0, "why": ""}
    assert R.reconcile(ev.trades, 13.50 + 2.99, NOW)["ok"]                  # within the £3 floor
    assert not R.reconcile(ev.trades, 13.50 + 3.10, NOW)["ok"]
    missing = R.reconcile(ev.trades, None, NOW)
    assert missing["ok"] is False and missing["page_today"] is None          # no page figure: no grants


def test_the_reconciliation_tolerance_is_10_percent_of_a_large_figure():
    t = [R.Trade("journal", "tnb", 1, "A", "FX_MINOR", "TREND", "EURGBP", "BUY", NOW - dt.timedelta(hours=2),
                 NOW - dt.timedelta(hours=1), True, -100.0, 0.6, 10.0)]
    ok = R.reconcile(t, -109.0, NOW)
    assert ok["ok"] and ok["tolerance"] == pytest.approx(10.9)
    assert not R.reconcile(t, -112.0, NOW)["ok"]
    # yesterday's trades are not today's
    assert R.reconcile(t, -100.0, NOW + dt.timedelta(days=1))["records_today"] == 0.0


# ------------------------------------------------------------------ weights --
def test_the_weight_is_practice_times_age_times_epoch():
    t = R.Trade("journal", "crowd", 1, "*", "GOLD", "*", "XAUUSD", "BUY", NOW - dt.timedelta(hours=2),
                NOW - dt.timedelta(hours=1), True, 1.0, 0.0, 10.0)
    assert R.weight(t, NOW) == 1.0
    assert R.weight(R.Trade(**{**_fields(t), "real": False}), NOW) == 0.5
    # 30 UK trading days old: half; 6 weeks = 30 weekdays
    old = NOW - dt.timedelta(weeks=6)
    assert R.weight(R.Trade(**{**_fields(t), "closed_utc": old, "opened_utc": old}), NOW) == pytest.approx(0.25)
    # (0.5 for 30 days) x (0.5: before the 5 Oct reset epoch)
    tnb_old = R.Trade(**{**_fields(t), "bot": "tnb", "closed_utc": dt.datetime(2026, 10, 2, 12, tzinfo=UTC)})
    age = R.trading_days_between(dt.date(2026, 10, 2), dt.date(2026, 10, 12))
    assert age == 6
    assert R.weight(tnb_old, NOW) == pytest.approx(0.5 ** (6 / 30) * 0.5)


def _fields(t):
    return {f: getattr(t, f) for f in t.__dataclass_fields__}


def test_uk_trading_days():
    assert R.trading_days_between(dt.date(2026, 10, 9), dt.date(2026, 10, 12)) == 1        # Fri -> Mon
    assert R.trading_days_between(dt.date(2026, 10, 10), dt.date(2026, 10, 12)) == 1       # Sat -> Mon
    assert R.trading_days_between(dt.date(2026, 10, 12), dt.date(2026, 10, 12)) == 0
    assert R.trading_days_back(dt.date(2026, 10, 12), 2) == dt.date(2026, 10, 9)
    # UK day across the clock change (25 Oct): 23:30 UTC on 24 Oct is 00:30 UK on 25 Oct (BST)
    assert R.uk_date(dt.datetime(2026, 10, 24, 23, 30, tzinfo=UTC)) == dt.date(2026, 10, 25)
    assert R.uk_date(dt.datetime(2026, 10, 25, 23, 30, tzinfo=UTC)) == dt.date(2026, 10, 25)


# ----------------------------------------------------------------- haircut --
def _hx(i, real, x, week):
    day = dt.datetime(2026, 9, 7, 12, tzinfo=UTC) + dt.timedelta(weeks=week, days=i % 5)
    return R.Trade("journal", "tnb", i, "A", "FX_MINOR", "TREND", "EURGBP", "BUY", day, day, real, x * 10.0, 0.0, 10.0)


def test_the_practice_haircut():
    few = [_hx(i, i % 2 == 0, 0.5, 0) for i in range(30)]
    assert R.haircut(few) == R.Haircut(0.10, False, None, None, 15, 15)
    # 20 real at 0.0 R and 20 practice at +0.20 R in the same weeks: h = the gap (0.20)
    rows = [_hx(i, True, 0.0 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)] + \
           [_hx(100 + i, False, 0.2 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)]
    h = R.haircut(rows)
    assert h.measured and h.h == pytest.approx(0.20) and h.gap == pytest.approx(0.20)
    # clipped to [0.05, 0.30], and practice is never credited above real
    worse_practice = [_hx(i, True, 1.0 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)] + \
                     [_hx(100 + i, False, 0.0 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)]
    assert R.haircut(worse_practice).h == 0.05
    big = [_hx(i, True, 0.0, i % 2) for i in range(20)] + [_hx(100 + i, False, 1.0, i % 2) for i in range(20)]
    assert R.haircut(big).h == 0.30
    # moved only when the gap moves by more than 2 standard errors
    nudged = [_hx(i, True, 0.0 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)] + \
             [_hx(100 + i, False, 0.25 + (0.5 if i % 2 else -0.5), i % 2) for i in range(20)]
    assert R.haircut(nudged, previous=h).h == pytest.approx(0.20)
    assert R.haircut(big, previous=h).h == 0.30
    # weeks without both kinds are not overlapping: they do not count
    apart = [_hx(i, True, 0.0, 0) for i in range(25)] + [_hx(100 + i, False, 1.0, 3) for i in range(25)]
    assert not R.haircut(apart).measured


# ------------------------------------------------------------- the gate log --
def test_the_gate_log_joins_a_coach_granted_ticket_to_its_route(data):
    write_journal(data, [journal_row(REAL_TICKET, symbol="US30", tactic="SESSION_EXPANSION", regime="TREND")])
    write_paper_book(data, [])
    gate = data / "coach" / "gate"
    gate.mkdir(parents=True)
    (gate / "tnb-2026-10-12.jsonl").write_text(
        "not json\n" + json.dumps({"ts": iso(NOW), "ticket": REAL_TICKET, "key": "tnb|SESSION_EXPANSION|INDEX|TREND",
                                   "route": "REAL", "money": 6.53, "seq": 1842, "source": "policy",
                                   "why": MADE_UP}) + "\n")
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    t = by_source(ev, R.JOURNAL)[REAL_TICKET]
    assert t.coach_route == "REAL" and t.policy_seq == 1842 and R.F_COACH_REAL in t.flags


def test_the_paper_records_coach_real_table_marks_a_coach_granted_ticket(data):
    write_journal(data, [journal_row(REAL_TICKET, symbol="US30", tactic="SESSION_EXPANSION", regime="TREND"),
                         journal_row(REAL_TICKET + 1)])
    write_paper_book(data, [])
    con = sqlite3.connect(data / P.DB_NAME)
    con.execute("CREATE TABLE IF NOT EXISTS coach_real (ticket INTEGER PRIMARY KEY, opened_utc TEXT, key TEXT)")
    con.execute("INSERT INTO coach_real VALUES (?,?,?)", (REAL_TICKET, iso(NOW), "tnb|SESSION_EXPANSION|INDEX|TREND"))
    con.commit()
    con.close()
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    t = by_source(ev, R.JOURNAL)
    assert R.F_COACH_REAL in t[REAL_TICKET].flags and R.F_COACH_REAL not in t[REAL_TICKET + 1].flags
    assert R.coach_real_keys(ev.trades) == {"tnb|SESSION_EXPANSION|INDEX|TREND"}


def test_no_bot_process_loads_the_evidence_code():
    """The bots obey only through the gate: the evidence modules are never
    imported by a trading process, so whatever they compute, the bots' orders,
    sizes and daily-loss figure are today's unless a valid policy says
    otherwise."""
    import subprocess
    import sys
    code = ("import sys, mintel.engine.trader, mintel.ian.run, mintel.ian.engine, mintel.broker.paper, mintel.run\n"
            "print(sorted(m for m in sys.modules if m.startswith('mintel.coach.')))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120,
                         cwd=str(Path(__file__).resolve().parents[1]), env={**os.environ, "PYTHONHASHSEED": "0"})
    assert out.returncode == 0, out.stderr
    loaded = eval(out.stdout.strip().splitlines()[-1])
    for mod in ("records", "estimate", "rules", "tweaks", "epochs"):
        assert f"mintel.coach.{mod}" not in loaded


def test_the_reports_record_read_back_through_a_journal_gives_the_same_evidence(data):
    """Trend & Breakout's REAL trades (reports branch) written into a journal and
    a paper book, then read back the way the coach reads the Mac's files: the
    same keys, outcomes and clusters as the fixture's own trades."""
    from tests.test_coach_estimate import reports_trades
    from mintel.coach import estimate as ES
    fixture = reports_trades()
    rows = []
    for t in fixture:
        rows.append(journal_row(t.ticket, symbol=t.symbol, side=t.side, tactic=t.approach, regime=t.condition,
                                opened=t.opened_utc, closed=t.closed_utc, pnl=t.net_gbp, risk=t.risk_gbp,
                                sides=2, entry_commission=-t.commission_gbp / 2, opportunity=t.score))
    write_journal(data, rows)
    write_paper_book(data, [t.ticket for t in fixture if not t.real])
    now = dt.datetime(2026, 10, 12, 21, 20, tzinfo=UTC)
    ev = R.read_evidence(data, now=now, sources=(R.JOURNAL,))
    assert ev.data_ok and ev.checks[R.JOURNAL].rows == 442 and len(ev.trades) == 442
    got = {t.ticket: t for t in ev.trades}
    for t in fixture:
        g = got[t.ticket]
        assert (g.key, g.real, g.cluster) == (t.key, t.real, t.cluster)
        assert g.x == pytest.approx(t.x) and g.commission_gbp == pytest.approx(t.commission_gbp)
    a = ES.fit(ev.trades, now, pins=[R.make_key("tnb", "MOMENTUM_CONTINUATION", "FX_MINOR", "*")])
    b = ES.fit(fixture, now, pins=[R.make_key("tnb", "MOMENTUM_CONTINUATION", "FX_MINOR", "*")])
    key = "tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL"
    assert a.keys[key].m == pytest.approx(b.keys[key].m) and a.keys[key].p_plus == pytest.approx(b.keys[key].p_plus)
