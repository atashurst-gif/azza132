"""The coach replayed over the record (spec sections 18 and 19, step 3):
``replay --from 2026-09-17`` on the reports-branch fixture
(tests/fixtures/coach_reports_trades.csv - Trend & Breakout's REAL closed
trades 17 Sep - 9 Oct 2026, copied from the reports branch, not made up).

Each day's decision is made from earlier data only; no promotion of any kind
comes before the 8th trading day; every figure in the output comes from the
records; and the output is stamped "flatters: the segments were picked by
people looking at this same record".

What the replay says the coach would have done (asserted below): no key was
ever granted real money over these 17 trading days - the forward-only
evidence a trial needs (15 trades after registration, inside the current
epoch) never built up - while Aaron's three-days rule (S1) would have taken
several approach-in-condition pairs off real money as they turned negative.
"""
from __future__ import annotations

import datetime as dt
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from mintel.coach import Envelope
from mintel.coach import records as R
from mintel.coach.__main__ import main as cli
from mintel.coach.report import FLATTERS
from mintel.coach.run import load_reports_csv, replay
from tests.coach_d_world import cfg

FIXTURE = Path(__file__).parent / "fixtures" / "coach_reports_trades.csv"
START, END = dt.date(2026, 9, 17), dt.date(2026, 10, 9)
UP = ("G1", "G2", "G3", "F1_KEEP", "F1_JUMP", "STOP_UP")


@pytest.fixture(scope="module")
def trades():
    return load_reports_csv(FIXTURE)


@pytest.fixture(scope="module")
def rows(trades):
    return replay(trades, START, END, cfg(), Envelope(mode="PILOT"))       # 200 shuffles a night, as on the Mac


def test_the_replay_covers_every_trading_day_and_is_stamped(rows):
    days = [r["date"] for r in rows]
    assert days[0] == "2026-09-17" and days[-1] == "2026-10-09" and len(days) == 17
    assert all(r["stamp"] == FLATTERS for r in rows)
    assert FLATTERS == "flatters: the segments were picked by people looking at this same record"


def test_every_figure_comes_from_the_records(rows, trades):
    assert sum(1 for t in trades if t.closed_utc <= dt.datetime(2026, 10, 8, 21, 20, tzinfo=dt.timezone.utc)) == 436
    for r in rows:
        day = dt.date.fromisoformat(r["date"])
        want = round(sum(t.net_gbp for t in trades if t.real and t.uk_day == day), 2)
        assert r["real_net_that_day"] == want
        night = dt.datetime.combine(day, dt.time(21, 20), tzinfo=dt.timezone.utc)
        assert r["trades_seen"] <= sum(1 for t in trades if t.closed_utc <= night + dt.timedelta(hours=1))


def test_no_promotion_before_the_eighth_trading_day(rows):
    for i, r in enumerate(rows):
        ups = [p for p in r["planned_tonight"] + r["released_this_morning"] if p.split()[0] in UP]
        if i < 7:
            assert ups == [], (r["date"], ups)
    # and over the whole record nothing went real under the coach (see the module's note)
    assert all(r["real_under_coach"] == [] for r in rows)
    downs = [m for r in rows for m in r["moves_down"]]
    assert any(m.startswith("S1 ") for m in downs)                         # Aaron's three-days rule did fire
    assert all(r["luck"]["shuffles"] == 200 for r in rows)


def test_each_days_decision_uses_earlier_data_only(trades):
    """Replaying the record cut after 30 Sep gives the same decisions up to 30 Sep as the whole record."""
    cut = dt.datetime(2026, 9, 30, 23, 59, tzinfo=dt.timezone.utc)
    full = replay(trades, START, dt.date(2026, 9, 30), cfg(), Envelope(mode="PILOT"), shuffles=40)
    early = replay([t for t in trades if t.closed_utc <= cut], START, dt.date(2026, 9, 30), cfg(),
                   Envelope(mode="PILOT"), shuffles=40)
    assert full == early


def test_the_command_prints_the_replay_from_a_reports_file(tmp_path):
    (tmp_path / "data").mkdir()
    out = io.StringIO()
    with redirect_stdout(out):
        rc = cli(["replay", "--data", str(tmp_path / "data"), "--from", "2026-09-17", "--to", "2026-09-18",
                  "--csv", str(FIXTURE)])
    assert rc == 0
    lines = [json.loads(x) for x in out.getvalue().splitlines() if x.strip()]
    assert [x["date"] for x in lines] == ["2026-09-17", "2026-09-18"]
    assert all(x["stamp"] == FLATTERS for x in lines)
    assert not (tmp_path / "data" / "coach").exists() or not list((tmp_path / "data" / "coach").iterdir())
