"""Shared fixtures.

Everything is built on :class:`~mintel.broker.sim.SimBroker` so the whole
system - including its failure handling - can be tested deterministically on
any operating system, with no MetaTrader 5 installation and no network.
"""
from __future__ import annotations

import os
os.environ.setdefault("MINTEL_OFFLINE", "1")   # no test reaches the internet

import datetime as dt
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mintel.broker.base import CalendarEvent, Side, TF          # noqa: E402
from mintel.broker.sim import SimBroker                          # noqa: E402
from mintel.config import Config                                 # noqa: E402
from mintel.engine.journal import Journal                        # noqa: E402
from mintel.engine.trader import Trader                          # noqa: E402

MID_LONDON = dt.datetime(2026, 3, 10, 13, 30, tzinfo=dt.timezone.utc)


@pytest.fixture
def workdir(tmp_path) -> Path:
    d = tmp_path / "mintel"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def sim() -> SimBroker:
    s = SimBroker(seed=101, start=MID_LONDON, history_bars=6000)
    s.connect()
    return s


@pytest.fixture
def cfg(workdir) -> Config:
    c = Config()
    c.ops.data_dir = str(workdir / "data")
    c.ops.log_dir = str(workdir / "logs")
    c.news.store_path = "calendar.sqlite"
    # A smaller universe keeps the suite fast; universe discovery itself is
    # tested separately against the full symbol list.
    c.universe.max_symbols = 6
    # The lifecycle suite exercises the machinery (entries, management,
    # journal, learning) on synthetic markets, so it runs on the permissive
    # day-one profile.  The selective "big moves only" defaults have their
    # own tests in test_big_moves_only.py.
    lifecycle_profile(c)
    return c


def lifecycle_profile(c: Config) -> Config:
    c.scan.entry_tier = "NORMAL"
    c.scan.min_reward_risk = 1.15
    c.scan.tier_normal = 58.0
    c.scan.disabled_tactics = ()
    c.scan.disabled_tactic_regimes = ()
    c.scan.tactic_min_score = {}
    c.scan.max_cost_fraction_of_stop = 0.30
    c.flowlock.profit_floor_giveback = 0.0
    c.scan.max_losses_per_symbol_per_day = 3
    c.scan.min_seconds_between_entries = 180.0
    c.scan.max_new_positions_per_hour = 4
    c.scan.max_new_positions_per_day = 0
    c.flowlock.breakeven_at_r = 1.0
    c.flowlock.partial_at_r = 1.4
    c.flowlock.partial_fraction = 0.4
    c.flowlock.reversal_thesis_floor = 25.0
    c.flowlock.structure_break_max_r = 0.6
    return c


@pytest.fixture
def journal(workdir) -> Journal:
    j = Journal(workdir / "journal.sqlite")
    yield j
    j.close()


def make_trader(sim: SimBroker, cfg: Config, **kw) -> Trader:
    cfg.account_login = sim.login
    cfg.account_server = sim.server
    t = Trader(sim, cfg, clock=lambda: sim.now, enable_model=False, **kw)
    t.bootstrap()
    return t


@pytest.fixture
def trader(sim, cfg) -> Trader:
    return make_trader(sim, cfg)


def run_cycles(trader: Trader, sim: SimBroker, n: int,
               advance: int = 3) -> list:
    out = []
    for _ in range(n):
        out.append(trader.cycle())
        sim.advance(advance)
    return out
