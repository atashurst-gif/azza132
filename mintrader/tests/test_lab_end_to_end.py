"""The lab end to end, on a simulated broker (slow).

SimBroker -> the lab's fetch (through the night's own step, gently paced on a
fake clock) -> the night's ten steps -> the board -> the page: "/", "/lab" and an
idea's own page. Everything here is made up, so every lab figure must read
"TEST DATA - not a result" and the board must say ``synthetic: true``.

EVERY FIGURE HERE IS MADE-UP TEST DATA.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.lab import ON_THE_BENCH, PRACTICE, LabPaths
from mintel.lab import nightly as NL
from mintel.lab.ledger import LabLedger

# 13 Oct (the TRADING SQUAD redesign): "/" is the new squad page (mintel/ops/squad.py). The old Home these tests
# pin - the account cards, the bot tiles, the lab and coach lines - is kept, unchanged, at /advanced (ADVANCED >
# Overview); "/?period=", "/?bot=" and "/?show=" still open the old pages. So "/" below reads "/advanced".

UTC = dt.timezone.utc
NAMES = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD", "XAUUSD", "GER40"]


class SimFeed:
    """The simulated broker behind the lab's four questions (MADE-UP prices)."""
    synthetic = True

    def __init__(self, broker):
        self.broker = broker
        self.asked = []

    def ping(self):
        self.asked.append("ping")
        return {"ok": True, "mt5_connected": True, "backend": "sim"}

    def symbols(self):
        self.asked.append("symbols")
        return self.broker.symbols()

    def spec(self, s):
        self.asked.append("spec")
        return self.broker.spec(s)

    def bars(self, symbol, tf, count, end=None):
        self.asked.append("bars")
        return self.broker.bars(symbol, tf, count, end)


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


@pytest.mark.slow
def test_simbroker_to_fetch_to_the_night_to_the_board_to_the_page(tmp_path):
    from tests.test_page_figures import a_day
    from tests.test_lab_page import lab_rows, pages

    end = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
    broker = SimBroker(NAMES, seed=5, start=end, history_bars=58 * 1440)
    broker.connect()
    feed = SimFeed(broker)
    d = a_day(tmp_path)
    cfg = Config()
    cfg.ops.data_dir = str(d.data)
    cfg.ops.log_dir = str(tmp_path / "logs")
    paths = LabPaths(d.data, tmp_path / "logs")
    mono = [1000.0]

    def sleep(s):
        mono[0] += s

    # night 1: Saturday, the fetch window is open all day; the markets, the prices, the cut, 30 ideas
    sat = dt.datetime(2026, 10, 10, 12, 0, tzinfo=UTC)               # 13:00 in London: that night is the 10th
    lines = []
    n = NL.Night(cfg, str(d.data / "config.json"), now_fn=Clock(sat), log=lines.append, no_fetch=False,
                 max_candidates=30, feed_factory=lambda: feed,
                 fetch_options={"sleep": sleep, "clock": lambda: mono[0], "pace_s": 1.0})
    assert n.run() == 0, "\n".join(lines[-20:])
    assert set(feed.asked) <= {"ping", "symbols", "spec", "bars"}
    state = json.loads((paths.run_dir("2026-10-10") / "state.json").read_text())
    assert all(v["done"] for v in state["steps"].values()), state
    assert "pages" in state["steps"]["fetch"]["note"]
    uni = json.loads(paths.universe.read_text())
    assert len(uni["symbols"]) >= 8
    summary = json.loads(paths.summary.read_text())
    assert summary["synthetic"] is True and summary["store"]["synthetic"] is True
    assert summary["store"]["ready"] and summary["store"]["complete_days"] >= 40
    assert summary["store"]["past"] and summary["store"]["fresh"]
    assert summary["store"]["past"][1] < summary["store"]["cut_day"] <= summary["store"]["fresh"][0]
    assert summary["nightly"]["tested"] == 30 and summary["trials"]["lab"] == 30
    led = LabLedger(paths.sqlite)
    assert {r[0] for r in led.query("SELECT synthetic FROM tests")} == {1}
    # (scaffolding) one idea not yet tried is put on the bench by hand, to carry the rest of the
    # machine through a promotion and onto the page; nothing about it is a result
    idea = led.one("SELECT id FROM candidates WHERE stage='IDEA' AND family='PREV_DAY_LEVELS' ORDER BY id")[0]
    led.execute("UPDATE candidates SET stage=? WHERE id=?", (ON_THE_BENCH, idea))
    led.close()

    # night 2: Monday night, no fetch; it gives the bench idea a practice place
    mon = dt.datetime(2026, 10, 12, 22, 10, tzinfo=UTC)
    n = NL.Night(cfg, str(d.data / "config.json"), now_fn=Clock(mon), log=lines.append, no_fetch=True,
                 max_candidates=10)
    assert n.run() == 0
    led = LabLedger(paths.sqlite)
    assert led.candidate(idea)["stage"] == PRACTICE
    place = led.one("SELECT * FROM roster WHERE candidate_id=?", (idea,))
    assert place is not None and json.loads(place["symbols_json"])
    assert led.n_tests() == 40
    led.close()
    summary = json.loads(paths.summary.read_text())
    entry = next(c for c in summary["candidates"] if c["id"] == idea)
    assert entry["stage"] == PRACTICE and entry["practice"]["look1_due"] == "2026-10-26"
    assert [c["key"] for c in entry["checks"]] == ["trades", "steady", "fresh", "costs", "luck"]
    assert entry["sources"] and entry["synthetic"] is True
    assert (paths.run_dir("2026-10-12") / "report.md").read_text().count("TEST DATA") >= 2

    # the page: "/", "/lab" and the idea's own page, all saying TEST DATA, none showing money
    home, board, one = pages(d, "/advanced", "/lab", f"/lab?id={idea}")
    rows = lab_rows(home)
    assert idea in rows and "TEST DATA - not a result" in rows[idea] and "£" not in rows[idea]
    assert "TEST DATA - not a result" in board and "£" not in board
    assert "TEST DATA - not a result" in one and PRACTICE in one
