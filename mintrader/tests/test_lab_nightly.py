"""The lab's night: its guards, its ten steps, the deadline and the upload.

What must hold: a finished night is never run twice; a running night is never
doubled (a lock with a live pid; a dead pid's lock is removed); data/lab/STOP
stops it cleanly between steps and the night resumes where it stopped; less
than 1 GB free runs nothing; every step is idempotent and a night cut short (a
crash, a signal) resumes with no test counted twice; the deadline stops the past
tests between ideas and the board is still written; the batch is sized from the
first ten ideas measured; --no-fetch fetches nothing, and nothing is fetched while
MetaTrader is not connected; the upload goes to reports/lab/<night>/ with the
token from the report uploader's own reader; a store of made-up prices gives a
board that says ``synthetic: true``.

EVERY FIGURE HERE IS MADE-UP TEST DATA (a SYNTHETIC store, made-up results).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys

import pytest

from mintel.config import Config
from mintel.lab import LabPaths, Result, WindowInfo, synth
from mintel.lab import features as FEAT
from mintel.lab import nightly as NL
from mintel.lab import stages as ST
from mintel.lab.store import BarStore, write_universe

UTC = dt.timezone.utc
START = dt.datetime(2026, 10, 12, 22, 10, tzinfo=UTC)              # Monday 23:10 in London
NIGHT = "2026-10-12"


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t = self.t + dt.timedelta(seconds=seconds)


@pytest.fixture(scope="module")
def template(tmp_path_factory):
    """A MADE-UP store: two majors, 45 weekdays of SYNTHETIC minute prices, features built."""
    root = tmp_path_factory.mktemp("labtemplate")
    paths = LabPaths(root / "data")
    store = BarStore(paths)
    ents = [synth.entry_for("EURUSD"), synth.entry_for("GBPUSD")]
    synth.fill_store(store, ents, synth.weekdays("2026-08-10", 45), seed=7,
                     now=dt.datetime(2026, 10, 12, 12, tzinfo=UTC))
    write_universe(paths, ents)
    FEAT.ensure_months(store, [e.name for e in ents])
    return root


@pytest.fixture
def home(template, tmp_path):
    dst = tmp_path / "home"
    shutil.copytree(template, dst)
    cfg = Config()
    cfg.ops.data_dir = str(dst / "data")
    cfg.ops.log_dir = str(dst / "logs")
    cfg.save(dst / "data" / "config.json")
    return dst


def make(home, clock=None, **kw):
    cfg = Config.load(home / "data" / "config.json")
    clock = clock or Clock(START)
    lines: list = []
    kw.setdefault("no_fetch", True)
    n = NL.Night(cfg, str(home / "data" / "config.json"), now_fn=clock, log=lines.append, **kw)
    return n, clock, lines


def paths_of(home):
    return LabPaths(home / "data", home / "logs")


def fake_runs(monkeypatch, clock, seconds):
    """Each past test takes ``seconds`` on the fake clock and makes no trade (so it is binned)."""
    calls = []

    def run(c, window, family=None):
        calls.append(c.id)
        clock.advance(seconds)
        return Result(c.id, WindowInfo("past"), [], {d: (0.0, 0) for d in window.days}, {}, True, seconds)

    monkeypatch.setattr(ST.sim, "run", run)
    return calls


def db(home):
    from mintel.lab.ledger import LabLedger
    return LabLedger(paths_of(home).sqlite)


# ------------------------------------------------------------------ a night ---
def test_a_night_on_a_synthetic_store_writes_a_synthetic_board(home):
    n, clock, lines = make(home, max_candidates=12)
    assert n.run() == 0
    p = paths_of(home)
    summary = json.loads(p.summary.read_text())
    assert summary["schema"] == 1 and summary["synthetic"] is True
    assert summary["store"]["ready"] and summary["store"]["stretch_id"] == "F-2026-10"
    assert summary["nightly"]["date"] == NIGHT and summary["nightly"]["tested"] == 12
    assert summary["nightly"]["ok"] is True
    assert summary["funnel"]["ideas"] == 632 and summary["trials"]["lab"] == 12
    run_dir = p.run_dir(NIGHT)
    assert (run_dir / "done").exists()
    report = (run_dir / "report.md").read_text()
    assert report.splitlines()[2].startswith("**TEST DATA - not a result.**")
    tests = (run_dir / "tests.csv").read_text().splitlines()
    assert len(tests) == 13 and tests[0].startswith("id,candidate_id,family")
    state = json.loads((run_dir / "state.json").read_text())
    assert set(state["steps"]) == set(NL.STEPS) and all(v["done"] for v in state["steps"].values())
    assert state["steps"]["fetch"]["note"] == "skipped (--no-fetch)"
    assert any("step past: 12 of 12 tested" in ln for ln in lines)
    # every step's seconds are in the log
    for step in NL.STEPS:
        assert any(f"step {step}:" in ln and ln.rstrip().endswith(" s)") for ln in lines), step
    led = db(home)
    assert {r["synthetic"] for r in led.query("SELECT synthetic FROM tests")} == {1}
    run = led.one("SELECT * FROM runs WHERE date=?", (NIGHT,))
    assert run["ok"] == 1 and run["tested"] == 12 and run["finished_utc"]
    led.close()
    # nobody's heartbeat: the lab writes none
    assert not any("lab" in f.name for f in (home / "data" / "heartbeats").glob("*")) \
        if (home / "data" / "heartbeats").exists() else True


def test_a_finished_night_is_never_run_again(home):
    p = paths_of(home)
    (p.run_dir(NIGHT)).mkdir(parents=True)
    (p.run_dir(NIGHT) / "done").write_text("x")
    n, _c, lines = make(home)
    assert n.run() == 0
    assert not p.sqlite.exists() and "already done" in lines[-1]
    assert NL.due(home / "data", START) == (False, NIGHT)


def test_a_live_lock_means_a_night_is_running(home):
    p = paths_of(home)
    p.root.mkdir(parents=True, exist_ok=True)
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        p.lock.write_text(json.dumps({"pid": other.pid, "started_utc": START.isoformat()}))
        n, _c, lines = make(home)
        assert n.run() == 0
        assert not p.sqlite.exists() and "another lab night is running" in lines[-1]
        assert NL.due(home / "data", START) == (False, NIGHT)
        assert NL.nightly_running(p, START)
    finally:
        other.kill()
        other.wait()
    # a dead pid's lock is removed and the night runs
    n, _c, _l = make(home, max_candidates=1)
    assert n.run() == 0 and (p.run_dir(NIGHT) / "done").exists() and not p.lock.exists()


def test_stop_before_and_between_steps_and_the_night_resumes(home, monkeypatch):
    p = paths_of(home)
    p.root.mkdir(parents=True, exist_ok=True)
    p.stop.write_text("")
    n, _c, lines = make(home)
    assert n.run() == 0 and not p.sqlite.exists() and "STOP" in lines[-1]
    p.stop.unlink()
    # STOP appears while the night is running: it stops before the next step
    lines = []

    def log(msg):
        lines.append(msg)
        if "step generate:" in msg:
            p.stop.write_text("")
    cfg = Config.load(home / "data" / "config.json")
    n = NL.Night(cfg, str(home / "data" / "config.json"), now_fn=Clock(START), log=log, no_fetch=True,
                 max_candidates=6)
    assert n.run() == 0
    state = json.loads((p.run_dir(NIGHT) / "state.json").read_text())
    assert set(state["steps"]) == {"universe", "fetch", "cut", "features", "practice", "generate"}
    assert not (p.run_dir(NIGHT) / "done").exists() and not p.lock.exists()
    led = db(home)
    committed = led.n_tests()
    assert committed == 6 and {r[0] for r in led.query("SELECT status FROM tests")} == {"running"}
    led.close()
    # STOP removed: the same night resumes from the past tests, counting nothing twice
    p.stop.unlink()
    n, _c, lines = make(home, max_candidates=6)
    assert n.run() == 0
    assert any("step generate: already done tonight" in ln for ln in lines)
    led = db(home)
    assert led.n_tests() == committed and {r[0] for r in led.query("SELECT status FROM tests")} == {"done"}
    led.close()
    assert (p.run_dir(NIGHT) / "done").exists()


def test_a_night_cut_short_resumes_with_nothing_counted_twice(home, monkeypatch):
    clock = Clock(START)
    calls = fake_runs(monkeypatch, clock, 1.0)
    real = ST.run_past

    def run_past(*a, **k):
        if len(calls) >= 5:
            raise KeyboardInterrupt
        return real(*a, **k)

    monkeypatch.setattr(ST, "run_past", run_past)
    n, _c, _l = make(home, clock, max_candidates=9)
    with pytest.raises(KeyboardInterrupt):
        n.run()
    p = paths_of(home)
    assert not (p.run_dir(NIGHT) / "done").exists() and not p.lock.exists()
    led = db(home)
    assert led.n_tests() == 9
    assert sorted(r[0] for r in led.query("SELECT status FROM tests")) == ["done"] * 5 + ["running"] * 4
    assert led.one("SELECT step FROM runs WHERE date=?", (NIGHT,))[0] == "interrupted"
    led.close()
    monkeypatch.setattr(ST, "run_past", real)
    n, _c, lines = make(home, clock, max_candidates=9)
    assert n.run() == 0
    led = db(home)
    assert led.n_tests() == 9 and {r[0] for r in led.query("SELECT status FROM tests")} == {"done"}
    assert len(set(calls)) == 9 and len(calls) == 9                   # each idea simulated once
    led.close()


def test_less_than_a_gigabyte_free_runs_nothing(home):
    n, _c, lines = make(home, disk_free=lambda _p: 200 * 1024 ** 2)
    assert n.run() == 1
    assert "GB free" in lines[-1] and not paths_of(home).sqlite.exists()


def test_the_lab_switched_off_runs_nothing(home):
    (home / "data" / "lab.json").write_text(json.dumps({"mode": "OFF"}))
    n, _c, lines = make(home)
    assert n.run() == 0 and "paused" in lines[-1] and not paths_of(home).sqlite.exists()


# ------------------------------------------------------------ time and size ---
def test_the_deadline_stops_the_past_tests_and_still_writes_the_board(home, monkeypatch):
    clock = Clock(START)
    calls = fake_runs(monkeypatch, clock, 3600.0)                     # an hour an idea (made up)
    n, _c, lines = make(home, clock, upload=True, token_fn=lambda _p: "tok",
                        upload_fn=lambda files, *a, **k: len(files))
    assert n.run() == 0
    assert len(calls) == 7                                           # 23:10 + 7 hours passes 06:00
    p = paths_of(home)
    state = json.loads((p.run_dir(NIGHT) / "state.json").read_text())
    assert state["steps"]["past"]["note"].startswith("ran out of time at 06:00: 7 of ")
    assert state["steps"]["past"]["note"].endswith("tested; the rest tonight")
    for step in ("fresh", "board", "upload"):
        assert state["steps"][step]["done"], step
    summary = json.loads(p.summary.read_text())
    assert summary["nightly"]["deadline_hit"] is True and summary["nightly"]["tested"] == 7
    assert (p.run_dir(NIGHT) / "done").exists()
    led = db(home)
    assert led.one("SELECT deadline_hit FROM runs WHERE date=?", (NIGHT,))[0] == 1
    left = led.one("SELECT COUNT(*) FROM tests WHERE status='running'")[0]
    assert left > 0
    led.close()
    # the next night picks up the rest (the rows left running are revived, never duplicated)
    before = len(calls)
    nxt = Clock(START + dt.timedelta(days=1))
    calls2 = fake_runs(monkeypatch, nxt, 1.0)
    n, _c, _l = make(home, nxt, max_candidates=0)
    assert n.run() == 0
    led = db(home)
    assert led.one("SELECT COUNT(*) FROM tests WHERE status!='done'")[0] == 0
    assert len(calls2) == left and before == 7
    led.close()


def test_the_batch_is_sized_from_the_first_ten_measured(home, monkeypatch):
    clock = Clock(START)
    calls = fake_runs(monkeypatch, clock, 1000.0)
    n, _c, lines = make(home, clock)
    assert n.run() == 0
    # after ten (10,000 s), 14,600 s are left to 06:00: 60% of that at 1,000 s an idea is 8 more
    assert len(calls) == 18
    assert any("1000.00 s per idea measured" in ln for ln in lines)
    led = db(home)
    run = led.one("SELECT * FROM runs WHERE date=?", (NIGHT,))
    assert run["seconds_per_candidate"] == 1000.0 and run["deadline_hit"] == 0
    assert "the night's measured batch" in run["note"]
    led.close()


# ---------------------------------------------------------- prices and feed ---
class FakeFeed:
    """A MADE-UP feed that records every question."""

    def __init__(self, ping):
        self._ping = ping
        self.calls = []

    def ping(self):
        self.calls.append("ping")
        return self._ping

    def bars(self, *a, **k):
        self.calls.append("bars")
        return []

    def symbols(self):
        self.calls.append("symbols")
        return ()

    def spec(self, s):
        self.calls.append("spec")
        return None


def test_no_fetch_asks_the_feed_nothing(home):
    feeds = []
    n, _c, _l = make(home, max_candidates=0, feed_factory=lambda: feeds.append(FakeFeed({})) or feeds[-1])
    assert n.run() == 0 and feeds == []


def test_nothing_is_fetched_while_metatrader_is_not_connected(home):
    feed = FakeFeed({"ok": True, "mt5_connected": False})
    n, _c, _l = make(home, max_candidates=0, no_fetch=False, feed_factory=lambda: feed)
    assert n.run() == 0
    state = json.loads((paths_of(home).run_dir(NIGHT) / "state.json").read_text())
    assert state["steps"]["fetch"]["note"] == "skipped: MetaTrader is not connected"
    assert feed.calls == ["ping"]


def test_the_first_night_chooses_the_markets_from_the_feed(tmp_path):
    """No universe yet: step 1 asks the feed (a SimBroker: made-up markets)."""
    from mintel.broker.sim import SimBroker
    broker = SimBroker(["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "GER40"], seed=3,
                       start=START, history_bars=50)
    broker.connect()

    class Feed:
        synthetic = True

        def ping(self):
            return {"ok": True, "mt5_connected": True, "backend": "sim"}

        def symbols(self):
            return broker.symbols()

        def spec(self, s):
            return broker.spec(s)

    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    cfg.ops.log_dir = str(tmp_path / "logs")
    n = NL.Night(cfg, str(tmp_path / "data" / "config.json"), now_fn=Clock(START), log=lambda m: None,
                 no_fetch=True, max_candidates=0, feed_factory=Feed)
    assert n.run() == 0
    uni = json.loads(LabPaths(tmp_path / "data").universe.read_text())
    assert {s["name"] for s in uni["symbols"]} == {"EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "GER40"}
    summary = json.loads(LabPaths(tmp_path / "data").summary.read_text())
    assert summary["store"]["ready"] is False and summary["store"]["complete_days"] == 0
    # a second night with MetaTrader away still reads the file it chose
    n = NL.Night(cfg, str(tmp_path / "data" / "config.json"), now_fn=Clock(START + dt.timedelta(days=1)),
                 log=lambda m: None, no_fetch=True, feed_factory=lambda: FakeFeed(None))
    assert n.run() == 0


def test_without_a_universe_and_without_metatrader_the_night_says_so(tmp_path):
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    lines = []
    n = NL.Night(cfg, str(tmp_path / "c.json"), now_fn=Clock(START), log=lines.append, no_fetch=True,
                 feed_factory=lambda: FakeFeed(None))
    assert n.run() == 0
    assert any("no universe yet: the price feed is not connected" in ln for ln in lines)
    summary = json.loads(LabPaths(tmp_path / "data").summary.read_text())
    assert summary["store"]["ready"] is False and summary["candidates"] == []


# ------------------------------------------------------------------ upload ---
def test_the_upload_goes_to_reports_lab_with_the_uploaders_own_token(home, monkeypatch):
    seen = {}

    def upload(files, repo, branch, token, **kw):
        seen.update(files=files, repo=repo, branch=branch, token=token, kw=kw)
        return len(files)

    import mintel.ops.report_upload as RU
    asked = []
    monkeypatch.setattr(RU, "read_token", lambda path: asked.append(path) or "made-up-token")
    n, _c, lines = make(home, max_candidates=2, upload=True, upload_fn=upload)
    assert n.run() == 0
    assert asked == [str(home / "data" / "config.json")]                # the uploader's reader, nothing else
    assert seen["token"] == "made-up-token" and seen["kw"] == {"day": f"lab/{NIGHT}"}
    assert seen["repo"] == Config().report_repo and seen["branch"] == Config().report_branch
    assert sorted(seen["files"]) == [f"reports/lab/{NIGHT}/report.md", f"reports/lab/{NIGHT}/summary.json",
                                     f"reports/lab/{NIGHT}/tests.csv"]
    assert json.loads(seen["files"][f"reports/lab/{NIGHT}/summary.json"])["synthetic"] is True


def test_no_token_means_not_published_and_the_night_still_finishes(home, monkeypatch):
    import mintel.ops.report_upload as RU
    monkeypatch.setattr(RU, "read_token", lambda path: "")
    called = []
    n, _c, _l = make(home, max_candidates=0, upload=True, upload_fn=lambda *a, **k: called.append(1))
    assert n.run() == 0 and called == []
    state = json.loads((paths_of(home).run_dir(NIGHT) / "state.json").read_text())
    assert state["steps"]["upload"]["note"] == "not published: no GitHub token saved"
    assert (paths_of(home).run_dir(NIGHT) / "done").exists()


def test_the_command_line_runs_the_same_night(home):
    """``python -m mintel.lab.nightly`` on the made-up store (a real process, no upload; at any hour of
    the day the test runs, which the lab itself refuses on a weekday between 06:00 and 23:10)."""
    env = {**os.environ, "PYTHONHASHSEED": "0", "MINTEL_LAB_ANY_HOUR": "1"}
    r = subprocess.run([sys.executable, "-m", "mintel.lab.nightly", "--config", str(home / "data" / "config.json"),
                        "--no-fetch", "--max-candidates", "0", "--workers", "1", "--deadline", "06:00"],
                       capture_output=True, text=True, env=env, timeout=300,
                       cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    assert r.returncode == 0, r.stderr
    assert "step board:" in r.stdout and "is done" in r.stdout
    r = subprocess.run([sys.executable, "-m", "mintel.lab", "show", "--one-line", "--config",
                        str(home / "data" / "config.json")], capture_output=True, text=True, timeout=120,
                       cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    assert r.returncode == 0 and r.stdout.strip() == "The lab: TEST DATA - not a result."


def test_a_bad_deadline_is_refused():
    assert NL.main(["--config", "/nonexistent/config.json", "--deadline", "25:99"]) == 2


def test_the_deadline_is_the_next_six_in_the_morning_in_london():
    assert NL._london_deadline(START, "06:00") == dt.datetime(2026, 10, 13, 5, 0, tzinfo=UTC)       # BST
    winter = dt.datetime(2026, 11, 2, 23, 10, tzinfo=UTC)
    assert NL._london_deadline(winter, "06:00") == dt.datetime(2026, 11, 3, 6, 0, tzinfo=UTC)       # GMT
    early = dt.datetime(2026, 10, 13, 1, 0, tzinfo=UTC)                                            # 02:00 BST
    assert NL._london_deadline(early, "06:00") == dt.datetime(2026, 10, 13, 5, 0, tzinfo=UTC)


# ------------------------------------------------------------------ the board ---
def _board_world(tmp_path):
    """A MADE-UP ledger with one idea at each listed stage."""
    from tests import test_lab_stages as TS
    from mintel.lab.ledger import LabLedger
    led = LabLedger(tmp_path / "data" / "lab" / "lab.sqlite")
    bench, prac, ready = TS.grid_of("PREV_DAY_LEVELS")[:2] + TS.grid_of("CONTROL_TIME_OF_DAY")[:1]
    lost = TS.grid_of("CONTROL_TIME_OF_DAY")[1]
    TS._practising(led, prac)
    TS._practising(led, ready)
    TS._bench(led, [(bench, 0.1, 0.02)])
    for d in TS.PRACTICE_DAYS[:10]:
        TS.add_trades(led, prac.id, d, [3.0, -1.0])
        TS.add_trades(led, ready.id, d, [3.0, 3.0, 2.0, -1.0])
    TS.ST.grade_practice(led, [], TS.LabSettings(), now=TS.night(TS.PRACTICE_DAYS[9]))
    TS._ready_for_fresh(led, [])
    TS.past_done(led, lost, TS.good_cols(), stage="PAST TEST")
    TS.gate(led, lost, plateau="na")
    tid = led.claim_fresh(lost.id, "F-2026-10", TS.CUT, 30, now=TS.NOW)
    led.finish_test(tid, {"trades": 31, "net_gbp": -44.0, "passed": 0, "p": 0.6, "pf": 0.8, "days": 20},
                    {}, reasons=["LOST"], skipped={}, synthetic=False, seconds=0.1, now=TS.NOW)
    led.move(lost.id, "FRESH TEST", "its one look", now=TS.NOW)
    led.move(lost.id, "BINNED", "failed the fresh test", bin_reason="Failed the fresh test (Steady): made up",
             now=TS.night(TS.PRACTICE_DAYS[9]))
    return led, {"bench": bench, "prac": prac, "ready": ready, "lost": lost}


def test_the_board_says_each_stage_in_the_exact_words(tmp_path):
    from mintel.lab import LabSettings
    from mintel.lab import report as R
    from tests import test_lab_stages as TS
    led, w = _board_world(tmp_path)
    when = TS.night(TS.PRACTICE_DAYS[9]) + dt.timedelta(hours=1)
    sm = R.build_summary(led, LabPaths(tmp_path / "data"), settings=LabSettings(), universe=[], now=when)
    by = {c["id"]: c for c in sm["candidates"]}
    assert by[w["ready"].id]["stage"] == "READY FOR LIVE" and by[w["ready"].id]["ready"] is True
    assert by[w["ready"].id]["headline"] == ("Ready for a live trial: 40 practice trades over 10 days, +£70 after "
                                            "costs, 10 days up, 0 down, every check passed.")
    assert by[w["ready"].id]["what_to_do"] == ("Your call: say the word and it goes live at £13 a trade, judged "
                                              "after 20 live trades - or leave it practising.")
    assert by[w["ready"].id]["chip"] == {"code": "YOUR CALL", "colour": "sand"}
    p = by[w["prac"].id]
    assert p["stage"] == "PRACTICE"
    assert p["headline"] == "Practising for 10 days: +£20 after costs over 20 practice trades, 10 days up, 0 down."
    assert p["what_to_do"] == "Watch - judged on 26 Oct after 10 trading days and 30+ trades (10 to go)."
    assert [c["state"] for c in p["checks"]] == ["Running", "Running", "Passed", "Running", "Running"]
    assert p["checks"][4]["p"] is not None                          # the luck figure is worked out
    assert p["figures"]["trades"] == 20 and p["figures"]["last10"]["trades"] == 20
    assert p["figures"]["last10"]["from"] == "2026-10-13" and p["figures"]["last10"]["to"] == "2026-10-26"
    assert len(p["practice_trades"]) == 20 and p["practice"]["look1_due"] == "2026-10-26"
    b = by[w["bench"].id]
    assert b["headline"] == "Passed both tests; waiting for a practice place (2 of 6 taken, 1st in line)."
    assert b["what_to_do"] == "Nothing - it starts practising when a place frees."
    assert b["figures"]["trades"] is None and b["practice_trades"] == []              # no record: no figure
    lost = by[w["lost"].id]
    assert lost["headline"] == ("Binned on 26 Oct: failed the fresh test (-£44 over 31 trades on prices it never "
                                "saw).")
    assert lost["what_to_do"] == "Nothing - it stays binned and is not retried."
    assert lost["chip"]["code"] == "FAILED"
    assert sm["binned_recent"][0]["id"] == w["lost"].id and sm["binned_recent"][0]["stage"] == "FRESH TEST"
    # every money figure says where it came from
    for c in sm["candidates"]:
        assert c["sources"] and all("data/lab/lab.sqlite" in s for s in c["sources"])
        if c["figures"]["practice"]:
            assert c["figures"]["source"].startswith("data/lab/lab.sqlite practice_trades")
            assert c["practice"]["source"]
        for kind in ("past", "fresh"):
            if c["tests"][kind]:
                assert c["tests"][kind]["source"].startswith("data/lab/lab.sqlite tests ")
    assert sm["trials"]["source"] and sm["store"]["source"]
    assert sm["synthetic"] is True                                   # some made-up test rows are in the ledger
    led.close()


def test_ideas_binned_on_old_prices_are_counted_not_listed(tmp_path):
    from mintel.lab import LabSettings
    from mintel.lab import report as R
    from tests import test_lab_stages as TS
    from mintel.lab.ledger import LabLedger
    led = LabLedger(tmp_path / "data" / "lab" / "lab.sqlite")
    for c in TS.grid_of("CONTROL_TIME_OF_DAY")[:5]:
        TS.past_done(led, c, TS.bad_cols(), stage="PAST TEST")
        led.move(c.id, "BINNED", "made up", now=TS.NOW)
    sm = R.build_summary(led, LabPaths(tmp_path / "data"), settings=LabSettings(), universe=[],
                         now=TS.NOW + dt.timedelta(hours=1))
    assert sm["candidates"] == [] and sm["binned_recent"] == [] and sm["funnel"]["binned"] == 5
    assert sm["changes_24h"][0]["text"] == "0 ideas tried on past prices; 5 failed and were binned."
    led.close()


# ------------------------------------------------------------------ commands ---
def test_mode_is_the_only_writer_and_reopen_is_logged_as_claudes(tmp_path, capsys):
    from mintel.lab.__main__ import main
    from mintel.lab.ledger import LabLedger
    from mintel.lab.candidate import registered
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    cfg.save(tmp_path / "data" / "config.json")
    conf = str(tmp_path / "data" / "config.json")
    assert main(["mode", "off", "--config", conf]) == 0
    assert json.loads((tmp_path / "data" / "lab.json").read_text())["mode"] == "OFF"
    assert main(["mode", "paper", "--config", conf]) == 0
    assert json.loads((tmp_path / "data" / "lab.json").read_text())["mode"] == "PAPER"
    assert main(["mode", "show", "--config", conf]) == 0
    assert "The lab is PAPER" in capsys.readouterr().out
    led = LabLedger(LabPaths(tmp_path / "data").sqlite)
    c = registered()[0]
    led.register(c, "made up")
    led.move(c.id, "BINNED", "made up")
    led.close()
    assert main(["reopen", c.id, "--reason", "the grid was wrong (made up)", "--config", conf]) == 0
    led = LabLedger(LabPaths(tmp_path / "data").sqlite)
    ev = led.one("SELECT * FROM events WHERE kind='reopen'")
    assert ev["by"] == "claude" and ev["why"] == "the grid was wrong (made up)" and ev["to_stage"] == "IDEA"
    assert led.candidate(c.id)["retest_due"] == 1
    led.close()
    assert main(["reopen", "lab-0000000000", "--reason", "x", "--config", conf]) == 2
    assert main(["show", "--config", conf]) == 0
    assert "The lab:" in capsys.readouterr().out
