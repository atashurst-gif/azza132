"""Version 4 (8 Oct): the switch-off by kind of market, the day bound on the
nightly rows, the Band Breaker's check log, the scalper's look counters."""
import datetime as dt

from mintel.config import Config, STRATEGY_NAME
from mintel.engine.tactics import blocked_in_group


class TestRules:
    def test_version_4_names_and_lists(self):
        c = Config()
        # version 5 (9 Oct) kept every version 4 rule and added to them
        assert STRATEGY_NAME in ("Commission First (version 4)", "Commission First (version 5)")
        assert "USDCHF" in c.universe.excluded_symbols and "XAUJPY" in c.universe.excluded_symbols
        assert ("MOMENTUM_CONTINUATION", "GOLD") in c.scan.disabled_tactic_groups
        assert ("MOMENTUM_CONTINUATION", "SILVER") in c.scan.disabled_tactic_groups

    def test_momentum_is_off_on_gold_and_silver_and_on_elsewhere(self):
        groups = Config().scan.disabled_tactic_groups
        assert blocked_in_group("MOMENTUM_CONTINUATION", "GOLD", groups)
        assert blocked_in_group("MOMENTUM_CONTINUATION", "silver", groups)
        assert not blocked_in_group("MOMENTUM_CONTINUATION", "INDEX", groups)
        # FX majors: off from version 5 (9 Oct, tests/test_version5.py); the minors stay on
        assert not blocked_in_group("MOMENTUM_CONTINUATION", "FX_MINOR", groups)
        assert not blocked_in_group("SESSION_EXPANSION", "GOLD", groups)

    def test_the_crosses_are_grouped_as_gold_or_metal(self):
        from mintel.contracts import infer_group
        assert infer_group("XAUCHF") == "GOLD" and infer_group("XAUUSD") == "GOLD"
        assert infer_group("XAGEUR") == "SILVER" and infer_group("XAGUSD") == "SILVER"
        assert infer_group("US30") == "INDEX"

    def test_a_file_can_carry_the_group_switch_offs(self, tmp_path):
        import json
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"scan": {"disabled_tactic_groups": [["SESSION_EXPANSION", "FX_MAJOR"]]}}))
        c = Config.load(p)
        assert tuple(map(tuple, c.scan.disabled_tactic_groups)) == (("SESSION_EXPANSION", "FX_MAJOR"),)


class TestDayBound:
    def test_the_nightly_rows_stop_at_the_end_of_the_day(self, tmp_path):
        from mintel.engine.journal import Journal
        from mintel.ops.day_review import journal_rows_for
        cfg = Config(); cfg.ops.data_dir = str(tmp_path)
        j = Journal(tmp_path / "journal.sqlite")
        for ticket, closed in ((1, "2026-10-07T12:00:00+00:00"), (2, "2026-10-08T06:43:00+00:00")):
            j._exec("INSERT INTO trades (ticket, symbol, opened_utc, closed_utc, pnl_money) VALUES (?,?,?,?,?)",
                    (ticket, "US30", closed, closed, 1.0))
        j.close()
        day = dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc)
        assert [r["ticket"] for r in journal_rows_for(cfg, day)] == [1]


class TestBandBreakerChecks:
    def test_every_half_hour_check_is_recorded_with_its_decision(self, tmp_path):
        from tests.test_band_breaker import TestTheTrade, engine, at, TODAY
        e = engine(tmp_path)
        t = TestTheTrade()
        today = [("09:30", 5000, 5012, 4995, 5010), ("10:00", 5010, 5015, 5005, 5012)]
        t._run(e, at(TODAY, 10, 30), 5012.0, today, (10, 30))
        rows = e.checks_since(at(TODAY, 0, 0))
        assert len(rows) == 1 and rows[0]["check_ny"] == "10:30" and "inside the band" in rows[0]["decision"]
        assert rows[0]["band_upper"] > rows[0]["close"] > rows[0]["band_lower"]
        up = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        e2 = engine(tmp_path / "b")
        t._run(e2, at(TODAY, 10, 30), 5030.0, up, (10, 30))
        rows = e2.checks_since(at(TODAY, 0, 0))
        assert rows[-1]["decision"] == "entered buy" and e2.entries_today[("US500", TODAY.isoformat())] == 1
        st = e2.status(at(TODAY, 10, 31))
        assert st["checks_today"] and "US500" in st["bands_today"]


class TestScalperLooks:
    def test_the_tally_separates_quiet_from_strict(self, tmp_path):
        from mintel.scalper.score import Opportunity
        from tests.test_velocity_scalper import TestInsideTheEngine
        e, b = TestInsideTheEngine()._engine(tmp_path)
        now = b.now
        e._count_look("EURUSD", now, Opportunity("EURUSD", 0, 0.0, blockers=["no acceleration"]))
        e._count_look("EURUSD", now, Opportunity("EURUSD", 1, 55.0, blockers=["momentum 55/100 below 70 for this window"]))
        e._count_look("EURUSD", now, Opportunity("EURUSD", 1, 80.0))
        c = e.looks["EURUSD"]
        assert c["passes"] == 3 and c["with_direction"] == 2 and c["tradable"] == 1
        assert c["blockers"] == {"momentum below the bar": 1} and c["best"] == 80.0
        assert e.status(now, 0.5)["looks_today"]["EURUSD"]["passes"] == 3
