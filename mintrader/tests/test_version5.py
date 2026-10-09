"""Version 5 (9 Oct): the top-opportunity size, a calibration one broken
trade cannot poison, momentum continuation off on the FX majors, and the
Momentum Runner's skip list and confirmed entry. Every price here is
synthetic test data."""
from __future__ import annotations

import datetime as dt
import inspect
import json
import math

import pytest

from mintel.broker.base import AccountInfo, Bar, Position, Side, Tick
from mintel.config import Config, STRATEGY_NAME, STRATEGY_VERSION
from mintel.contracts import SymbolSpec
from mintel.data.regime import Regime
from mintel.engine.evidence import Evidence, Family, MarketState
from mintel.engine.journal import Journal, clip_r, planned_r
from mintel.engine.opportunity import Calibrator
from mintel.engine.risk import RiskManager

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC)


# ------------------------------------------------------------- helpers --
def fx_spec(name="GBPCHF", tick_value=0.93, **kw) -> SymbolSpec:
    base = dict(name=name, digits=5, point=0.00001, tick_size=0.00001, tick_value=tick_value,
                contract_size=100_000, volume_min=0.01, volume_max=100.0, volume_step=0.01,
                stops_level_points=10.0, freeze_level_points=0.0, base_currency=name[:3],
                profit_currency=name[3:6], asset_group="FX_MINOR")
    base.update(kw)
    return SymbolSpec(**base)


def index_spec(name="US30") -> SymbolSpec:
    # one point = about 0.74 GBP a lot: tick 0.01 worth 0.0074
    return SymbolSpec(name=name, digits=2, point=0.01, tick_size=0.01, tick_value=0.0074,
                      contract_size=1.0, volume_min=0.01, volume_max=500.0, volume_step=0.01,
                      stops_level_points=30.0, freeze_level_points=0.0, asset_group="INDEX")


def account(equity=2_000.0, **kw) -> AccountInfo:
    base = dict(login=1, server="S", currency="GBP", balance=equity, equity=equity, margin=0.0,
                margin_free=equity, margin_level=0.0, leverage=30, trade_allowed=True, is_demo=True)
    base.update(kw)
    return AccountInfo(**base)


def state(symbol="GBPCHF", tactic="MOMENTUM_CONTINUATION", entry=1.07000, stop=1.06800,
          tier="STRONG", opportunity=74.0, direction=1, regime=Regime.HIGH_VOL) -> MarketState:
    return MarketState(symbol=symbol, as_of=NOW, regime=regime, regime_label=regime.value,
                       direction=direction, tactic=tactic,
                       evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0)},
                       raw_score=opportunity, opportunity=opportunity, tier=tier, entry=entry, stop=stop,
                       target=entry + 3 * (entry - stop), reward_risk=2.0, headroom_pips=40.0, cost_pips=1.0)


def seed(journal: Journal, rows, start_ticket=1):
    """rows: (symbol, tactic, regime, day "2026-10-01", pnl_money, r)"""
    for i, (sym, tactic, regime, day, pnl, r) in enumerate(rows):
        entry, stop = 1.1000, 1.0980
        exit_price = entry + r * (entry - stop)
        journal._exec("""INSERT INTO trades (ticket, symbol, side, entry, stop, exit_price, opened_utc, closed_utc,
                         pnl_money, realised_r, regime, tactic, opportunity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (start_ticket + i, sym, "BUY", entry, stop, exit_price, f"{day}T08:00:00+00:00",
                       f"{day}T09:00:00+00:00", pnl, r, regime, tactic, 74.0))


def earning_record(journal, symbol="GBPCHF", tactic="MOMENTUM_CONTINUATION", days=6, per_day=4):
    """A segment that earned: each day three winners of +6 (+1.5 R) and one loser of -5 (-1 R)."""
    rows = []
    for d in range(days):
        day = (dt.date(2026, 9, 22) + dt.timedelta(days=d)).isoformat()
        for k in range(per_day):
            win = k < per_day - 1
            rows.append((symbol, tactic, "HIGH_VOL", day, 6.0 if win else -5.0, 1.5 if win else -1.0))
    seed(journal, rows)


def rm_with(tmp_path, record=earning_record, **risk) -> tuple[RiskManager, Journal]:
    cfg = Config()
    for k, v in risk.items():
        setattr(cfg.risk, k, v)
    j = Journal(tmp_path / "journal.sqlite")
    if record is not None:
        record(j)
    return RiskManager(cfg, j), j


# ------------------------------------------------------------ the rules --
class TestRules:
    def test_version_5_name_and_the_clock_is_unchanged(self):
        assert STRATEGY_NAME == "Commission First (version 5)"
        assert STRATEGY_VERSION.startswith("2026-10-05 complete reset to zero")

    def test_momentum_continuation_is_off_on_the_fx_majors_and_on_on_the_minors(self):
        from mintel.engine.tactics import blocked_in_group
        groups = Config().scan.disabled_tactic_groups
        assert blocked_in_group("MOMENTUM_CONTINUATION", "FX_MAJOR", groups)
        assert not blocked_in_group("MOMENTUM_CONTINUATION", "FX_MINOR", groups)
        assert not blocked_in_group("MOMENTUM_CONTINUATION", "INDEX", groups)
        assert not blocked_in_group("SESSION_EXPANSION", "FX_MAJOR", groups)

    def test_the_new_settings_and_their_defaults(self):
        r = Config().risk
        assert r.max_risk_money == 10.0 and r.top_risk_money == 40.0 and r.top_max_lots == 0.5
        assert r.top_size_enabled and ("MOMENTUM_CONTINUATION", "FX_MINOR") in r.top_segments
        assert (r.top_min_trades, r.top_min_days) == (20, 4)
        assert not Config().validate()
        rc = Config().runner
        assert rc.skip_tactics == ("MOMENTUM_CONTINUATION",) and rc.confirm_r == 0.0 and rc.trail_r == 3.0

    def test_a_file_can_carry_the_top_settings(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"risk": {"top_risk_money": 25.0, "top_segments": [["SESSION_EXPANSION", "INDEX"]]},
                                 "runner": {"skip_tactics": [], "confirm_r": 1.0}}))
        c = Config.load(p)
        assert c.risk.top_risk_money == 25.0 and c.risk.max_risk_money == 10.0
        assert [tuple(x) for x in c.risk.top_segments] == [("SESSION_EXPANSION", "INDEX")]
        assert tuple(c.runner.skip_tactics) == () and c.runner.confirm_r == 1.0

    def test_the_trader_hands_the_runner_its_new_settings(self):
        from mintel.runner.shadow import RunnerConfig
        assert {"skip_tactics", "confirm_r"} <= set(RunnerConfig.__dataclass_fields__)
        assert {"skip_tactics", "confirm_r"} <= set(type(Config().runner).__dataclass_fields__)


# ------------------------------------------------------- calibration --
class TestCalibrationOutlier:
    def test_one_broken_trade_no_longer_poisons_its_band(self):
        """8 Oct: "band 76-82 (E=-136390.07R) underperforms 70-76". The same
        shape: two healthy bands, one trade in the higher one with a
        near-zero recorded risk."""
        c = Calibrator()
        rows = [(73.0, 0.3)] * 30 + [(79.0, 0.35)] * 30 + [(79.0, -136390.07)]
        c.load(rows)
        e = c.expected_r(79.0)
        assert e is not None and -0.2 < e < 0.4                  # bounded: held to -2 R at worst
        assert c.monotonic_violation() is None
        assert c.clipped == 1 and c.ignored == 0
        # what the old arithmetic gave, for the record
        assert (0.35 * 30 - 136390.07) / 31 < -4000

    def test_not_a_number_is_not_counted(self):
        c = Calibrator()
        c.load([(75.0, float("nan")), (75.0, float("inf")), (75.0, "x"), (float("nan"), 1.0), (75.0, 1.0)])
        assert c.ignored == 4 and c.report()[4]["trades"] == 1          # the 70-76 band

    def test_huge_wins_are_held_too(self):
        c = Calibrator(min_samples=5)
        c.load([(85.0, 1e9)] + [(85.0, 0.5)] * 9)
        assert c.expected_r(85.0) == pytest.approx((6.0 + 4.5) / 10)

    def test_a_band_needs_its_minimum_before_its_average_is_believed(self):
        c = Calibrator(min_band_samples=10)
        c.load([(75.0, 1.0)] * 9)
        assert c.expected_r(75.0) is None and c.win_probability(75.0) is None
        c.observe(75.0, 1.0)
        assert c.expected_r(75.0) == pytest.approx(1.0)

    def test_confidence_stays_inside_zero_and_one_with_an_outlier(self):
        c = Calibrator(min_samples=20)
        c.load([(79.0, 0.4)] * 25 + [(79.0, -136390.07)])
        assert 0.0 <= c.calibrated_confidence(79.0) <= 1.0
        assert c.calibrated_confidence(79.0) > 0.3               # the healthy trades still count


class TestJournalR:
    def test_planned_r_reads_the_stop_the_trade_opened_with(self):
        assert planned_r("BUY", 1.1000, 1.0980, 1.0980) == (True, pytest.approx(-1.0))
        assert planned_r("SELL", 1.1000, 1.1020, 1.0960) == (True, pytest.approx(2.0))
        assert planned_r("BUY", 1.1000, 1.1000, 1.0990)[0] is False          # no risk at all
        assert planned_r("BUY", 1.1000, 1.1005, 1.0990)[0] is False          # a stop on the profit side
        assert planned_r("BUY", 1.1000, 0.0, 1.0990) == (True, None)          # unknown: the stored R is used

    def test_calibration_rows_use_the_planned_stop_and_drop_a_zero_risk_row(self, tmp_path):
        j = Journal(tmp_path / "journal.sqlite")
        # the tracker was rebuilt with the stop at the entry: stored R is absurd, the planned stop says -1 R
        j._exec("""INSERT INTO trades (ticket, symbol, side, entry, stop, exit_price, closed_utc, realised_r, opportunity)
                   VALUES (1, 'US30', 'BUY', 50000, 49975, 49975, '2026-10-08T10:00:00+00:00', -136390.07, 79.0)""")
        # no planned risk at all: left out
        j._exec("""INSERT INTO trades (ticket, symbol, side, entry, stop, exit_price, closed_utc, realised_r, opportunity)
                   VALUES (2, 'US30', 'BUY', 50000, 50000, 50010, '2026-10-08T11:00:00+00:00', 99999.0, 79.0)""")
        # no stop recorded: the stored R, which the calibrator then holds to range
        j._exec("""INSERT INTO trades (ticket, symbol, side, entry, stop, exit_price, closed_utc, realised_r, opportunity)
                   VALUES (3, 'US30', 'BUY', 50000, NULL, 50010, '2026-10-08T12:00:00+00:00', 0.8, 72.0)""")
        rows = sorted(j.calibration_rows())
        assert rows == [(72.0, 0.8), (79.0, pytest.approx(-1.0))]

    def test_kelly_and_the_historical_match_hold_r_to_range(self, tmp_path):
        j = Journal(tmp_path / "journal.sqlite")
        for i in range(12):
            r = 1.0 if i % 2 else -1.0
            j._exec("""INSERT INTO trades (ticket, symbol, side, tactic, regime, closed_utc, realised_r)
                       VALUES (?, 'US30', 'BUY', 'MC', 'HIGH_VOL', '2026-10-08T10:00:00+00:00', ?)""", (i + 1, r))
        j._exec("""INSERT INTO trades (ticket, symbol, side, tactic, regime, closed_utc, realised_r)
                   VALUES (99, 'US30', 'BUY', 'MC', 'HIGH_VOL', '2026-10-08T10:00:00+00:00', -136390.07)""")
        n, win, payoff = j.kelly_stats("MC", "HIGH_VOL")
        assert n == 13 and payoff == pytest.approx(1.0 / ((6 * 1.0 + 2.0) / 7))
        m = j.match_stats("US30", "HIGH_VOL", "MC", Side.BUY)
        assert m["exact"]["n"] == 13 and m["exact"]["expectancy_r"] == pytest.approx(-2.0 / 13)
        assert clip_r(float("nan")) is None and clip_r(-50) == -2.0 and clip_r(50) == 6.0

    def test_a_rebuilt_tracker_takes_the_journals_opening_stop(self, tmp_path):
        """The root cause: after a restart that lost the tracker, the broker's
        stop (already pulled up to the entry) was taken as the initial stop."""
        from types import SimpleNamespace
        from mintel.engine.trader import Trader
        j = Journal(tmp_path / "journal.sqlite")
        j._exec("INSERT INTO trades (ticket, symbol, side, entry, stop) VALUES (7, 'US30', 'BUY', 50000, 49975)")
        fake = SimpleNamespace(journal=j)
        p = Position(7, "US30", Side.BUY, 0.5, 50000.0, 50000.5, 0.0, NOW)       # stop pulled up to the entry
        assert Trader._original_stop(fake, p) == 49975.0
        unknown = Position(8, "US30", Side.BUY, 0.5, 50000.0, 49980.0, 0.0, NOW)
        assert Trader._original_stop(fake, unknown) == 49980.0                    # not in the journal: the broker's
        j._exec("INSERT INTO trades (ticket, symbol, side, entry, stop) VALUES (9, 'US30', 'BUY', 50000, 50010)")
        odd = Position(9, "US30", Side.BUY, 0.5, 50000.0, 49990.0, 0.0, NOW)
        assert Trader._original_stop(fake, odd) == 49990.0                         # a nonsense record is not used
        assert Trader._original_stop(SimpleNamespace(journal=None), odd) == 49990.0


# ------------------------------------------------- top-opportunity size --
class TestWhoQualifies:
    def test_the_earning_segment_qualifies_and_says_why(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        ok, why = rm.top_opportunity(state(), "FX_MINOR", NOW)
        assert ok and why.startswith("top opportunity: momentum continuation on fx minor has earned it")
        assert "24 trades over 6 days" in why and "6 days up and 0 down" in why

    def test_the_score_is_not_what_qualifies(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        assert rm.top_opportunity(state(opportunity=70.1), "FX_MINOR", NOW)[0]
        rm2, _ = rm_with(tmp_path / "b")
        assert not rm2.top_opportunity(state(symbol="EURUSD", opportunity=95.0), "FX_MAJOR", NOW)[0]

    def test_other_approaches_and_kinds_of_market_do_not(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        assert rm.top_opportunity(state(tactic="SESSION_EXPANSION"), "FX_MINOR", NOW) == (False, "")
        assert rm.top_opportunity(state(symbol="US30"), "INDEX", NOW) == (False, "")
        assert rm.top_opportunity(state(symbol="EURUSD"), "FX_MAJOR", NOW) == (False, "")

    def test_a_short_record_is_not_enough(self, tmp_path):
        rm, _ = rm_with(tmp_path, record=lambda j: earning_record(j, days=3, per_day=4))
        ok, why = rm.top_opportunity(state(), "FX_MINOR", NOW)
        assert not ok and "too short" in why and "12 trades over 3 days" in why

    def test_a_segment_that_stopped_earning_is_sized_as_usual(self, tmp_path):
        def losing(j):
            rows = []
            for d in range(6):
                day = (dt.date(2026, 9, 22) + dt.timedelta(days=d)).isoformat()
                rows += [("GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", day, -5.0, -1.0)] * 3
                rows += [("GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", day, 6.0, 1.5)]
            seed(j, rows)
        rm, _ = rm_with(tmp_path, record=losing)
        ok, why = rm.top_opportunity(state(), "FX_MINOR", NOW)
        assert not ok and "not earning now" in why

    def test_more_losing_days_than_winning_ones_is_not_earning(self, tmp_path):
        def lumpy(j):
            rows = []
            for d in range(6):
                day = (dt.date(2026, 9, 22) + dt.timedelta(days=d)).isoformat()
                if d == 0:
                    rows += [("GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", day, 40.0, 4.0)] * 4
                else:
                    rows += [("GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", day, -2.0, -0.2)] * 4
            seed(j, rows)
        rm, _ = rm_with(tmp_path, record=lumpy)
        ok, why = rm.top_opportunity(state(), "FX_MINOR", NOW)
        assert not ok and "1 days up and 5 down" in why

    def test_trades_in_markets_and_conditions_now_off_are_not_evidence(self, tmp_path):
        def mixed(j):
            earning_record(j, days=3, per_day=4)                                     # 12 good trades
            rows = []
            for d in range(4):
                day = (dt.date(2026, 10, 1) + dt.timedelta(days=d)).isoformat()
                rows += [("CHFJPY", "MOMENTUM_CONTINUATION", "HIGH_VOL", day, 6.0, 1.5)] * 3    # an excluded market
                rows += [("AUDJPY", "MOMENTUM_CONTINUATION", "TREND", day, 6.0, 1.5)] * 3     # a condition now off
            seed(j, rows, start_ticket=100)
        rm, _ = rm_with(tmp_path, record=mixed)
        ok, why = rm.top_opportunity(state(), "FX_MINOR", NOW)
        assert not ok and "12 trades over 3 days" in why

    def test_off_switches(self, tmp_path):
        rm, _ = rm_with(tmp_path, top_size_enabled=False)
        assert rm.top_opportunity(state(), "FX_MINOR", NOW) == (False, "")
        rm, _ = rm_with(tmp_path / "b", top_risk_money=0.0)
        assert rm.top_opportunity(state(), "FX_MINOR", NOW) == (False, "")
        rm, _ = rm_with(tmp_path / "c")
        rm.cfg.aggression = "CONSERVATIVE"
        assert not rm.top_opportunity(state(), "FX_MINOR", NOW)[0]
        rm = RiskManager(Config(), None)                                               # no record, no extra size
        assert not rm.top_opportunity(state(), "FX_MINOR", NOW)[0]


class TestTheTopSize:
    def size(self, rm, st, spec, acct=None, top=True, snap=None, margin_per_lot=500.0):
        acct = acct or account()
        return rm.size(st, spec, acct, [], 0.5, snap if snap is not None else rm.snapshot(acct, [], NOW),
                       margin_per_lot, top=top)

    def test_a_top_trade_risks_up_to_the_ceiling_not_the_ten_cap(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        # 20 pips at 0.93 a pip-tenth = 186 a lot; 1.5% of 2,000 = 30 binds before 40
        r = self.size(rm, state(), fx_spec())
        assert r.ok and r.volume == pytest.approx(0.16) and 29.0 < r.risk_money <= 30.0
        assert r.risk_pct <= Config().risk.max_risk_pct + 1e-9
        assert any("top opportunity" in x for x in r.reasons)

    def test_on_a_bigger_account_top_risk_money_is_the_cap(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        r = self.size(rm, state(), fx_spec(), acct=account(4_000))       # 1.5% = 60, the cap is 40
        assert r.ok and 39.0 < r.risk_money <= 40.0 and any("capped at 40.00" in x for x in r.reasons)

    def test_the_same_trade_without_the_top_flag_keeps_the_ten_cap(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        r = self.size(rm, state(), fx_spec(), top=False)
        assert r.ok and r.risk_money <= 10.0 + 1e-9 and r.volume == pytest.approx(0.05)

    def test_fx_and_metals_carry_at_most_half_a_lot(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        tight = state(entry=1.07000, stop=1.06950)                   # 5 pips: 30 would need 0.64 lots
        r = self.size(rm, tight, fx_spec())
        assert r.ok and r.volume == pytest.approx(0.5) and r.risk_money == pytest.approx(0.5 * 46.5)
        assert any("held to 0.5 lots" in x for x in r.reasons)
        gold = SymbolSpec(name="XAUCHF", digits=2, point=0.01, tick_size=0.01, tick_value=0.93, contract_size=100,
                          volume_min=0.01, volume_max=100, volume_step=0.01, stops_level_points=10,
                          freeze_level_points=0, asset_group="GOLD")
        r = self.size(rm, state(symbol="XAUCHF", entry=3500.0, stop=3499.5), gold)   # 50 ticks = 46.5 a lot
        assert r.ok and r.volume == pytest.approx(0.5)

    def test_an_index_has_the_money_cap_but_no_lot_cap(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        # 25 points = 18.50 a lot: 30 is 1.62 lots, allowed on an index
        r = self.size(rm, state(symbol="US30", entry=50000.0, stop=49975.0), index_spec(), margin_per_lot=100.0)
        assert r.ok and r.volume == pytest.approx(1.62) and r.risk_money <= 30.0 + 1e-9
        assert not any("held to" in x for x in r.reasons)
        normal = self.size(rm, state(symbol="US30", entry=50000.0, stop=49975.0), index_spec(), top=False,
                           margin_per_lot=100.0)
        assert normal.risk_money <= 10.0 + 1e-9 and normal.volume == pytest.approx(0.54)

    def test_the_correlated_cap_still_applies(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        acct = account()
        rm.register_open_risk(11, "EURCHF", Side.BUY, 25.0)          # already short CHF, 25 at risk; the cap is 40
        snap = rm.snapshot(acct, [Position(11, "EURCHF", Side.BUY, 0.1, 0.93, 0.92, 0.0, NOW)], NOW)
        r = self.size(rm, state(), fx_spec(), acct=acct, snap=snap)
        assert r.ok and r.risk_money <= 15.0 + 1e-9 and any("correlated" in x for x in r.reasons)

    def test_the_total_exposure_cap_still_applies(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        acct = account()
        positions = []
        for t, sym in ((21, "US500"), (22, "XAUUSD"), (23, "JP225")):
            rm.register_open_risk(t, sym, Side.BUY, 24.0)             # 72 of the 80 allowed (4%)
            positions.append(Position(t, sym, Side.BUY, 0.1, 100.0, 90.0, 0.0, NOW))
        snap = rm.snapshot(acct, positions, NOW)
        r = self.size(rm, state(), fx_spec(), acct=acct, snap=snap)
        assert r.ok and r.risk_money <= 8.0 + 1e-9 and any("total exposure" in x for x in r.reasons)

    def test_the_margin_checks_still_apply(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        r = self.size(rm, state(), fx_spec(), margin_per_lot=20_000.0)   # 2,000/3 of margin: 0.03 lots
        assert r.ok and r.volume == pytest.approx(0.03) and any("margin level" in x for x in r.reasons)

    def test_the_breakers_still_stop_entries(self, tmp_path):
        rm, _ = rm_with(tmp_path)
        rm.snapshot(account(2_000), [], NOW)
        snap = rm.snapshot(account(1_930), [], NOW + dt.timedelta(hours=1))   # down 3.5%: the daily loss stop
        assert not snap.entries_allowed and any("DAILY_LOSS" in b for b in snap.blocking_reasons())
        # nothing about the top size reaches the breakers or the daily loss stop
        assert "top" not in inspect.getsource(RiskManager.snapshot)

    def test_the_ceiling_is_never_exceeded_whatever_the_top_money(self, tmp_path):
        for equity in (500.0, 2_000.0, 10_000.0):
            for money in (5.0, 40.0, 1_000.0):
                rm, _ = rm_with(tmp_path / f"{equity}-{money}", top_risk_money=money)
                r = self.size(rm, state(), fx_spec(), acct=account(equity))
                assert r.risk_money <= equity * rm.cfg.risk.max_risk_pct / 100 + 1e-9
                assert r.risk_money <= money + 1e-9
                assert r.volume <= 0.5 + 1e-9

    def test_no_martingale_the_top_size_has_no_loss_input(self, tmp_path):
        params = set(inspect.signature(RiskManager.top_opportunity).parameters)
        assert params == {"self", "state", "kind", "now"}


class TestInsideTheTrader:
    def _trader(self, tmp_path, record=earning_record):
        from mintel.broker.sim import SimBroker
        from mintel.engine.trader import Trader
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        b = SimBroker(["EURGBP", "EURUSD"], start=NOW - dt.timedelta(days=1), history_bars=400, balance=2_000.0)
        b.connect()
        tr = Trader(b, cfg, enable_model=False, dry_run=True, clock=lambda: NOW)
        if record is not None:
            record(tr.journal)
        tr.scanner.timing_confirmation = lambda sym, direction: (True, "")
        return tr, b

    def _state(self, b, symbol):
        t = b.tick(symbol)
        return state(symbol=symbol, entry=t.ask, stop=round(t.ask - 0.0020, 5))

    def test_a_top_trade_is_sized_up_and_a_normal_one_is_not(self, tmp_path):
        tr, b = self._trader(tmp_path, record=lambda j: earning_record(j, symbol="EURGBP"))
        acct = b.account()
        snap = tr.risk.snapshot(acct, [], NOW)
        tr.act([self._state(b, "EURGBP")], acct, snap, [], NOW)
        top = tr.would_have_traded[-1]
        assert top["top_opportunity"] is True and 20.0 < top["risk_money"] <= 30.0
        tr.last_entry_utc = None
        tr.act([self._state(b, "EURUSD")], acct, snap, [], NOW)
        normal = tr.would_have_traded[-1]
        assert normal["symbol"] == "EURUSD" and normal["top_opportunity"] is False and normal["risk_money"] <= 10.0

    def test_without_a_record_the_trader_sizes_as_before(self, tmp_path):
        tr, b = self._trader(tmp_path, record=None)
        acct = b.account()
        tr.act([self._state(b, "EURGBP")], acct, tr.risk.snapshot(acct, [], NOW), [], NOW)
        t = tr.would_have_traded[-1]
        assert t["top_opportunity"] is False and t["risk_money"] <= 10.0


# --------------------------------------------------- the Momentum Runner --
class TestRunnerSkipsMomentum:
    def test_momentum_continuation_is_not_ridden_and_says_so_once(self, tmp_path):
        from tests.test_momentum_runner import pos, runner, tick, T0
        r = runner(tmp_path)
        notes = r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50000.0), T0,
                          tactics={1: "MOMENTUM_CONTINUATION"})
        assert not r.open and any("momentum continuation is not ridden" in n for n in notes)
        assert r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50000.0), T0,
                         tactics={1: "MOMENTUM_CONTINUATION"}) == []
        assert r.db.execute("SELECT COUNT(*) FROM skipped").fetchone()[0] == 0
        assert r.status()["skip_tactics"] == ["MOMENTUM_CONTINUATION"]

    def test_other_approaches_and_unknown_ones_are_ridden(self, tmp_path):
        from tests.test_momentum_runner import pos, runner, T0
        r = runner(tmp_path)
        assert r.adopt(pos(ticket=1), 49990.0, T0, tactic="SESSION_EXPANSION") is not None
        assert r.adopt(pos(ticket=2), 49990.0, T0, tactic="SQUEEZE_RELEASE") is not None
        assert r.adopt(pos(ticket=3), 49990.0, T0, tactic="") is not None
        assert r.adopt(pos(ticket=4), 49990.0, T0, tactic="MOMENTUM_CONTINUATION_2X") is None

    def test_an_empty_list_rides_everything_as_before(self, tmp_path):
        from tests.test_momentum_runner import pos, runner, T0
        r = runner(tmp_path, skip_tactics=())
        assert r.adopt(pos(), 49990.0, T0, tactic="MOMENTUM_CONTINUATION") is not None

    def test_live_copies_the_real_volume_and_never_more(self, tmp_path):
        from mintel.botexec import MAGIC_RUNNER
        from tests.test_momentum_runner import T0, live_runner, sim
        b = sim()
        r = live_runner(tmp_path, b)
        t = b.tick("US500")
        for ticket, vol in ((1, 0.5), (2, 1.37)):
            p = Position(ticket, "US500", Side.BUY, vol, t.ask, t.ask - 30.0, 0.0, T0, magic=Config().magic)
            r.observe([p], {ticket: t.ask - 30.0}, b.tick, T0, tactics={ticket: "SESSION_EXPANSION"})
        sent = [x["req"].volume for x in b.order_log]
        assert sent == [0.5, 1.37]
        assert sorted(p.volume for p in b.positions(MAGIC_RUNNER)) == [0.5, 1.37]

    def test_live_sends_nothing_for_momentum(self, tmp_path):
        from tests.test_momentum_runner import T0, live_runner, real_trade, sim
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0, tactics={1: "MOMENTUM_CONTINUATION"})
        assert not b.order_log and not r.open


class TestRunnerConfirmedEntry:
    """confirm_r: enter only once the real trade has proved itself. NOT YET
    EVALUATED on real prices; these tests pin the mechanics only."""

    def test_paper_waits_for_the_mark_then_enters_at_the_touch_one_r_behind(self, tmp_path):
        from tests.test_momentum_runner import pos, runner, tick, T0
        r = runner(tmp_path, confirm_r=1.0)
        tac = {1: "SESSION_EXPANSION"}
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50003.0), T0, tactics=tac)       # ask 50005: +0.5 R
        assert not r.open
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50009.0), T0 + dt.timedelta(minutes=30), tactics=tac)
        s = r.open[1]                                                        # ask 50011: +1.1 R
        assert s.entry == 50011.0 and s.initial_stop == 50001.0 and s.risk_distance == pytest.approx(10.0)
        assert s.opened == T0 + dt.timedelta(minutes=30) and s.volume == 1.0
        assert r.status()["confirm_r"] == 1.0

    def test_past_the_zone_it_waits_rather_than_chasing(self, tmp_path):
        from tests.test_momentum_runner import pos, runner, tick, T0
        r = runner(tmp_path, confirm_r=1.0)
        notes = r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50020.0), T0, tactics={1: "SESSION_EXPANSION"})
        assert not r.open and any("waiting" in n for n in notes)
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50010.0), T0, tactics={1: "SESSION_EXPANSION"})
        assert 1 in r.open and r.open[1].entry == 50012.0

    def test_live_enters_late_trades_at_the_mark_with_its_own_stop(self, tmp_path):
        from mintel.botexec import MAGIC_RUNNER
        from tests.test_momentum_runner import T0, live_runner, minutes, sim
        b = sim()
        r = live_runner(tmp_path, b, confirm_r=1.0)
        t = b.tick("US500")
        p = Position(1, "US500", Side.BUY, 0.5, t.ask, t.ask - 30.0, 0.0, T0, magic=Config().magic)
        r.observe([p], {1: t.ask - 30.0}, b.tick, minutes(20), tactics={1: "SESSION_EXPANSION"})
        assert not b.order_log                                              # not proved yet
        b.push_bar("US500", t.ask + 33.0, high=t.ask + 34.0, low=t.ask + 20.0)    # about +1.1 R
        now = b.tick("US500")
        r.observe([p], {1: t.ask - 30.0}, b.tick, minutes(25), tactics={1: "SESSION_EXPANSION"})
        assert len(b.order_log) == 1                                        # 25 minutes on: not "too late" here
        mine = b.positions(MAGIC_RUNNER)[0]
        assert mine.volume == 0.5 and mine.sl == pytest.approx(now.ask - 30.0, abs=0.11)
        assert mine.sl > p.entry_price - 1e-9 - 0.11                         # its stop sits about at the real entry


# ------------------------------------------- the replay of the Runner --
T0 = dt.datetime(2026, 10, 6, 13, 0, tzinfo=UTC)


def bars(path, spread=0.0):
    return [Bar(T0 + dt.timedelta(minutes=i), c, h, l, c, 100.0, 0.0, spread) for i, (l, h, c) in enumerate(path)]


def trade(**kw):
    t = dict(ticket=1, symbol="US30", side="BUY", entry=100.0, stop=90.0, opened_utc=T0.isoformat(),
             tactic="SESSION_EXPANSION", realised_r=1.0, mfe_r=2.0)
    t.update(kw)
    return t


class TestRunnerReplay:
    def test_entering_with_the_trade_is_the_plain_three_r_trail(self):
        from mintel.ops.potential import _walk, runner_walk
        bs = bars([(99, 101, 100), (100, 150, 148), (115, 141, 130)])
        assert runner_walk(bs, 1, 100.0, 10.0, 0.01, T0, 3.0) == _walk(bs, 1, 100.0, 10.0, 0.01, T0, 1.0, 3.0, 8.0)[3]
        assert runner_walk(bs, 1, 100.0, 10.0, 0.01, T0, 3.0) == pytest.approx(2.0)     # 150 - 30 = 120: +2 R

    def test_a_trade_that_never_proves_itself_is_never_entered(self):
        from mintel.ops.potential import runner_walk
        assert runner_walk(bars([(99, 101, 100), (98, 105, 104), (95, 106, 100)]), 1, 100.0, 10.0, 0.01, T0,
                           3.0, 1.0) is None
        assert runner_walk(bars([(99, 101, 100), (89, 100, 95), (95, 140, 139)]), 1, 100.0, 10.0, 0.01, T0,
                           3.0, 1.0) is None                                     # stopped first

    def test_a_confirmed_entry_rides_from_the_mark(self):
        from mintel.ops.potential import runner_walk
        # +1 R at minute 1 (110, with no spread), then to 160 (+5 R from the mark), back to 125
        bs = bars([(99, 101, 100), (101, 111, 110), (110, 160, 158), (125, 158, 126)])
        assert runner_walk(bs, 1, 100.0, 10.0, 0.01, T0, 3.0, 1.0) == pytest.approx(2.0)   # out at 160 - 30 = 130

    def test_the_mark_bar_that_also_reaches_the_new_stop_counts_as_stopped(self):
        from mintel.ops.potential import runner_walk
        bs = bars([(99, 101, 100), (99, 112, 111), (110, 160, 158)])           # 99 is under the new stop (100)
        assert runner_walk(bs, 1, 100.0, 10.0, 0.01, T0, 3.0, 1.0) == -1.0

    def test_the_spread_is_paid_on_the_way_in(self):
        from mintel.ops.potential import runner_walk
        bs = bars([(99, 101, 100), (105, 111, 110), (110, 120, 119)], spread=100.0)   # 1.00 spread = 0.1 R
        v = runner_walk(bs, 1, 100.0, 10.0, 0.01, T0, 3.0, 1.0)
        assert v == pytest.approx((119 - 100) / 10 - 1.1)

    def test_the_variants_table_skips_momentum_and_non_index_trades(self):
        from mintel.ops.potential import RUNNER_VARIANTS, render_runner, runner_csv, runner_run
        path = [(99, 101, 100), (101, 111, 110), (110, 160, 158), (125, 158, 126)]
        trades = [trade(ticket=1), trade(ticket=2, tactic="MOMENTUM_CONTINUATION"),
                  trade(ticket=3, symbol="EURUSD")]
        res = runner_run(trades, lambda sym, end, n: bars(path), lambda s: 0.01, 8.0)
        assert [r.ticket for r in res] == [1, 2]
        mc = res[1].results
        assert mc["with the trade, trail 3R, every approach (to 8 Oct)"] is not None
        assert mc["with the trade, trail 3R, no momentum (version 5)"] is None
        assert res[0].results["at +1R, trail 3R, no momentum"] == pytest.approx(2.0)
        text = render_runner(res)
        assert "THE MOMENTUM RUNNER, REPLAYED: 2 index trades" in text
        assert all(name in text for name in RUNNER_VARIANTS)
        assert "session expansion" in text and "momentum continuation" in text
        assert runner_csv(res).splitlines()[0].startswith("ticket,symbol,tactic,kept_r,note")

    def test_the_command_has_the_runner_switch(self):
        from mintel.ops import potential
        assert "--runner" in inspect.getsource(potential.main)
