"""End-to-end wiring.

These tests exist to prove the components are actually connected.  A beautiful
news engine that does not change a trade, or a ranking engine not wired to the
executor, would pass every unit test above and be worthless.
"""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import CalendarEvent, Side, TF
from mintel.engine.evidence import Family
from mintel.engine.scanner import Scanner, discover
from tests.conftest import make_trader, run_cycles

UTC = dt.timezone.utc


class TestUniverseDiscovery:
    def test_symbols_are_discovered_not_hard_coded(self, sim, cfg):
        cfg.universe.max_symbols = 60      # the full broker list
        uni = discover(sim, cfg)
        assert len(uni.symbols) > 5
        assert "EURUSD" in uni.symbols
        assert "XAUUSD" in uni.symbols
        assert any(uni.groups[s] == "INDEX" for s in uni.symbols)

    def test_incomplete_specifications_are_rejected_with_a_reason(self, sim,
                                                                  cfg):
        from mintel.contracts import SymbolSpec
        broken = SymbolSpec(**{**sim.spec("EURUSD").to_dict(),
                               "tick_value": 0.0,
                               "filling_modes": ("IOC",)})
        sim._specs["EURUSD"] = broken
        uni = discover(sim, cfg)
        assert "EURUSD" not in uni.symbols
        assert "tick_value" in uni.rejected["EURUSD"]

    def test_group_configuration_is_respected(self, sim, cfg):
        cfg.universe.groups = ("GOLD",)
        uni = discover(sim, cfg)
        assert set(uni.symbols) == {"XAUUSD"}

    def test_symbol_budget_is_enforced(self, sim, cfg):
        cfg.universe.max_symbols = 3
        uni = discover(sim, cfg)
        assert len(uni.symbols) == 3

    def test_exclusion_patterns_work(self, sim, cfg):
        cfg.universe.exclude_patterns = ("XAU",)
        uni = discover(sim, cfg)
        assert "XAUUSD" not in uni.symbols


class TestScanning:
    def test_every_market_competes_on_one_scale(self, sim, cfg):
        cfg.universe.max_symbols = 60          # the whole broker list
        sc = Scanner(sim, cfg)
        states = sc.scan(sim.now)
        assert len(states) >= 4, (
            "several markets should produce a candidate on any given scan")
        symbols = {s.symbol for s in states}
        assert len(symbols) > 2, "more than one market must be considered"
        scores = [s.opportunity for s in states]
        assert scores == sorted(scores, reverse=True), "must be ranked"
        assert all(0 <= x <= 100 for x in scores)

    def test_both_directions_are_considered(self, sim, cfg):
        sc = Scanner(sim, cfg)
        states = sc.scan(sim.now)
        assert {s.direction for s in states} <= {1, -1}

    def test_multiple_timeframes_are_analysed(self, sim, cfg):
        sc = Scanner(sim, cfg)
        ctx = sc.build_context("EURUSD", sim.now)
        assert len(ctx.structure) >= 4, "top-down and bottom-up views"
        assert TF.H1 in ctx.structure or TF.H4 in ctx.structure
        assert TF.M5 in ctx.momentum

    def test_evidence_families_are_populated(self, sim, cfg):
        sc = Scanner(sim, cfg)
        ctx = sc.build_context("EURUSD", sim.now)
        ev = sc.evidence_for(ctx, 1)
        for fam in (Family.STRUCTURE, Family.MOMENTUM, Family.VOLATILITY,
                    Family.MULTI_TF, Family.EXECUTION, Family.REGIME):
            assert fam in ev

    def test_indicators_do_not_vote_three_times(self, sim, cfg):
        """EMA slope, separation and ROC must share one MOMENTUM family."""
        sc = Scanner(sim, cfg)
        ctx = sc.build_context("EURUSD", sim.now)
        ev = sc.evidence_for(ctx, 1)
        momentum_like = [f for f in ev
                         if f.value in ("MOMENTUM", "EMA", "MACD", "ROC")]
        assert momentum_like == [Family.MOMENTUM]

    def test_currency_strength_folds_into_cross_market(self, sim, cfg):
        sc = Scanner(sim, cfg)
        states = sc.scan(sim.now)
        fx = next(s for s in states if s.symbol.startswith(("EUR", "GBP")))
        assert Family.CROSS_MARKET in fx.evidence
        assert "vs" in fx.evidence[Family.CROSS_MARKET].note

    def test_a_market_with_no_history_is_still_eligible(self, sim, cfg):
        """History adds confidence, never permission."""
        sc = Scanner(sim, cfg)
        from mintel.engine.learning import HistoricalMatcher

        class EmptyJournal:
            def match_stats(self, *a):
                return {"exact": {"n": 0}, "tactic_regime": {"n": 0},
                        "tactic": {"n": 0}}

        sc.history_match = HistoricalMatcher(EmptyJournal())
        states = sc.scan(sim.now)
        assert states, "no history must not mean no opportunities"
        with_hist = [s for s in states if Family.HISTORICAL in s.evidence]
        assert with_hist
        ev = with_hist[0].evidence[Family.HISTORICAL]
        assert ev.confidence == 0.0
        assert ev.effective == 50.0, "zero confidence means zero influence"

    def test_explanations_are_plain_english(self, sim, cfg):
        sc = Scanner(sim, cfg)
        states = sc.scan(sim.now)
        text = states[0].explain()
        assert len(text) > 60
        assert "_" not in text.split("using")[-1][:40]


class TestExecutionGate:
    def test_spread_explosion_blocks_entries(self, sim, cfg):
        sc = Scanner(sim, cfg)
        sim.fault.spread_multiplier = 12.0
        states = sc.scan(sim.now)
        assert states
        assert all(not s.tradable for s in states)
        assert any("spread" in b for s in states for b in s.blockers)

    def test_stale_data_blocks_entries(self, sim, cfg):
        sc = Scanner(sim, cfg)
        sim.fault.stale_ticks = True
        sim.advance(30)
        states = sc.scan(sim.now)
        assert all(not s.tradable for s in states)

    def test_costs_are_priced_before_entry(self, sim, cfg):
        sc = Scanner(sim, cfg)
        states = sc.scan(sim.now)
        assert all(s.cost_pips >= 0 for s in states)
        assert any(s.cost_pips > 0 for s in states)


class TestNewsAffectsTrading:
    def test_an_imminent_release_blocks_new_entries(self, sim, cfg):
        """Proves the news engine is wired into the decision, not decoration."""
        t = make_trader(sim, cfg)
        sim.add_event(CalendarEvent("nfp", sim.now + dt.timedelta(seconds=45),
                                    "USD", "US", "Nonfarm Payrolls", 3))
        t.refresh_news(force=True)
        states = t.scanner.scan(sim.now)
        usd = [s for s in states if "USD" in s.symbol]
        assert usd
        assert all(not s.tradable for s in usd)
        assert any("no new entries" in b for s in usd for b in s.blockers)

    def test_news_evidence_appears_in_the_market_state(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.add_event(CalendarEvent("cpi", sim.now, "USD", "US", "CPI y/y", 3,
                                    3.6, 3.1, 3.4))
        t.refresh_news(force=True)
        for _ in range(6):
            sim.push_bar("EURUSD",
                         sim.bars("EURUSD", TF.M1, 1)[-1].close - 0.0004)
        states = t.scanner.scan(sim.now)
        eu = [s for s in states if s.symbol == "EURUSD"]
        assert eu and Family.NEWS in eu[0].evidence
        short = next((s for s in eu if s.direction < 0), None)
        long = next((s for s in eu if s.direction > 0), None)
        if short and long:
            assert (short.evidence[Family.NEWS].score
                    > long.evidence[Family.NEWS].score)


class TestFullLifecycle:
    @pytest.mark.slow
    def test_bot_opens_manages_and_closes_trades(self, sim, cfg):
        t = make_trader(sim, cfg)
        results = run_cycles(t, sim, 60, advance=3)
        opened = [r for r in results if r.acted]
        assert opened, "the bot must actually trade"
        assert all(r.top for r in results[5:]), (
            "it must always be watching, even with no trade")
        # Every entry must have journalled a decision and a trade.
        assert t.journal.open_trades() or t.journal.closed_trades()
        # Every position ever opened got a broker-side stop.
        for c in sim.order_log:
            if c.get("ticket"):
                assert c["req"].sl > 0

    @pytest.mark.slow
    def test_flowlock_manages_live_positions(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 60, advance=3)
        assert sim.modify_log, "FlowLock must actually move stops at the broker"
        # Stops only ever tightened.
        by_ticket: dict[int, list[float]] = {}
        for row in sim.modify_log:
            by_ticket.setdefault(row["ticket"], []).append(row["sl"])
        for ticket, stops in by_ticket.items():
            pos_side = next((c["req"].side for c in sim.order_log
                             if c.get("ticket") == ticket), None)
            if pos_side is Side.BUY:
                assert stops == sorted(stops)
            elif pos_side is Side.SELL:
                assert stops == sorted(stops, reverse=True)

    @pytest.mark.slow
    def test_trades_are_journalled_with_lessons(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 80, advance=3)
        closed = t.journal.closed_trades(limit=50)
        assert closed, "trades must complete and be recorded"
        row = closed[0]
        for field in ("symbol", "tactic", "regime", "opportunity", "realised_r",
                      "mfe_r", "mae_r", "mfe_capture_pct", "entry_state_json",
                      "learned"):
            assert row.get(field) is not None, field
        assert len(row["learned"]) > 20

    def test_no_quota_mechanism_exists_in_the_code(self):
        """Structural: there is nothing that could impose a daily quota.

        The only per-symbol limit is the configurable
        ``max_positions_per_symbol``, and the only time-based restraints are
        the anti-machine-gun cooldowns.  No counter of trades per day, per
        week, or per market exists to be tripped over.
        """
        import inspect
        import re
        from mintel.engine import risk, scanner, trader
        banned = ("trades_today", "daily_trade_count", "trades_per_day_limit",
                  "one_per_day", "weekly_limit", "approved_symbols",
                  "symbol_whitelist", "holding_period", "min_hold_bars")
        for module in (trader, scanner, risk):
            # Strip comments and docstrings: this checks the CODE, and the
            # docstrings deliberately discuss the absence of these things.
            code_lines = []
            for line in inspect.getsource(module).splitlines():
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                code_lines.append(re.sub(r"#.*$", "", line))
            src = "\n".join(code_lines)
            src = re.sub(r'"""(?:.|\n)*?"""', "", src).lower()
            for token in banned:
                assert token not in src, f"{token} found in {module.__name__}"

    @pytest.mark.slow
    def test_many_trades_across_many_markets_in_one_session(self, sim, cfg):
        """Behavioural: multiple trades per day, across markets, are normal."""
        t = make_trader(sim, cfg)
        results = run_cycles(t, sim, 200, advance=3)
        acted = [r.acted for r in results if r.acted]
        assert len(acted) >= 3, (
            f"only {len(acted)} trades in {len(results)} cycles - the engine "
            f"should be aggressive in LOOKING for opportunities")
        assert len({a.symbol for a in acted}) >= 2, (
            "capital must compete across markets, not sit on one pair")
        # And nothing in the block reasons may look like a quota.
        for r in results:
            for b in r.blocked_reasons:
                assert "quota" not in b.lower()
                assert "per day" not in b.lower()

    @pytest.mark.slow
    def test_re_entry_is_allowed_after_an_exit(self, sim, cfg):
        cfg.scan.cooldown_seconds_after_exit = 1.0
        t = make_trader(sim, cfg)
        results = run_cycles(t, sim, 120, advance=3)
        acted = [r.acted for r in results if r.acted]
        symbols = [a.symbol for a in acted]
        assert len(symbols) > len(set(symbols)) or len(symbols) >= 3, (
            "getting stopped out must not ban a market for the day")

    @pytest.mark.slow
    def test_calibration_learns_from_closed_trades(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 80, advance=3)
        report = t.calibrator.report()
        assert any(b["trades"] > 0 for b in report)


class TestManagementCadence:
    def test_management_runs_without_a_full_scan(self, sim, cfg):
        """Open positions must not be blind between scans."""
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 40, advance=3)
        if not sim.positions():
            pytest.skip("no position was opened in this window")
        scans_before = t.scanner.last_scan_utc
        modifies_before = len(sim.modify_log)
        for _ in range(15):
            sim.advance(1)
            t.manage_cycle()
        assert t.scanner.last_scan_utc == scans_before, (
            "manage_cycle must not run a universe scan")
        assert len(sim.modify_log) >= modifies_before

    def test_management_protects_a_naked_position(self, sim, cfg):
        from mintel.broker.base import OrderRequest
        t = make_trader(sim, cfg)
        sim.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=0.0,
                              magic=cfg.magic))
        t.manage_cycle()
        assert all(p.sl > 0 for p in sim.positions())

    def test_management_is_safe_with_no_positions(self, sim, cfg):
        t = make_trader(sim, cfg)
        assert t.manage_cycle() == []


class TestRealisedPnL:
    def test_pnl_comes_from_the_broker_not_a_local_guess(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 60, advance=3)
        closed = t.journal.closed_trades(limit=50)
        if not closed:
            pytest.skip("no trade completed in this window")
        for row in closed:
            deal = sim.closed_deal(row["ticket"])
            if deal is None:
                continue
            assert row["pnl_money"] == pytest.approx(deal["pnl"], abs=0.01), (
                "the journal must record what the broker actually paid")

    def test_sign_of_money_agrees_with_r_multiple(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 80, advance=3)
        closed = t.journal.closed_trades(limit=50)
        if not closed:
            pytest.skip("no trade completed in this window")
        for row in closed:
            r = row["realised_r"] or 0.0
            money = row["pnl_money"] or 0.0
            if abs(r) > 0.05 and abs(money) > 0.01:
                assert (r > 0) == (money > 0), (
                    f"{row['symbol']}: {r:+.2f}R but {money:+.2f} money")

    def test_mfe_is_never_less_than_the_realised_gain(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 80, advance=3)
        for row in t.journal.closed_trades(limit=50):
            r = row["realised_r"] or 0.0
            mfe = row["mfe_r"] or 0.0
            if r > 0:
                assert mfe >= r - 1e-6, "MFE cannot be below the result"
            assert 0.0 <= (row["mfe_capture_pct"] or 0.0) <= 100.0


class TestEntryTiming:
    def test_timing_never_vetoes_on_missing_data(self, sim, cfg):
        """Many brokers cannot serve enough ticks; that is not a veto."""
        sc = Scanner(sim, cfg)
        ok, note = sc.timing_confirmation("EURUSD", 1)
        assert ok is True
        assert note

    def test_an_efficient_move_against_the_trade_delays_entry(self, sim, cfg):
        import datetime as dt
        from mintel.broker.base import Bar
        sc = Scanner(sim, cfg)
        base = sim.now
        falling = [Bar(base + dt.timedelta(seconds=10 * i), 1.10 - 0.0001 * i,
                       1.10 - 0.0001 * i, 1.10 - 0.0001 * (i + 1),
                       1.10 - 0.0001 * (i + 1), 50.0) for i in range(20)]
        original = sim.bars
        sim.bars = lambda symbol, tf, count, end=None: (
            falling if tf.is_synthetic else original(symbol, tf, count, end))
        try:
            ok, note = sc.timing_confirmation("EURUSD", 1)
            assert ok is False, "must not buy into an active flush"
            assert "against" in note
            ok_short, _ = sc.timing_confirmation("EURUSD", -1)
            assert ok_short is True, "selling with it is fine"
        finally:
            sim.bars = original

    def test_noise_against_the_trade_does_not_block(self, sim, cfg):
        import datetime as dt
        from mintel.broker.base import Bar
        sc = Scanner(sim, cfg)
        base = sim.now
        px, chop = 1.10, []
        for i in range(20):
            px += 0.00012 if i % 2 else -0.00013
            chop.append(Bar(base + dt.timedelta(seconds=10 * i), px, px + 5e-5,
                            px - 5e-5, px, 50.0))
        original = sim.bars
        sim.bars = lambda symbol, tf, count, end=None: (
            chop if tf.is_synthetic else original(symbol, tf, count, end))
        try:
            ok, _ = sc.timing_confirmation("EURUSD", 1)
            assert ok is True, "waiting out normal noise means never entering"
        finally:
            sim.bars = original
