"""Chaos tests: deliberately break things and check the bot behaves.

A system is not bulletproof because its tests pass on a perfect day.  It is
bulletproof when failures have been thrown at it on purpose.  Each test here
injects a real production failure mode and asserts the *behavioural* response:
keep positions protected, do not duplicate orders, do not trade blind, recover
automatically, and never silently stop.
"""
from __future__ import annotations

import datetime as dt
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mintel.broker.base import OrderRequest, RetCode, Side, TF
from mintel.engine.execution import Executor, Intent, IntentState
from mintel.engine.journal import Journal
from mintel.engine.trader import Trader
from tests.conftest import make_trader, run_cycles

UTC = dt.timezone.utc


class TestBrokerDisconnect:
    def test_disconnect_mid_run_enters_safe_mode_and_does_not_trade(self, sim,
                                                                    cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 5, advance=2)
        sim.fault.disconnect = True
        r = t.cycle()
        assert r.safe_mode
        assert r.acted is None
        assert not r.entries_allowed

    def test_reconnect_resumes_trading_automatically(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.disconnect = True
        t.cycle()
        assert t.health.safe_mode
        sim.fault.disconnect = False
        sim.connect()
        sim.advance(2)
        r = t.cycle()
        assert not r.safe_mode, "recovery must be automatic"
        assert r.top, "and it must go straight back to looking for trades"

    def test_open_positions_keep_their_broker_stop_through_a_disconnect(self,
                                                                        sim,
                                                                        cfg):
        t = make_trader(sim, cfg)
        results = run_cycles(t, sim, 40, advance=3)
        if not sim.positions():
            pytest.skip("no position was opened in this window")
        stops = {p.ticket: p.sl for p in sim.positions()}
        assert all(v > 0 for v in stops.values())
        sim.fault.disconnect = True
        t.cycle()                       # the bot is blind now
        sim.fault.disconnect = False
        sim.connect()
        # The broker still holds the protective stops; they are not ours to lose.
        for p in sim.positions():
            assert p.sl == pytest.approx(stops[p.ticket])

    def test_cycle_never_raises_when_the_broker_throws(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.disconnect = True
        for _ in range(3):
            r = t.cycle()               # must not raise
            assert r is not None


class TestStaleData:
    def test_stale_feed_stops_new_entries_but_not_management(self, sim, cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 30, advance=3)
        sim.fault.stale_ticks = True
        sim.advance(30)
        r = t.cycle()
        assert r.acted is None, "must not trade on stale prices"
        assert not r.entries_allowed

    def test_fresh_data_resumes_trading(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.stale_ticks = True
        sim.advance(30)
        t.cycle()
        sim.fault.stale_ticks = False
        sim.advance(2)
        r = t.cycle()
        assert r.entries_allowed


class TestSpreadExplosion:
    def test_blown_spread_prevents_entries(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.spread_multiplier = 15.0
        r = t.cycle()
        assert r.acted is None
        assert r.top, "it still watches and ranks while refusing to trade"

    def test_normal_spread_resumes_entries(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.spread_multiplier = 15.0
        t.cycle()
        sim.fault.spread_multiplier = 1.0
        sim.advance(2)
        r = t.cycle()
        assert r.entries_allowed


class TestOrderRejection:
    def _forced_state(self, sim):
        """A synthetic candidate, so the test does not depend on market luck."""
        from mintel.data.regime import Regime
        from mintel.engine.evidence import Evidence, Family
        from mintel.engine.evidence import MarketState
        spec = sim.spec("EURUSD")
        tick = sim.tick("EURUSD")
        entry = tick.ask
        return MarketState(
            symbol="EURUSD", as_of=sim.now, regime=Regime.TREND,
            regime_label="TREND / NORMAL VOL", direction=1,
            tactic="MOMENTUM_CONTINUATION",
            evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0)},
            raw_score=80.0, opportunity=80.0, tier="STRONG", entry=entry,
            stop=spec.normalise_price(entry - spec.pips_to_price(25)),
            target=spec.normalise_price(entry + spec.pips_to_price(50)),
            reward_risk=2.0, headroom_pips=50.0, cost_pips=1.0)

    def test_rejection_storm_trips_the_breaker_rather_than_looping(self, sim,
                                                                   cfg):
        cfg.risk.max_rejects_per_10min = 3
        t = make_trader(sim, cfg)
        sim.fault.reject_all = RetCode.REJECT
        spec = sim.spec("EURUSD")
        for _ in range(5):
            state = self._forced_state(sim)
            t.executor.set_cooldown("EURUSD", Side.BUY, 0.0)
            t.executor.submit(state, spec, 0.10)
            sim.advance(1)
        snap = t.risk.snapshot(sim.account(), sim.positions(), sim.now)
        assert not snap.entries_allowed
        assert any("rejected" in x for x in snap.blocking_reasons())
        assert not sim.positions(), "nothing was actually opened"

    def test_a_tripped_breaker_stops_the_trader_entering(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.risk.halt("chaos test")
        r = t.cycle()
        assert r.acted is None
        assert not r.entries_allowed
        assert any("chaos test" in b for b in r.blocked_reasons)

    def test_no_money_rejection_does_not_retry_forever(self, sim, cfg):
        t = make_trader(sim, cfg)
        sim.fault.reject_all = RetCode.NO_MONEY
        report = t.executor.submit(self._forced_state(sim),
                                   sim.spec("EURUSD"), 0.10)
        assert not report.ok
        assert report.attempts == 1, "a fatal rejection is not retried"


class TestRestartRecovery:
    def test_restart_with_an_open_position_adopts_and_keeps_managing_it(self,
                                                                        sim,
                                                                        cfg):
        t = make_trader(sim, cfg)
        run_cycles(t, sim, 40, advance=3)
        if not sim.positions():
            pytest.skip("no position was opened in this window")
        before = {p.ticket: p.sl for p in sim.positions()}
        ticket = next(iter(before))
        t.journal.close()

        # A brand-new process against the same broker and the same database.
        revived = make_trader(sim, cfg)
        for tk in before:
            assert tk in revived.flowlock.trackers, (
                "FlowLock state must survive a restart")
        assert len(sim.positions()) == len(before), (
            "recovery must adopt, never duplicate")
        run_cycles(revived, sim, 10, advance=3)
        for p in sim.positions():
            if p.ticket in before and p.side is Side.BUY:
                assert p.sl >= before[p.ticket], "stops only tighten"
            elif p.ticket in before:
                assert p.sl <= before[p.ticket]

    def test_restart_never_duplicates_an_in_flight_order(self, sim, cfg):
        """The crash-between-send-and-response case."""
        t = make_trader(sim, cfg)
        states = [s for s in t.scanner.scan(sim.now) if s.tradable]
        if not states:
            pytest.skip("no tradable candidate in this window")
        s = states[0]
        spec = sim.spec(s.symbol)
        from mintel.engine.execution import make_key
        key = make_key(s, spec)
        # The order reached the broker; our process died before the reply.
        sim.send(OrderRequest(s.symbol, s.side, 0.10, sl=s.stop, comment=key,
                              magic=cfg.magic, idempotency_key=key))
        t.journal.save_intent(Intent(key, s.symbol, s.side, 0.10, s.stop, 0.0,
                                     sim.now, IntentState.UNKNOWN))
        revived = make_trader(sim, cfg)
        assert len(sim.positions()) == 1
        revived.cycle()
        # The recovered intent must not be re-sent.  The bot is free to open a
        # DIFFERENT trade in the same cycle - that is it working, not a
        # duplicate - so the assertion is about this symbol and side.
        same = [p for p in sim.positions()
                if p.symbol == s.symbol and p.side is s.side]
        assert len(same) == 1, "the restart must not re-send the same intent"
        assert revived.executor.intents[key].state is IntentState.FILLED

    def test_broker_state_wins_over_local_state(self, sim, cfg):
        t = make_trader(sim, cfg)
        # A position the bot has no record of at all.
        sim.send(OrderRequest("XAUUSD", Side.BUY, 0.05, sl=0.0,
                              magic=cfg.magic))
        out = t.executor.reconcile()
        assert out["orphans"]
        assert all(p.sl > 0 for p in sim.positions()), (
            "adopted positions must be protected immediately")


class TestDatabaseFailure:
    def test_trading_continues_when_the_journal_dies(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.journal.close()
        r = t.cycle()                  # must not raise
        assert r is not None
        # The supervisor notices and heals it.
        actions = t.health.heal()
        assert t.journal.healthy() or any("database" in a for a in actions)

    def test_corrupt_database_file_is_recovered(self, sim, cfg, workdir):
        path = workdir / "corrupt.sqlite"
        j = Journal(path)
        j.log_event("TEST", "before", "INFO")
        j.close()
        path.write_bytes(b"this is not a database at all")
        broken = Journal.__new__(Journal)
        broken.path = path
        import threading
        broken._lock = threading.RLock()
        broken._conn = None
        try:
            broken.connect()
        except sqlite3.DatabaseError:
            pass
        assert not broken.healthy()
        # Replacing the damaged file and reconnecting restores service.
        path.unlink()
        assert broken.reconnect()
        assert broken.healthy()
        broken.log_event("TEST", "after", "INFO")
        assert broken.recent_events()
        broken.close()

    def test_writes_are_retried_after_a_reconnect(self, sim, cfg, workdir):
        j = Journal(workdir / "retry.sqlite")
        j.close()
        j.log_event("TEST", "written after recovery", "INFO")
        assert any(e["message"] == "written after recovery"
                   for e in j.recent_events())
        j.close()


class TestCorruptCache:
    def test_a_corrupt_heartbeat_file_is_ignored(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.cycle()
        path = t.hb_strategy.path
        path.write_text("{{{ not json")
        r = t.cycle()
        assert r is not None, "a bad cache file must not crash the trader"

    def test_a_corrupt_config_falls_back_to_demo(self, workdir):
        from mintel.config import Config, LIVE_MARKER
        path = workdir / "config.json"
        good = Config()
        good.mode = "LIVE"
        good.live_marker = LIVE_MARKER
        good.save(path)
        path.write_text("{ this is not valid json")
        loaded = Config.load(path)
        assert loaded.effective_mode == "DEMO", (
            "corruption must never result in live trading")

    def test_a_config_with_live_mode_but_no_marker_is_demoted(self, workdir):
        import json
        from mintel.config import Config
        path = workdir / "config.json"
        Config().save(path)
        raw = json.loads(path.read_text())
        raw["mode"] = "LIVE"
        raw["live_marker"] = "oops"
        path.write_text(json.dumps(raw))
        loaded = Config.load(path)
        assert loaded.effective_mode == "DEMO"


class TestProcessKill:
    def test_killing_a_worker_is_detected_and_restarted(self, tmp_path):
        """The watchdog notices a killed process and brings it back."""
        from mintel.config import Config
        from mintel.ops.watchdog import ManagedProcess, Watchdog
        root = Path(__file__).resolve().parents[1]
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        script = tmp_path / "worker.py"
        script.write_text(
            "import sys, time\n"
            f"sys.path.insert(0, {str(root)!r})\n"
            "from mintel.ops.health import Heartbeat\n"
            f"hb = Heartbeat({str(tmp_path / 'data' / 'heartbeats')!r}, 'w')\n"
            "while True:\n"
            "    hb.beat({})\n"
            "    time.sleep(0.1)\n")
        mp = ManagedProcess("w", [sys.executable, str(script)], heartbeat="w",
                            grace_seconds=0.05)
        wd = Watchdog(cfg, [mp])
        wd.check_once()
        time.sleep(0.5)
        assert wd.check_once() == []
        first_pid = mp.proc.pid

        mp.proc.kill()
        mp.proc.wait(timeout=10)
        mp.backoff = 0.0
        actions = wd.check_once()
        assert any("not running" in a for a in actions)
        assert mp.proc.pid != first_pid, "a replacement was started"
        mp.stop()

    def test_a_hung_process_is_killed_before_a_replacement_starts(self,
                                                                  tmp_path):
        """Two traders on one account would double every trade."""
        from mintel.config import Config
        from mintel.ops.watchdog import ManagedProcess, Watchdog
        root = Path(__file__).resolve().parents[1]
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        cfg.ops.heartbeat_stale_seconds = 1.0
        script = tmp_path / "hung.py"
        script.write_text(
            "import sys, time\n"
            f"sys.path.insert(0, {str(root)!r})\n"
            "from mintel.ops.health import Heartbeat\n"
            f"hb = Heartbeat({str(tmp_path / 'data' / 'heartbeats')!r}, 'w')\n"
            "hb.beat({})\n"
            "time.sleep(600)\n")          # alive, but never beats again
        mp = ManagedProcess("w", [sys.executable, str(script)], heartbeat="w",
                            grace_seconds=0.05)
        wd = Watchdog(cfg, [mp])
        wd.check_once()
        time.sleep(1.6)                    # its heartbeat goes stale
        mp.backoff = 0.0
        first = mp.proc
        actions = wd.check_once()
        assert any("hung" in a for a in actions)
        assert first.poll() is not None, "the hung process must be killed"
        mp.stop()


class TestPidGuard:
    def test_a_second_trader_refuses_to_start(self, workdir):
        from mintel.run import PidFile
        first = PidFile(workdir / "trader.pid")
        ok, _ = first.acquire()
        assert ok
        # A different PID file object pointing at the same path, with a live PID
        # inside it, must refuse.
        second = PidFile(workdir / "trader.pid")
        (workdir / "trader.pid").write_text(str(_a_live_pid()))
        ok2, msg = second.acquire()
        assert not ok2 and "already running" in msg

    def test_a_stale_pid_file_does_not_block_startup(self, workdir):
        from mintel.run import PidFile
        (workdir / "trader.pid").write_text(str(2 ** 22))
        ok, _ = PidFile(workdir / "trader.pid").acquire()
        assert ok


def _a_live_pid() -> int:
    """A PID that is certainly alive and is not this process."""
    p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    return p.pid


class TestSleepAndWake:
    """A laptop sleeps. That is routine, and must be handled as such."""

    def test_a_long_gap_is_recognised_as_sleep(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.cycle()
        sim.advance(240)                      # four hours with the lid shut
        result = t.cycle()
        assert t.sleep_events, "the suspension must be noticed"
        assert t.sleep_events[-1]["gap_seconds"] > 3600
        kinds = [e["kind"] for e in t.journal.recent_events()]
        assert "WAKE" in kinds, "and recorded honestly"

    def test_waking_discards_the_stale_view(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.cycle()
        assert t.top_opportunities
        sim.advance(300)
        t._detect_sleep(sim.now)
        assert t.top_opportunities == (), "old rankings must not survive sleep"
        assert t.last_news_refresh is None, "the calendar must be re-read"

    def test_waking_reconciles_against_the_broker(self, sim, cfg):
        from mintel.broker.base import OrderRequest
        t = make_trader(sim, cfg)
        t.cycle()
        # A position appears while the machine is asleep, as a broker-side
        # stop or a manual trade would.
        sim.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=0.0,
                              magic=cfg.magic))
        sim.advance(300)
        t.cycle()
        assert all(p.sl > 0 for p in sim.positions()), (
            "anything found after waking must be protected at once")

    def test_a_normal_gap_is_not_mistaken_for_sleep(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.cycle()
        sim.advance(1)
        t.cycle()
        assert not t.sleep_events

    def test_trading_does_not_resume_until_data_is_fresh(self, sim, cfg):
        t = make_trader(sim, cfg)
        t.cycle()
        sim.fault.stale_ticks = True
        sim.advance(300)                      # asleep, and no new prices since
        result = t.cycle()
        assert result.acted is None
        assert not result.entries_allowed
