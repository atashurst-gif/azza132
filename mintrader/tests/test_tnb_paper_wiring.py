"""Trend & Breakout on PAPER, wired in: the setting (``tnb``), the wrapper
handed to the trader in ``mintel/run.py`` ``main()`` and nowhere else, the
other bots on the REAL broker, and a real trade left open at the switch
wound down at the real broker to its natural end."""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
from types import SimpleNamespace

import pytest

from mintel.botexec import MAGIC_RUNNER
from mintel.broker.base import Side
from mintel.broker.paper import PAPER_FIRST_TICKET, PaperBroker, real_broker
from mintel.broker.sim import SimBroker
from mintel.config import Config, TnbConfig, tnb_mode
from mintel.engine.evidence import Evidence, Family, MarketState
from mintel.engine.trader import Trader
from mintel.data.regime import Regime
from tests.conftest import MID_LONDON, lifecycle_profile

TNB = 990_311


# ---------------------------------------------------------------- helpers --
def setup(tmp_path, *, tnb="PAPER", runner="LIVE", symbols=("EURUSD", "US500")):
    sim = SimBroker(list(symbols), seed=101, start=MID_LONDON, history_bars=3000)
    sim.connect()
    cfg = lifecycle_profile(Config())
    cfg.ops.data_dir = str(tmp_path / "data")
    cfg.ops.log_dir = str(tmp_path / "logs")
    cfg.news.store_path = "calendar.sqlite"
    cfg.account_login, cfg.account_server = sim.login, sim.server
    cfg.tnb.mode = tnb
    cfg.runner.mode = runner
    return sim, cfg


def paper_on(sim, cfg) -> PaperBroker:
    pb = PaperBroker.from_config(sim, cfg)
    pb._now = lambda: sim.now               # the wrapper judges freshness on the simulator's clock
    return pb


def trader_on(broker, sim, cfg) -> Trader:
    tr = Trader(broker, cfg, clock=lambda: sim.now, enable_model=False)
    tr.bootstrap()
    tr.scanner.timing_confirmation = lambda symbol, direction: (True, "")
    return tr


def entry(sim, symbol, distance, tactic="SESSION_EXPANSION") -> MarketState:
    """A STRONG long setup at the current ask, ``distance`` to its stop."""
    t = sim.tick(symbol)
    spec = sim.spec(symbol)
    e = t.ask
    stop = spec.normalise_price(e - distance)
    return MarketState(symbol=symbol, as_of=sim.now, regime=Regime.TREND, regime_label="TREND", direction=1,
                       tactic=tactic, evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0)},
                       raw_score=75.0, opportunity=75.0, tier="STRONG", entry=e, stop=stop,
                       target=spec.normalise_price(e + 3 * distance), reward_risk=2.0, headroom_pips=40.0,
                       cost_pips=1.0)


def act(tr, sim, state):
    acct = tr.broker.account()
    tr.last_entry_utc = None
    snap = tr.risk.snapshot(acct, tr.broker.positions(TNB), sim.now)
    tr._last_snapshot = snap                # as cycle() keeps it: the other bots' account gate reads it
    acted, msg, blocked = tr.act([state], acct, snap, tr.broker.positions(TNB), sim.now)
    assert acted is not None, blocked
    return acted


def tnb_orders(sim):
    return [o for o in sim.order_log if int(o["req"].magic or 0) == TNB]


def move(sim, symbol, by):
    """A flat bar ``by`` away: the simulator judges a stop against its last
    bar's range, so a flat bar keeps a stop set after it from being hit by
    prices printed before it."""
    price = sim.tick(symbol).mid + by
    sim.push_bar(symbol, price, high=price, low=price)


# --------------------------------------------------------------- setting --
class TestTheSetting:
    def test_live_by_default_so_nothing_changes_until_the_line_up_flips_it(self):
        c = Config()
        assert c.tnb.mode == "LIVE" and tnb_mode(c) == "LIVE" and c.tnb_paper is None
        assert c.tnb.manage_leftovers is True and c.tnb.live_groups == () and c.tnb.slippage_points == 1.0
        assert c.validate() == []

    def test_it_round_trips_through_the_file(self, tmp_path):
        c = Config()
        c.tnb.mode = "PAPER"
        c.tnb.slippage_points = 2.0
        c.tnb.live_groups = ("INDEX",)
        c.tnb.manage_leftovers = False
        c.tnb.replay_max_hours = 24.0
        c.save(tmp_path / "config.json")
        raw = json.loads((tmp_path / "config.json").read_text())
        assert raw["tnb"]["mode"] == "PAPER" and raw["tnb"]["live_groups"] == ["INDEX"]
        back = Config.load(tmp_path / "config.json")
        assert back.tnb == c.tnb and tnb_mode(back) == "PAPER" and back.tnb_paper is back.tnb

    def test_a_malformed_mode_is_live_with_a_warning_never_a_crash(self, tmp_path, caplog):
        p = tmp_path / "config.json"
        for bad in ("papr", 7, None, ["PAPER"], {"x": 1}):
            p.write_text(json.dumps({"tnb": {"mode": bad}}))
            with caplog.at_level(logging.WARNING, logger="mintel.config"):
                c = Config.load(p)
            assert c.tnb.mode == "LIVE" and tnb_mode(c) == "LIVE" and c.tnb_paper is None, bad
        assert any("Trend & Breakout stays LIVE" in r.getMessage() for r in caplog.records)

    def test_a_block_that_is_not_a_table_is_live(self, tmp_path, caplog):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": "PAPER"}))
        with caplog.at_level(logging.WARNING, logger="mintel.config"):
            c = Config.load(p)
        assert c.tnb == TnbConfig() and tnb_mode(c) == "LIVE"
        assert any("not a table" in r.getMessage() for r in caplog.records)

    def test_bad_numbers_fall_back_to_the_defaults(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": " paper ", "slippage_points": -2, "replay_max_hours": "lots",
                                         "live_groups": "INDEX", "commission": "free"}}))
        c = Config.load(p)
        assert c.tnb.mode == "PAPER" and c.tnb.slippage_points == 1.0 and c.tnb.replay_max_hours == 72.0
        assert c.tnb.live_groups == ("INDEX",) and c.tnb.commission == {}

    def test_a_mode_set_in_code_is_read_safely_too(self):
        c = Config()
        c.tnb.mode = "Paper-ish"
        assert tnb_mode(c) == "LIVE" and c.tnb_paper is None
        assert c.validate() == [] and c.tnb.mode == "LIVE"

    def test_the_paper_wrapper_reads_the_block(self, tmp_path):
        sim, cfg = setup(tmp_path)
        cfg.tnb.slippage_points = 2.5
        cfg.tnb.live_groups = ("indices",)
        cfg.tnb.manage_leftovers = False
        pb = PaperBroker.from_config(sim, cfg)
        assert pb.slippage_points == 2.5 and pb.live_groups == {"INDEX"} and pb.cfg.manage_leftovers is False
        assert pb.magic == TNB and str(pb.path).startswith(cfg.ops.data_dir)
        pb.close_db()
        cfg.tnb = TnbConfig(mode="PAPER")
        pb = PaperBroker.from_config(sim, cfg)
        assert pb.cfg.manage_leftovers is True                    # the default: real leftovers wound down at the broker
        pb.close_db()


# ---------------------------------------------------------------- run.py --
class _FakeTrader:
    """Stands in for the Trader in main(): records the broker it was given."""
    made: list = []

    def __init__(self, broker, cfg, **kw):
        _FakeTrader.made.append(broker)
        self.broker, self.cfg = broker, cfg
        self.health = SimpleNamespace(last_report=None, safe_mode=False)
        self.journal = SimpleNamespace(log_event=lambda *a, **k: None, close=lambda: None)
        self._stop = threading.Event()

    def bootstrap(self):
        return {}

    def cycle(self):
        return SimpleNamespace(summary=lambda: "ok")

    def thinking(self):
        return []

    def stop(self):
        self._stop.set()


@pytest.fixture
def run_main(tmp_path, monkeypatch):
    """main() with the simulator for MetaTrader and the fake trader."""
    import mintel.run as run
    sim = SimBroker(["EURUSD", "US500"], seed=101, start=MID_LONDON, history_bars=500)
    sim.connect()
    built = []

    def build(cfg):
        built.append(sim)
        return sim

    _FakeTrader.made = []
    monkeypatch.setattr(run, "build_broker", build)
    monkeypatch.setattr(run, "wait_for_broker", lambda *a, **k: True)
    monkeypatch.setattr(run, "Trader", _FakeTrader)
    monkeypatch.setattr(run, "push_dashboard", lambda *a, **k: None)
    monkeypatch.setattr(run, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)

    def go(tnb_block):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        cfg.tracking_strategy = ""
        path = tmp_path / "config.json"
        cfg.save(path)
        raw = json.loads(path.read_text())
        raw["tnb"] = tnb_block
        path.write_text(json.dumps(raw))
        code = run.main(["--config", str(path), "--check-only", "--no-dashboard"])
        return code, built, _FakeTrader.made
    return go


class TestRunMain:
    def test_paper_hands_the_trader_the_wrapper_built_on_the_real_broker(self, run_main, caplog):
        with caplog.at_level(logging.WARNING, logger="mintel.run"):
            code, built, made = run_main({"mode": "PAPER"})
        assert code == 0 and len(made) == 1 and len(built) == 1
        assert isinstance(made[0], PaperBroker) and made[0].inner is built[0]
        assert not isinstance(built[0], PaperBroker)              # build_broker() itself is never wrapped
        assert made[0].cfg.manage_leftovers is True
        assert ("Trend & Breakout: PAPER - real prices, simulated orders; the Momentum Runner still rides its "
                "index entries with real orders") in [r.getMessage() for r in caplog.records]
        made[0].close_db()

    def test_live_hands_the_trader_the_real_broker_unchanged(self, run_main, caplog):
        with caplog.at_level(logging.WARNING, logger="mintel.run"):
            code, built, made = run_main({"mode": "LIVE"})
        assert code == 0 and made == built
        assert any(r.getMessage().startswith("Trend & Breakout: LIVE") for r in caplog.records)

    def test_a_malformed_mode_in_the_file_starts_it_live(self, run_main):
        code, built, made = run_main({"mode": "PAPERR"})
        assert code == 0 and made == built and not isinstance(made[0], PaperBroker)

    def test_a_paper_record_that_cannot_be_opened_stops_the_start_instead_of_trading_it_live(self, run_main,
                                                                                            monkeypatch):
        def broken(*a, **k):
            raise OSError("disk full")
        monkeypatch.setattr(PaperBroker, "from_config", broken)
        code, built, made = run_main({"mode": "PAPER"})
        assert code == 6 and made == []


# ----------------------------------------------------- the trader, PAPER --
class TestPaperTrader:
    def test_the_trader_holds_the_wrapper_and_everyone_else_the_real_broker(self, tmp_path):
        sim, cfg = setup(tmp_path)
        cfg.bandbreaker.mode = "LIVE"
        cfg.crowd.mode = "LIVE"
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.broker is pb and tr.real_broker is sim and real_broker(tr.broker) is sim
        assert tr.executor.broker is pb and tr.scanner.broker is pb       # Trend & Breakout's own orders: paper
        assert tr.runner.live and tr.runner.broker is sim and tr.runner.executor.broker is sim
        assert tr.bandbreaker is not None and tr.bandbreaker.broker is sim and tr.bandbreaker.executor.broker is sim
        assert tr.crowd is not None and tr.crowd.broker is sim and tr.crowd.executor.broker is sim
        assert tr.health.broker is sim                                    # the account health checks: the real account
        tr.journal.close()
        pb.close_db()

    def test_paper_band_breaker_and_crowd_fader_are_given_the_real_broker_too(self, tmp_path):
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.bandbreaker.broker is sim and tr.crowd.broker is sim
        tr.journal.close()
        pb.close_db()

    def test_its_orders_never_reach_the_real_broker(self, tmp_path):
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "EURUSD", 0.0030))
        mine = pb.positions(TNB)
        assert len(mine) == 1 and mine[0].ticket >= PAPER_FIRST_TICKET and mine[0].sl > 0
        assert sim.positions(TNB) == [] and tnb_orders(sim) == [] and sim.order_log == []
        row = tr.journal.open_trades()[0]
        assert row["ticket"] == mine[0].ticket
        # managed on paper: a move in its favour, then out
        for _ in range(3):
            move(sim, "EURUSD", +0.0025)
            tr.manage_cycle()
        move(sim, "EURUSD", -0.0300)
        tr.manage_cycle()
        assert pb.positions(TNB) == [] and pb.closed and sim.modify_log == [] and sim.order_log == []
        tr.cycle()                                            # the full cycle books the closed trade
        closed = [t for t in tr.journal.closed_trades() if t["ticket"] == mine[0].ticket]
        paper_net = sum(d["pnl"] for d in pb.closed if d["ticket"] == mine[0].ticket)
        assert closed and closed[0]["pnl_money"] == pytest.approx(pb.closed_deal(mine[0].ticket)["pnl"])
        assert paper_net < 0                                  # gapped through its stop: a paper loss, never real money
        # a few full cycles on top: still nothing for 990311 at the real broker
        for _ in range(5):
            tr.cycle()
            sim.advance(3)
        assert tnb_orders(sim) == [] and sim.positions(TNB) == []
        tr.journal.close()
        pb.close_db()

    def test_the_runners_live_order_reaches_the_real_broker_with_its_own_magic(self, tmp_path):
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "US500", 30.0))
        paper_trade = pb.positions(TNB)[0]
        tr.manage_cycle()
        runner = sim.positions(MAGIC_RUNNER)
        assert len(runner) == 1 and runner[0].magic == MAGIC_RUNNER and runner[0].comment == "RUNNER"
        assert runner[0].symbol == "US500" and runner[0].side is Side.BUY and runner[0].sl == pytest.approx(paper_trade.sl)
        assert runner[0].volume == pytest.approx(paper_trade.volume)    # top size off by default: the copied volume
        assert [int(o["req"].magic) for o in sim.order_log] == [MAGIC_RUNNER]
        assert sim.positions(TNB) == [] and pb.is_paper(paper_trade.ticket)   # Trend & Breakout's side stays paper
        s = next(iter(tr.runner.open.values()))
        assert s.mode == "LIVE" and s.source_ticket == paper_trade.ticket
        tr.journal.close()
        pb.close_db()


# ------------------------------------------------------ the trader, LIVE --
class TestLiveUnchanged:
    def test_live_mode_trades_at_the_broker_as_before(self, tmp_path):
        sim, cfg = setup(tmp_path, tnb="LIVE")
        tr = trader_on(sim, sim, cfg)
        assert tr.broker is sim and tr.real_broker is sim and tr.executor.broker is sim
        assert tr.runner.broker is sim and tr.runner.executor.broker is sim and tr.health.broker is sim
        act(tr, sim, entry(sim, "EURUSD", 0.0030))
        assert len(sim.positions(TNB)) == 1 and len(tnb_orders(sim)) == 1
        assert not (data := list((tmp_path / "data").glob("tnb_paper.sqlite")))
        tr.journal.close()


# ------------------------------------ a real trade open at the switch --
class TestLeftoverAtTheSwitch:
    def test_a_real_trade_open_at_the_switch_is_wound_down_at_the_real_broker(self, tmp_path):
        sim, cfg = setup(tmp_path, tnb="LIVE")
        live = trader_on(sim, sim, cfg)
        act(live, sim, entry(sim, "EURUSD", 0.0030))
        real = sim.positions(TNB)[0]
        live.journal.close()                                  # the bot stops; the switch; it starts again
        cfg.tnb.mode = "PAPER"
        pb = paper_on(sim, cfg)
        assert pb.cfg.manage_leftovers is True
        tr = trader_on(pb, sim, cfg)
        assert real.ticket in tr.flowlock.trackers            # adopted with its stop, as before the switch
        assert [p.ticket for p in pb.positions(TNB)] == [real.ticket]
        start_sl = sim.positions(TNB)[0].sl
        for _ in range(3):                                    # it runs: the stop is tightened AT THE BROKER
            move(sim, "EURUSD", +0.0020)
            tr.manage_cycle()
        moved = [m for m in sim.modify_log if m["ticket"] == real.ticket]
        assert moved and moved[-1]["sl"] > start_sl
        move(sim, "EURUSD", -0.0300)                          # its broker stop ends it
        tr.manage_cycle()
        assert sim.positions(TNB) == [] and real.ticket not in {p.ticket for p in pb.positions(TNB)}
        tr.cycle()                                            # the full cycle books it from the broker's record
        deal = sim.closed_deal(real.ticket)
        row = next(t for t in tr.journal.closed_trades() if t["ticket"] == real.ticket)
        assert row["pnl_money"] == pytest.approx(deal["pnl"])  # the real broker's own figure
        assert len(tnb_orders(sim)) == 1                       # the original order only: nothing new was sent
        assert not any(d["ticket"] == real.ticket for d in pb.closed)   # nothing booked on paper for it
        assert not pb.is_paper(real.ticket)
        tr.journal.close()
        pb.close_db()

    def test_with_leftovers_off_nothing_is_sent_for_it(self, tmp_path):
        sim, cfg = setup(tmp_path, tnb="LIVE")
        live = trader_on(sim, sim, cfg)
        act(live, sim, entry(sim, "EURUSD", 0.0030))
        real = sim.positions(TNB)[0]
        live.journal.close()
        cfg.tnb.mode = "PAPER"
        cfg.tnb.manage_leftovers = False
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        modifies = len(sim.modify_log)
        for _ in range(3):
            move(sim, "EURUSD", +0.0020)
            tr.manage_cycle()
        assert len(sim.modify_log) == modifies and sim.positions(TNB)[0].sl == pytest.approx(real.sl)
        tr.journal.close()
        pb.close_db()


# --------------------- the verify smoke test (a dry-run Trader beside it) --
def dry_run_beside(sim, cfg, tmp_path) -> Trader:
    """What ``mintel/verify.py`` builds: the REAL broker, the same journal
    file as the running trader, dry_run."""
    from mintel.engine.journal import Journal
    return Trader(sim, cfg, clock=lambda: sim.now, enable_model=False,
                  journal=Journal(tmp_path / "data" / "journal.sqlite"), dry_run=True)


class TestDryRunBesideIt:
    def test_it_leaves_open_paper_trades_open_in_the_shared_journal(self, tmp_path):
        sim, cfg = setup(tmp_path, runner="PAPER")
        pb = paper_on(sim, cfg)
        main = trader_on(pb, sim, cfg)
        act(main, sim, entry(sim, "EURUSD", 0.0030))
        main.manage(main.broker.positions(TNB), sim.now)          # its tracker is saved
        ticket = int(main.journal.open_trades()[0]["ticket"])
        assert pb.is_paper(ticket)
        book, orders = len(pb.closed), len(sim.order_log)
        v = dry_run_beside(sim, cfg, tmp_path)
        assert isinstance(v.broker, PaperBroker) and v.broker.read_only and v.real_broker is sim
        v.bootstrap()
        v.cycle()
        assert [int(r["ticket"]) for r in v.journal.open_trades()] == [ticket]
        assert not [r for r in v.journal.closed_trades(10) if int(r["ticket"]) == ticket]
        assert ticket in v.flowlock.trackers and ticket in {p.ticket for p in pb.positions(TNB)}
        assert len(pb.closed) == book and len(sim.order_log) == orders   # nothing booked, nothing sent
        v.broker.close_db()
        main.journal.close()
        pb.close_db()

    def test_verify_does_not_call_an_open_trade_an_order_sent(self, tmp_path):
        """The open paper trade is adopted by the dry run's reconciliation
        ('adopted:<ticket>'): a record of a trade the running trader holds,
        never an order (mintel/verify.py sent_any_order)."""
        from mintel.verify import sent_any_order
        sim, cfg = setup(tmp_path, runner="PAPER")
        pb = paper_on(sim, cfg)
        main = trader_on(pb, sim, cfg)
        act(main, sim, entry(sim, "EURUSD", 0.0030))
        v = dry_run_beside(sim, cfg, tmp_path)
        before = set(v.executor.intents)
        v.bootstrap()
        v.cycle()
        try:
            assert any(k.startswith("adopted:") for k in set(v.executor.intents) - before)   # the case in point
            assert sim.order_log == []                       # really nothing was sent
            assert sent_any_order(sim, v, before) is False
        finally:
            v.broker.close_db()
            main.journal.close()
            pb.close_db()

    def test_in_live_it_reads_the_real_broker_as_before(self, tmp_path):
        sim, cfg = setup(tmp_path, tnb="LIVE")
        v = dry_run_beside(sim, cfg, tmp_path)
        assert v.broker is sim and not list((tmp_path / "data").glob("tnb_paper.sqlite"))
        v.journal.close()

    def test_it_starts_none_of_the_other_bots(self, tmp_path):
        """A LIVE Runner closes, at start, any position with its number it
        has no record of yet - such as one the running Runner has just
        opened. The dry run's own copy must never be there to do it."""
        from mintel.broker.base import OrderRequest
        sim, cfg = setup(tmp_path, runner="LIVE")
        cfg.bandbreaker.mode = "LIVE"
        cfg.crowd.mode = "LIVE"
        t = sim.tick("US500")
        res = sim.send(OrderRequest("US500", Side.BUY, 1.0, sl=t.bid - 30.0, magic=MAGIC_RUNNER, comment="RUNNER"))
        assert res.ok
        orders, modifies = len(sim.order_log), len(sim.modify_log)
        v = dry_run_beside(sim, cfg, tmp_path)
        assert v.runner is None and v.bandbreaker is None and v.crowd is None
        v.bootstrap()
        v.cycle()
        assert [p.ticket for p in sim.positions(MAGIC_RUNNER)] == [res.ticket]
        assert len(sim.order_log) == orders and len(sim.modify_log) == modifies
        v.broker.close_db()
        v.journal.close()


# ------------------------------------------ the other bots' account gate --
class TestTheAccountGate:
    def test_closed_until_the_first_snapshot_then_open(self, tmp_path):
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr._bots_may_enter() is False                    # no risk snapshot taken yet: fails closed
        tr.cycle()
        assert tr._last_snapshot is not None and tr._last_snapshot.entries_allowed
        assert tr._bots_may_enter() is True
        tr.journal.close()
        pb.close_db()

    def test_the_daily_loss_stop_stops_the_live_runner_opening(self, tmp_path):
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "US500", 30.0))                 # a fresh paper index entry for the Runner
        tr.risk.realised_today = lambda: -1000.0                # far past the 3% daily-loss stop
        tr.cycle()
        assert any(r.startswith("DAILY_LOSS") for r in tr._last_snapshot.blocking_reasons())
        assert tr._bots_may_enter() is False
        assert sim.positions(MAGIC_RUNNER) == [] and sim.order_log == []
        assert tr.runner.last_note == "entries paused by the account gate"
        tr.journal.close()
        pb.close_db()
