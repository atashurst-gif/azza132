"""9 Oct, the clean-up after the line-up (Trend & Breakout on PAPER; the
Momentum Runner, the Rapid Momentum Rider and Financial Ian LIVE):

* The installer's check (``python -m mintel.verify``) runs a dry-run Trader
  beside the running one. It must never call a trade the running trader
  already holds "an order sent", never write Trend & Breakout's paper
  record, never move, close or open a REAL position, and never start the
  other bots a second time - on PAPER and on LIVE alike.
* Financial Ian's magic number is 990911 everywhere the reports look, as it
  is in Ian's own code: a "magic" in ian.json is ignored by Ian, so by them.
* The ten-minute pulse's ``today`` is the ACCOUNT's real UK day (the page's
  Account card), ``open`` every real open position on the account; Trend &
  Breakout's practice figure is shown apart, labelled, only on PAPER.

Every figure here is SYNTHETIC test data made by the tests themselves (a
simulator, a fake broker, and a real PaperBroker writing its own record on
the fake broker's prices): nothing in this file measures performance."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import socket
from pathlib import Path
from types import SimpleNamespace as NS

from mintel.botexec import MAGIC_RUNNER
from mintel.broker.base import AccountInfo, OrderRequest, Position, Side
from mintel.broker.paper import PaperBroker
from mintel.config import Config
from mintel.ops import pulse as pl
from mintel.ops.dashboard import DashboardState
from mintel.ops.standing import account_standing, bot_modes_and_magics, open_rows, period_view
from tests.test_paper_broker import FakeInner, make, req
from tests.test_tnb_paper_wiring import TNB, act, entry, move, paper_on, setup, trader_on

UTC = dt.timezone.utc
IAN, RIDER = 990_911, 990_811


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ===================================================== the installer's check --
def run_verify(monkeypatch, capsys, sim, cfg, tmp_path, after_build=None) -> tuple[int, str]:
    """``mintel.verify.verify`` exactly as the Mac installer runs it, with the
    simulator standing in for the MetaTrader bridge and its clock for the
    wall clock (so the price and clock checks judge the simulator's time).

    ``after_build``, if given, is called once the dry-run Trader is built and
    before it starts up: what the running trader does in that gap."""
    import mintel.broker.bridge_client as bridge_client
    import mintel.engine.trader as trader_mod
    from mintel import verify as vf
    cfg.broker_mode = "bridge"
    cfg.news.public_calendar_feed = False
    cfg.ops.dashboard_port = _free_port()                       # nothing answers: a warning, never a failure
    path = tmp_path / "config.json"
    cfg.save(path)
    sim.ping = lambda: {"backend": "sim"}
    sim.disconnect = lambda: None                               # the running trader keeps its connection
    monkeypatch.setattr(bridge_client, "BridgeBroker", lambda **kw: sim)
    monkeypatch.setattr(vf, "utcnow", lambda: sim.now)
    real_trader = trader_mod.Trader

    class OnTheSimulatorsClock(real_trader):
        def __init__(self, *a, **kw):
            kw.setdefault("clock", lambda: sim.now)
            kw.setdefault("enable_model", False)
            super().__init__(*a, **kw)
            if after_build is not None and kw.get("dry_run"):
                after_build()
    monkeypatch.setattr(trader_mod, "Trader", OnTheSimulatorsClock)
    capsys.readouterr()
    rc = vf.verify(str(path))
    return rc, capsys.readouterr().out


SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "US500")


class TestTheInstallersCheck:
    def test_on_paper_an_open_paper_trade_is_no_order_and_nothing_real_is_touched(self, tmp_path, monkeypatch,
                                                                                  capsys):
        sim, cfg = setup(tmp_path, tnb="PAPER", symbols=SYMBOLS)
        pb = paper_on(sim, cfg)
        main = trader_on(pb, sim, cfg)                          # the running trader, on PAPER
        act(main, sim, entry(sim, "EURUSD", 0.0030))
        main.manage(main.broker.positions(TNB), sim.now)
        paper_ticket = int(main.journal.open_trades()[0]["ticket"])
        assert pb.is_paper(paper_ticket)
        t = sim.tick("US500")                                   # the LIVE Runner's own real trade
        runner = sim.send(OrderRequest("US500", Side.BUY, 1.0, sl=t.bid - 30.0, magic=MAGIC_RUNNER, comment="RUNNER"))
        assert runner.ok
        record = tmp_path / "data" / "tnb_paper.sqlite"
        before = (_digest(record), len(pb.closed), len(sim.order_log), len(sim.modify_log))

        rc, out = run_verify(monkeypatch, capsys, sim, cfg, tmp_path)

        assert "[OK  ] no order was sent" in out and "[FAIL] no order was sent" not in out
        assert ("[OK  ] Trend & Breakout on PAPER - it only reads its paper record (1 paper trade open); "
                "nothing is booked or sent from here") in out
        assert "[OK  ] other bots not started twice" in out
        assert (_digest(record), len(pb.closed), len(sim.order_log), len(sim.modify_log)) == before
        assert [p.ticket for p in sim.positions(MAGIC_RUNNER)] == [runner.ticket]
        assert sim.positions(TNB) == []                         # no real Trend & Breakout trade appeared
        assert paper_ticket in {p.ticket for p in pb.positions(TNB)}
        assert [int(r["ticket"]) for r in main.journal.open_trades()] == [paper_ticket]
        assert "[FAIL]" not in out and rc == 0, out
        main.journal.close()
        pb.close_db()

    def test_on_paper_a_trade_opened_while_the_dry_run_starts_up_is_left_open_in_the_journal(
            self, tmp_path, monkeypatch, capsys):
        """The dry run's view of the paper record is read when it is built,
        and its trackers from the shared journal only later, at start-up. A
        paper trade the running trader opens in that gap must not be marked
        closed in the journal (at a made-up loss) by the dry run."""
        sim, cfg = setup(tmp_path, tnb="PAPER", symbols=SYMBOLS)
        pb = paper_on(sim, cfg)
        main = trader_on(pb, sim, cfg)                          # the running trader, on PAPER, nothing open
        assert main.journal.open_trades() == []
        opened = []

        def running_trader_opens_a_paper_trade():
            act(main, sim, entry(sim, "EURUSD", 0.0030))
            main.manage(main.broker.positions(TNB), sim.now)    # its tracker is saved in the journal
            opened.extend(int(r["ticket"]) for r in main.journal.open_trades())

        rc, out = run_verify(monkeypatch, capsys, sim, cfg, tmp_path, after_build=running_trader_opens_a_paper_trade)

        assert len(opened) == 1 and pb.is_paper(opened[0])
        assert [int(r["ticket"]) for r in main.journal.open_trades()] == opened
        assert [r for r in main.journal.closed_trades() if int(r["ticket"]) == opened[0]] == []
        assert opened[0] in {p.ticket for p in pb.positions(TNB)}
        assert "[OK  ] no order was sent" in out
        assert "[FAIL]" not in out and rc == 0, out
        main.journal.close()
        pb.close_db()

    def test_on_paper_a_record_that_cannot_be_read_again_skips_the_cycle_and_fails(self, tmp_path, monkeypatch,
                                                                                   capsys):
        sim, cfg = setup(tmp_path, tnb="PAPER", symbols=SYMBOLS)
        pb = paper_on(sim, cfg)
        main = trader_on(pb, sim, cfg)
        act(main, sim, entry(sim, "EURUSD", 0.0030))
        main.manage(main.broker.positions(TNB), sim.now)
        paper_ticket = int(main.journal.open_trades()[0]["ticket"])

        def locked(self):
            raise RuntimeError("database is locked")
        monkeypatch.setattr(PaperBroker, "reload", locked)
        rc, out = run_verify(monkeypatch, capsys, sim, cfg, tmp_path)

        assert ("[FAIL] Trend & Breakout's paper record read again - it could not be read again "
                "(database is locked), so the decision cycle was skipped") in out
        assert "full decision cycle" not in out and rc == 1
        assert [int(r["ticket"]) for r in main.journal.open_trades()] == [paper_ticket]
        main.journal.close()
        pb.close_db()

    def test_on_live_the_dry_run_never_moves_or_closes_the_real_trade(self, tmp_path, monkeypatch, capsys):
        """Before 9 Oct the dry run managed Trend & Breakout's real trade
        like the running trader does: here it would have part-closed it and
        moved its stop at the broker. Now every such call is refused."""
        sim, cfg = setup(tmp_path, tnb="LIVE", symbols=SYMBOLS)
        main = trader_on(sim, sim, cfg)
        act(main, sim, entry(sim, "EURUSD", 0.0030))
        main.manage(sim.positions(TNB), sim.now)
        for _ in range(3):                                      # far enough in profit to trail and take a part
            move(sim, "EURUSD", +0.0020)
        real = sim.positions(TNB)[0]
        sl, volume = real.sl, real.volume
        orders, modifies, closed = len(sim.order_log), len(sim.modify_log), len(sim.closed)

        rc, out = run_verify(monkeypatch, capsys, sim, cfg, tmp_path)

        assert "[OK  ] no order was sent" in out and "[FAIL] no order was sent" not in out
        assert f"it would have asked the broker to set ticket {real.ticket}'s stop" in out
        assert (len(sim.order_log), len(sim.modify_log), len(sim.closed)) == (orders, modifies, closed)
        now = sim.positions(TNB)[0]
        assert (now.ticket, now.sl, now.volume) == (real.ticket, sl, volume)
        assert "Trend & Breakout on PAPER" not in out
        assert "[FAIL]" not in out and rc == 0, out
        main.journal.close()

    def test_a_dry_run_that_cannot_be_built_is_a_failure_line_not_a_crash(self, tmp_path, monkeypatch, capsys):
        import mintel.engine.trader as trader_mod
        sim, cfg = setup(tmp_path, tnb="PAPER", symbols=SYMBOLS)

        def unreadable(broker, cfg):
            raise RuntimeError("the paper record could not be opened")
        monkeypatch.setattr(trader_mod, "dry_run_view", unreadable)
        rc, out = run_verify(monkeypatch, capsys, sim, cfg, tmp_path)
        assert "[FAIL] dry-run trader - could not be built: the paper record could not be opened" in out
        assert rc == 1 and "VERIFICATION STOPPED" not in out


class TestNoOrders:
    def test_every_order_call_is_refused_and_never_reaches_the_broker(self):
        from mintel.verify import NoOrders
        inner = FakeInner()                                     # its order methods raise if ever called
        guard = NoOrders(inner)
        assert not guard.send(req("EURUSD", Side.BUY, 0.5, sl=1.2)).ok
        assert not guard.modify_stops(5_000_001, 1.1, 1.3).ok
        assert not guard.close(5_000_001, 0.2).ok
        assert not guard.order_send("anything").ok and not guard.close_all().ok
        assert inner.order_calls == []
        assert guard.refused == ["open BUY 0.5 lots of EURUSD", "set ticket 5000001's stop to 1.1 and target to 1.3",
                                 "close ticket 5000001 (0.2 lots)", "call order_send", "call close_all"]

    def test_everything_it_reads_is_the_real_brokers(self):
        from mintel.verify import NoOrders
        inner = FakeInner()
        inner.real_positions.append(Position(5_000_002, "US500", Side.BUY, 1.0, 5000.0, 4970.0, 0.0, inner.now,
                                             magic=MAGIC_RUNNER))
        guard = NoOrders(inner)
        assert guard.positions(MAGIC_RUNNER) == inner.positions(MAGIC_RUNNER)
        assert guard.account() == inner.account() and guard.tick("GBPUSD") is inner.tick("GBPUSD")
        assert guard.clock is inner.clock and guard.spec("EURUSD") is inner.spec("EURUSD")


class TestSentAnyOrder:
    def test_an_adopted_trade_is_never_an_order(self):
        from mintel.verify import sent_any_order
        trader = NS(executor=NS(intents={"live-key": 1, "adopted:960000001": 2, "adopted:1001": 3}))
        assert sent_any_order(NS(), trader, {"live-key"}) is False

    def test_a_new_intent_or_a_new_logged_order_is_one(self):
        from mintel.verify import sent_any_order
        trader = NS(executor=NS(intents={"adopted:1001": 1, "new-key": 2}))
        assert sent_any_order(NS(), trader, set()) is True
        quiet = NS(executor=NS(intents={"adopted:1001": 1}))
        assert sent_any_order(NS(order_log=[{"req": 1}]), quiet, set(), orders_before=1) is False
        assert sent_any_order(NS(order_log=[{"req": 1}, {"req": 2}]), quiet, set(), orders_before=1) is True
        assert sent_any_order(NS(order_log=[{"req": 1}]), quiet, set()) is True      # no count given: strict


# ===================================================== Financial Ian's number --
def _with_ian_json(tmp_path, magic) -> Config:
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path)
    (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "magic": magic}))
    return cfg


class TestIansMagicIsIansOwn:
    def test_the_page_reads_990911_whatever_ian_json_says(self, tmp_path):
        from mintel.ian import MAGIC
        cfg = _with_ian_json(tmp_path, 123_456)
        assert MAGIC == IAN and bot_modes_and_magics(cfg)["financial_ian"] == (IAN, "LIVE")
        assert bot_modes_and_magics(cfg)["momentum_rider"][0] == RIDER

    def test_the_reconcile_report_reads_990911_whatever_ian_json_says(self, tmp_path):
        from mintel.ops.reconcile import bots
        cfg = _with_ian_json(tmp_path, RIDER)                  # even another bot's number: never read
        by = {k: m for k, _label, m in bots(cfg, 990_411)}
        assert by["ian"] == IAN and by["rider"] == RIDER and by["mi"] == TNB and by["rs"] == 990_411
        assert len(set(by.values())) == len(by)                # every bot its own number

    def test_ians_real_deals_land_on_ians_row(self, tmp_path):
        cfg = _with_ian_json(tmp_path, 123_456)
        now = dt.datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
        rows = [{"position": 7, "magic": IAN, "profit": -0.4, "commission": -0.4, "is_entry": True,
                 "time": now - dt.timedelta(hours=2)},
                {"position": 7, "magic": IAN, "profit": 6.1, "commission": -0.4, "is_entry": False,
                 "time": now - dt.timedelta(hours=1)}]
        broker = NS(clock=None, deals_since=lambda since, magic=0, closing_only=True: list(rows),
                    account=lambda: NS(balance=2005.7, equity=2005.7, currency="GBP"), positions=lambda m=None: [])
        sd = account_standing(broker, cfg, now, cache_seconds=0)
        by = {b["id"]: b for b in sd["bots"]}
        assert by["financial_ian"]["magic"] == IAN and by["financial_ian"]["today"] == 5.7
        assert sd["other"]["today"] == 0.0                    # nothing left over as "placed by hand"


# ============================================================== the pulse --
DAY = dt.datetime(2026, 10, 7, tzinfo=UTC)                     # FakeInner's day (a Wednesday)
NOW = dt.datetime(2026, 10, 7, 14, 0, tzinfo=UTC)
UK_DAY = dt.datetime(2026, 10, 6, 23, 0, tzinfo=UTC)            # 00:00 UK (BST) on 7 Oct: the page's today
LEFTOVER, RUN, RIDE, HAND = 5_000_001, 5_000_002, 5_000_003, 5_000_004


class Account(FakeInner):
    """The fake REAL broker: the balance is 2000 plus every real deal, so the
    account's figures are exactly the deals' sum, as at MetaTrader."""

    def account(self):
        bal = round(2000.0 + sum(float(r["profit"]) for r in self.real_deals), 2)
        eq = round(bal + sum(p.profit for p in self.real_positions), 2)
        return AccountInfo(login=1, server="ICMarketsSC-Demo", currency="GBP", balance=bal, equity=eq,
                           margin=0.0, margin_free=eq, margin_level=0.0, leverage=30, trade_allowed=True,
                           is_demo=True)


def _deal(pos, magic, profit, comm, entry_, when, symbol="US30"):
    return {"position": pos, "magic": magic, "symbol": symbol, "volume": 0.1, "profit": profit,
            "commission": comm, "is_entry": entry_, "time": when}


def an_account(tmp_path, *, paper: bool) -> NS:
    """Today on the account: a REAL Trend & Breakout trade (opened yesterday,
    closed today), a REAL Momentum Runner trade and a REAL Rider trade closed
    today, a hand trade; open now: the Runner, Ian and a hand trade. On
    PAPER also two Trend & Breakout PAPER trades, one closed today and one
    still open, booked by a real PaperBroker on the fake broker's prices."""
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    inner = Account()
    inner.real_deals = [
        _deal(LEFTOVER, TNB, -0.30, -0.30, True, DAY - dt.timedelta(hours=3)),
        _deal(LEFTOVER, TNB, -6.30, -0.30, False, DAY + dt.timedelta(hours=9)),
        _deal(RUN, MAGIC_RUNNER, 0.0, 0.0, True, DAY + dt.timedelta(hours=10)),
        _deal(RUN, MAGIC_RUNNER, 20.0, 0.0, False, DAY + dt.timedelta(hours=11)),
        _deal(RIDE, RIDER, -0.55, -0.55, True, DAY + dt.timedelta(hours=11), "EURUSD"),
        _deal(RIDE, RIDER, 4.45, -0.55, False, DAY + dt.timedelta(hours=12), "EURUSD"),
        _deal(HAND, 0, 0.0, 0.0, True, DAY + dt.timedelta(hours=8), "XAUUSD"),
        _deal(HAND, 0, -1.25, 0.0, False, DAY + dt.timedelta(hours=8, minutes=30), "XAUUSD"),
    ]
    inner.real_positions = [
        Position(5_000_010, "US500", Side.BUY, 1.0, 5000.0, 4970.0, 0.0, DAY + dt.timedelta(hours=12),
                 profit=3.10, magic=MAGIC_RUNNER),
        Position(5_000_011, "GBPUSD", Side.SELL, 0.2, 1.25, 1.252, 0.0, DAY + dt.timedelta(hours=12),
                 profit=-0.80, magic=IAN),
        Position(5_000_012, "XAUUSD", Side.BUY, 0.01, 2400.0, 0.0, 0.0, DAY + dt.timedelta(hours=12),
                 profit=0.40, magic=0),
    ]
    paper_open = None
    if paper:
        writer = make(inner, data)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        a = writer.send(req("EURUSD", Side.BUY, 0.5, sl=1.09500, key="a"))
        inner.set_tick("EURUSD", 1.10200, 1.10210, seconds=600)
        assert writer.close(a.ticket).ok
        b = writer.send(req("EURUSD", Side.BUY, 0.3, sl=1.09900, key="b"))
        assert b.ok
        paper_open = b.ticket
        writer.close_db()
        assert not inner.order_calls                            # nothing ever reached the real broker
    cfg = Config()
    cfg.ops.data_dir = str(data)
    cfg.ops.log_dir = str(tmp_path / "logs")
    cfg.tnb.mode = "PAPER" if paper else "LIVE"
    return NS(data=data, inner=inner, cfg=cfg, paper_open=paper_open)


def page_body(acct: NS) -> tuple[str, dict]:
    """The trader's /api body, built the way mintel/run.py push_dashboard
    builds it: Trend & Breakout's own day in the status block (on PAPER its
    wrapper's, paper and real together), the account's standing from the
    broker, and every open position on the account."""
    cfg, inner = acct.cfg, acct.inner
    paper = cfg.tnb.mode == "PAPER"
    broker = PaperBroker(inner, acct.data, read_only=True, now_fn=lambda: inner.now) if paper else inner
    account = broker.account()
    own = broker.positions(TNB)
    own_day = round(sum(float(r["profit"]) for r in broker.deals_since(DAY, TNB, False)), 2)
    every = broker.positions(None)
    sd = dict(account_standing(broker, cfg, NOW, account, every, cache_seconds=0))
    deals = sd.pop("deals", [])
    state = DashboardState()
    state.update(status={"bot": "RUNNING", "mt5_connected": True, "broker_connected": True,
                         "tnb_mode": "PAPER" if paper else "LIVE", "open_positions": len(own),
                         "equity": account.equity, "balance": account.balance, "currency": "GBP",
                         "today_pnl": own_day, "build": "test-build"},
                 standing=sd, open_live=open_rows(every, cfg), open_at=NOW.isoformat())
    if paper:
        broker.close_db()
    return json.dumps(state.snapshot(), indent=2, default=str), {"standing": sd, "deals": deals,
                                                                 "own_day": own_day, "account": account}


def pulse_of(acct: NS, body: str) -> dict:
    return pl.take_pulse(acct.cfg, NOW, fetch=lambda url: body, heartbeats={"strategy": {"age_seconds": 3},
                                                                           "watchdog": {"age_seconds": 4}},
                         bridge=lambda: {"mt5_connected": True}, port_is_open=lambda: True,
                         terminal_running=lambda: True)


def account_day_by_hand(inner) -> float:
    """The account's UK day, summed independently of the code under test:
    every real position whose last exit fell since 00:00 UK (and that is not
    still open), each in full - every deal of it, the commission charged
    when it opened too, even if that was before midnight."""
    open_now = {p.ticket for p in inner.real_positions}
    last: dict = {}
    for r in inner.real_deals:
        if not r["is_entry"]:
            last[r["position"]] = max(last.get(r["position"], r["time"]), r["time"])
    keep = {p for p, t in last.items() if UK_DAY <= t < UK_DAY + dt.timedelta(days=1) and p not in open_now}
    return round(sum(float(r["profit"]) for r in inner.real_deals if r["position"] in keep), 2)


def paper_day_by_hand(data: Path) -> float:
    """Trend & Breakout's paper positions whose last exit fell today (the UK
    day), every deal of each (the entry's commission too), from its own record."""
    view = PaperBroker(None, data, magic=TNB, read_only=True)
    try:
        rows = view.paper_deals_since(DAY - dt.timedelta(days=30), closing_only=False)
    finally:
        view.close_db()
    last: dict = {}
    for r in rows:
        if not r["is_entry"]:
            last[r["position"]] = max(last.get(r["position"], r["time"]), r["time"])
    keep = {p for p, t in last.items() if UK_DAY <= t < UK_DAY + dt.timedelta(days=1)}
    return round(sum(float(r["profit"]) for r in rows if r["position"] in keep), 2)


class TestThePulseFigures:
    def test_live_today_is_the_accounts_day_and_open_is_every_real_position(self, tmp_path):
        acct = an_account(tmp_path, paper=False)
        body, page = page_body(acct)
        p = pulse_of(acct, body)
        expected = account_day_by_hand(acct.inner)
        # Trend & Breakout's leftover (opened 22:00 UK on the 6th, closed today)
        # in full, both commissions; the Runner; the Rider in full; the hand trade
        assert expected == round((-0.30 - 6.30) + 20.0 + (-0.55 + 4.45) - 1.25, 2) == 16.05
        assert p["up"] is True and p["reason"] == ""
        assert p["today_pnl"] == expected == page["standing"]["today"]
        card = period_view(page["standing"], page["deals"], "today", now=NOW)
        assert card["made"] == expected                       # the Account card's Today
        assert page["own_day"] == -6.30 and p["today_pnl"] != page["own_day"]   # never Trend & Breakout's own day
        assert p["open_positions"] == 3 == len(acct.inner.real_positions)       # every bot's, and the hand trade
        assert p["equity"] == page["account"].equity and p["balance"] == page["account"].balance
        assert p["tnb_mode"] == "LIVE" and "tnb_practice_today" not in p
        line = pl.pulse_line(p)
        assert f"today={expected:+.2f}" in line and "open=3" in line and "tnb" not in line
        assert line.startswith("2026-10-07T14:00:00Z UP")
        slim = json.loads(pl.pulse_files(p, [line])["reports/live/pulse.json"])
        for k in ("up", "reason", "today_pnl", "equity", "open_positions"):
            assert k in slim, k                                # the names the nightly review reads

    def test_paper_practice_is_apart_labelled_and_never_in_today(self, tmp_path):
        acct = an_account(tmp_path, paper=True)
        record = acct.data / "tnb_paper.sqlite"
        stamp = _digest(record)
        body, page = page_body(acct)
        p = pulse_of(acct, body)
        expected = account_day_by_hand(acct.inner)
        practice = paper_day_by_hand(acct.data)
        assert practice != 0.0                                 # a real paper result to tell apart
        assert p["today_pnl"] == expected == page["standing"]["today"]
        assert period_view(page["standing"], page["deals"], "today", now=NOW)["made"] == expected
        assert p["today_pnl"] != page["own_day"]               # the wrapper's day holds the paper money
        assert p["tnb_mode"] == "PAPER" and p["tnb_practice_today"] == practice
        assert "tnb_practice_error" not in p
        assert p["open_positions"] == 3                        # the open PAPER trade is not on the account
        assert acct.paper_open not in {r["ticket"] for r in json.loads(body)["open_live"]}
        line = pl.pulse_line(p)
        assert f"today={expected:+.2f}" in line and "open=3" in line
        assert line.endswith(f"tnb=PAPER tnb_practice={practice:+.2f}")
        assert _digest(record) == stamp                        # the pulse only reads the paper record

    def test_paper_with_no_paper_trade_yet_is_zero_practice(self, tmp_path):
        acct = an_account(tmp_path, paper=False)
        acct.cfg.tnb.mode = "PAPER"                            # switched, nothing booked on paper yet
        body, _page = page_body(acct)
        p = pulse_of(acct, body)
        assert p["tnb_practice_today"] == 0.0 and p["today_pnl"] == account_day_by_hand(acct.inner)

    def test_no_account_figure_is_a_question_mark_never_a_guess(self, tmp_path):
        acct = an_account(tmp_path, paper=True)
        body = json.dumps({"status": {"bot": "RUNNING", "mt5_connected": True, "tnb_mode": "PAPER",
                                      "equity": 2010.0, "today_pnl": 3.2, "open_positions": 1},
                           "standing": {"error": "deal history not readable: timed out", "today": None},
                           "open_live": None, "health": {}})
        p = pulse_of(acct, body)
        assert p["today_pnl"] is None and p["open_positions"] is None
        assert p["account_error"] == "deal history not readable: timed out"
        assert p["tnb_practice_today"] is None and p["tnb_practice_error"]
        assert pl.pulse_line(p).endswith("UP   equity=2010.00 open=? today=? tnb=PAPER tnb_practice=?")

    def test_a_page_that_does_not_say_the_mode_falls_back_to_the_setting(self, tmp_path):
        acct = an_account(tmp_path, paper=True)
        body, _page = page_body(acct)
        raw = json.loads(body)
        raw["status"].pop("tnb_mode")
        p = pulse_of(acct, json.dumps(raw))
        assert p["tnb_mode"] == "PAPER" and p["tnb_practice_today"] == paper_day_by_hand(acct.data)
