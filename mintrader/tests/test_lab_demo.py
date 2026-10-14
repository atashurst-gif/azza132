"""14 Oct: promoted lab ideas trade on the DEMO account (mintel/labdemo).

Against the simulated broker (mintel/broker/sim.py SimBroker), which holds the
stop at the broker and fills it on a bar's extreme, and keeps closed deals:

* an entry is a genuine order with its stop AT THE BROKER, under the idea's own
  magic number, sized from equity x its risk tier and the stop distance;
* the simulator's own exits (time, trail) are acted on at the broker; a stop the
  broker filled is noticed; every close is booked from the broker's deal into
  data/labdemo/labdemo.sqlite with its R and its £1,500-account equivalent;
* a restart re-adopts an open position; one gone while the trader was away is
  booked from its deal; a position under a lab magic with no record is closed;
* not a demo account (or config.json not DEMO): nothing is ever sent;
* the smallest lot over the allowed risk, the margin, and the combined open-risk
  cap: skipped with the reason recorded;
* an idea taken off the roster is closed at once, without a restart; HALT stops
  entries;
* the lab's paper record is never written by it (PAPER and DEMO never mixed).

The clock-trade family (CONTROL_TIME_OF_DAY) is used here only because it trades
at a known minute; the squad's pick never promotes a control idea
(tests/test_promotion_rules.py). Verified against the SIMULATED broker only.

EVERY FIGURE HERE IS MADE-UP TEST DATA.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from mintel.broker.base import OrderRequest, Side
from mintel.broker.sim import SimBroker
from mintel.lab import Candidate, LabPaths
from mintel.lab.families import get as family
from mintel.lab.ledger import LabLedger
from mintel.lab.store import UniverseEntry, write_universe
from mintel.labdemo import record as REC
from mintel.labdemo import roster as R
from mintel.labdemo.engine import DemoOnlyExecutor, LabDemo, NotDemo, from_trader, risk_cap_pct

UTC = dt.timezone.utc
START = dt.datetime(2026, 10, 13, 6, 0, tzinfo=UTC)        # Tuesday 13 Oct, 07:00 UK (BST)
ENTRY_AT = dt.datetime(2026, 10, 13, 9, 1, tzinfo=UTC)     # 10:00 UK bar closes, the order goes at 09:01 UTC


class Sim(SimBroker):
    """The simulated broker with one steady regime: its own picks a regime by ``hash(symbol)``, which Python
    salts per process, so its made-up prices would differ from run to run."""

    def _regime_for(self, i, sym):
        return ("RANGE", 0.0, 1.0)


def clock_idea(exit_code: str = "time1h", hour: int = 10, side: str = "BUY") -> Candidate:
    ctl = family("CONTROL_TIME_OF_DAY")
    base = next(c for c in ctl.grid() if c.markets == "FX_MAJOR" and c.p("hour_uk") == hour and c.p("side") == side)
    return base if base.exit == exit_code else base.replace(exit=exit_code)


def world(tmp_path: Path, *, is_demo: bool = True, balance: float = 2000.0, exit_code: str = "time1h",
          tier: float = 1.0, side: str = "BUY") -> NS:
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    b = Sim(["EURUSD"], start=START, history_bars=3000, balance=balance, is_demo=is_demo, seed=7)
    b.connect()
    paths = LabPaths(data)
    write_universe(paths, [UniverseEntry("EURUSD", "FX_MAJOR", b.spec("EURUSD"))])
    c = clock_idea(exit_code, side=side)
    led = LabLedger(paths.sqlite)
    led.register(c, "made-up clock trade")
    led.close()
    promote(data, c, tier)
    clock = {"t": START}
    logs: list = []
    eng = LabDemo(data, b, cfg_mode="DEMO", now_fn=lambda: clock["t"], log_fn=logs.append, prime_bars=3000)
    return NS(data=data, broker=b, cand=c, clock=clock, logs=logs, eng=eng, paths=paths)


def promote(data: Path, c: Candidate, tier: float = 1.0) -> int:
    magics = R.read_magics(data)
    magic, magics = R.magic_for(magics, c.id, c.version)
    ideas = R.read_roster(data)
    ideas[c.id] = {"version": c.version, "magic": magic, "tier_pct": tier, "since_utc": START.isoformat(),
                   "plan": "2026-10-12"}
    R.write_roster(data, ideas, magics)
    return magic


def tick_to(w: NS, until: dt.datetime, eng: LabDemo = None) -> None:
    """The simulated market moves a minute at a time; the engine steps 3 s past each minute."""
    eng = eng or w.eng
    while w.broker.now < until:
        w.broker.advance(1)
        w.clock["t"] = w.broker.now + dt.timedelta(seconds=3)
        eng.step(w.clock["t"])


def flat_bars(w: NS, n: int, eng: LabDemo = None, drift: float = 0.0) -> None:
    """Hand-made bars that touch neither the stop nor a target (``drift`` a bar in price)."""
    eng = eng or w.eng
    for _ in range(n):
        last = w.broker._bars["EURUSD"][-1].close
        nxt = last + drift
        w.broker.push_bar("EURUSD", nxt, high=max(last, nxt) + 0.00002, low=min(last, nxt) - 0.00002)
        w.clock["t"] = w.broker.now + dt.timedelta(seconds=3)
        eng.step(w.clock["t"])


def demo_trades(data: Path) -> list[dict]:
    return REC.read_trades(data) or []


def orders(w: NS) -> list[OrderRequest]:
    return [o["req"] for o in w.broker.order_log]


# ======================================================= a genuine order, booked --
class TestTheOrder:
    def test_submit_manage_close_and_the_deal_recorded(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        sent = orders(w)
        assert len(sent) == 1
        req = sent[0]
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        assert R.MAGIC_FIRST <= magic <= R.MAGIC_LAST and req.magic == magic
        assert req.side is Side.BUY and req.sl > 0 and req.sl < w.broker.tick("EURUSD").bid    # the stop
        pos = w.broker.positions(magic)
        assert len(pos) == 1 and pos[0].sl == req.sl                              # ... held AT THE BROKER
        # sized from equity x the tier and the stop distance, floored to the lot step
        spec = w.broker.spec("EURUSD")
        risk_at_stop = (pos[0].entry_price - pos[0].sl) / spec.tick_size * spec.tick_value * pos[0].volume
        # rv14 #19: at 1:30 the 300% margin-level floor cuts it below half the tier (it was > 0.5 x the tier)
        assert 0 < risk_at_stop <= 0.01 * 2000.0 + 1e-6 and w.broker.account().margin_level >= 300.0
        # flat prices: neither the stop nor a target - out at the hold (an hour), a market close at the broker
        flat_bars(w, 65)
        assert w.broker.positions(magic) == []
        rows = demo_trades(w.data)
        assert len(rows) == 1
        r = rows[0]
        deal = w.broker.closed_deal(r["ticket"])
        assert r["exit_reason"] == "TIME" and r["net_gbp"] == round(deal["pnl"], 2)           # the broker's figure
        assert r["idea_id"] == w.cand.id and r["version"] == w.cand.version and r["magic"] == magic
        # 1% of £2,000 allowed; the money really at its stop (the broker's fill, the stop, the volume) is the R's
        # basis - here cut below the tier to keep the margin level at 300% (1:30 on a £2,000 account)
        assert r["risk_allowed_gbp"] == 20.0 and 0 < r["risk_gbp"] <= 20.0
        assert r["risk_gbp"] == pytest.approx(risk_at_stop, abs=0.02)
        assert r["tier_pct"] == 1.0 and r["mode"] == "DEMO"
        assert r["r"] == pytest.approx(r["net_gbp"] / r["risk_gbp"], abs=1e-4)
        assert r["equiv_1500_gbp"] == round(r["r"] * 7.5, 2) == REC.equiv_1500(r["r"])        # R x 0.5% of £1,500
        assert r["opened_utc"] == "2026-10-13T09:01:00+00:00"
        # the lab's paper record is untouched: PAPER and DEMO never mixed
        con = sqlite3.connect(w.paths.sqlite)
        assert con.execute("SELECT COUNT(*) FROM practice_trades").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM practice_open").fetchone()[0] == 0
        con.close()
        assert json.loads((w.data / "labdemo" / "status.json").read_text())["mode"] == "DEMO"

    def test_a_stop_the_broker_filled_is_booked_from_its_deal(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        sl = w.broker.positions(magic)[0].sl
        last = w.broker._bars["EURUSD"][-1].close
        w.broker.push_bar("EURUSD", sl - 0.0002, high=last, low=sl - 0.0003)            # through the stop
        w.clock["t"] = w.broker.now + dt.timedelta(seconds=3)
        w.eng.step(w.clock["t"])
        assert w.broker.positions(magic) == []
        r = demo_trades(w.data)[0]
        assert r["exit_reason"] == "STOP" and r["net_gbp"] < 0 and r["exit_price"] == sl
        assert r["r"] == pytest.approx(-1.0, abs=0.2)

    def test_the_trail_tightens_the_broker_stop_and_never_loosens_it(self, tmp_path):
        w = world(tmp_path, exit_code="tr1")
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        first = w.broker.positions(magic)[0].sl
        flat_bars(w, 40, drift=0.00012)                                           # a steady climb
        mods = [m for m in w.broker.modify_log if m["ticket"] == w.broker.order_log[0]["ticket"]]
        assert mods, w.logs[-10:]
        stops = [m["sl"] for m in mods]
        assert stops == sorted(stops) and stops[0] > first                        # only ever tighter
        held = w.broker.positions(magic)
        assert held and held[0].sl == stops[-1]
        assert all(o["req"].sl for o in w.broker.order_log)                       # every order had its stop


# ======================================================== restart re-adoption --
class TestRestart:
    def test_an_open_position_is_re_adopted_and_not_traded_twice(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        # the trader restarts: a new engine on the same files and the same account
        logs2: list = []
        eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=logs2.append,
                       prime_bars=3000)
        flat_bars(w, 65, eng=eng2)
        assert any(f"ticket {ticket} re-adopted" in m for m in logs2)
        assert len(w.broker.order_log) == 1                                       # no second entry
        rows = demo_trades(w.data)
        assert [(r["ticket"], r["exit_reason"]) for r in rows] == [(ticket, "TIME")]

    def test_a_position_closed_while_away_is_booked_from_its_deal(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        w.broker.close(ticket, 0.0, "by hand")                                     # while the trader was down
        eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=lambda m: None,
                       prime_bars=3000)
        flat_bars(w, 2, eng=eng2)
        rows = demo_trades(w.data)
        assert len(rows) == 1 and rows[0]["ticket"] == ticket
        assert rows[0]["net_gbp"] == round(w.broker.closed_deal(ticket)["pnl"], 2)

    def test_a_position_under_a_lab_magic_with_no_record_is_closed(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, START + dt.timedelta(minutes=5))
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        tk = w.broker.tick("EURUSD")
        w.broker.send(OrderRequest("EURUSD", Side.BUY, 0.01, sl=tk.bid - 0.002, magic=magic, comment="stray"))
        assert w.broker.positions(magic)
        flat_bars(w, 2)
        assert w.broker.positions(magic) == []                                    # fail closed, never adopted
        assert [(r["exit_reason"], r["idea_id"]) for r in demo_trades(w.data)] == [("ORPHAN", w.cand.id)]
        # an orphan beside the idea's own open position on the same market never disturbs it
        tick_to(w, ENTRY_AT)
        own = w.broker.positions(magic)
        assert len(own) == 1
        tk = w.broker.tick("EURUSD")
        w.broker.send(OrderRequest("EURUSD", Side.BUY, 0.01, sl=tk.bid - 0.002, magic=magic, comment="stray"))
        flat_bars(w, 2)
        assert [q.ticket for q in w.broker.positions(magic)] == [own[0].ticket]
        assert (w.cand.id, "EURUSD") in w.eng.open
        flat_bars(w, 65)
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["ORPHAN", "ORPHAN", "TIME"]


# ================================================================= demo only --
class TestDemoOnly:
    def test_a_real_money_account_gets_no_order(self, tmp_path):
        w = world(tmp_path, is_demo=False)
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=3))
        assert w.broker.order_log == [] and w.broker.positions(None) == []
        assert any("not a demo account" in m for m in w.logs)
        con = sqlite3.connect(REC.db_path(w.data))
        assert any("not a demo account" in r[0] for r in con.execute("SELECT why FROM demo_skips"))
        con.close()

    def test_the_executor_refuses_every_call_off_a_demo_account(self):
        b = SimBroker(["EURUSD"], start=START, is_demo=False)
        b.connect()
        ex = DemoOnlyExecutor(b, 992_001, "LAB", "DEMO")
        tk, spec = b.tick("EURUSD"), b.spec("EURUSD")
        fill = ex.open("EURUSD", Side.BUY, 0.01, tk.bid - 0.002, 0.0, tk, spec, START)
        assert not fill.ok and "not a demo account" in fill.message and b.order_log == []
        assert not ex.modify_stop(1, 1.0) and not ex.close(1, tk, spec).ok
        # and on a demo account with config.json saying LIVE (real money proper): refused too
        b2 = SimBroker(["EURUSD"], start=START, is_demo=True)
        b2.connect()
        ex2 = DemoOnlyExecutor(b2, 992_001, "LAB", "LIVE")
        assert not ex2.open("EURUSD", Side.BUY, 0.01, tk.bid - 0.002, 0.0, tk, spec, START).ok and b2.order_log == []
        # an order with no stop is never sent, even on a demo account
        ex3 = DemoOnlyExecutor(b2, 992_001, "LAB", "DEMO")
        assert not ex3.open("EURUSD", Side.BUY, 0.01, 0.0, 0.0, tk, spec, START).ok and b2.order_log == []

    def test_the_engine_is_never_built_off_demo(self, tmp_path):
        b = SimBroker(["EURUSD"], start=START)
        b.connect()
        with pytest.raises(NotDemo):
            LabDemo(tmp_path, b, cfg_mode="LIVE")
        assert from_trader(tmp_path, b, NS(mode="LIVE")) is None
        assert from_trader(tmp_path, b, NS(mode="DEMO")) is None                  # no lab markets yet

    def test_an_account_that_cannot_be_read_is_not_a_demo_account(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT - dt.timedelta(minutes=1))
        real = w.broker.account
        w.broker.account = lambda: (_ for _ in ()).throw(RuntimeError("bridge down"))
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        w.broker.account = real
        assert w.broker.order_log == []


# ============================================================= size and room --
def skips(data: Path) -> list[str]:
    con = sqlite3.connect(REC.db_path(data))
    try:
        return [r[0] for r in con.execute("SELECT why FROM demo_skips")]
    finally:
        con.close()


class TestSizing:
    def test_the_smallest_lot_over_the_allowed_risk_is_skipped(self, tmp_path):
        w = world(tmp_path, balance=40.0, tier=0.5)                              # 0.5% of £40 = £0.20
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert w.broker.order_log == []
        assert any(s.startswith("smallest_size: the broker's smallest lot (0.01) would risk more than the £0.20")
                   for s in skips(w.data))

    def test_not_enough_margin_is_skipped(self, tmp_path):
        w = world(tmp_path)
        w.broker.margin_factor = 50.0
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert w.broker.order_log == [] and any(s.startswith("margin: even 0.01 lot") for s in skips(w.data))

    def test_the_combined_open_risk_cap(self, tmp_path):
        def other_bot(w, share):
            """Another bot's position on the account whose stop puts ``share`` of the equity at risk."""
            tk, spec = w.broker.tick("EURUSD"), w.broker.spec("EURUSD")
            dist = 0.0100
            vol = round(share * 2000.0 / (dist / spec.tick_size * spec.tick_value), 2)
            w.broker.send(OrderRequest("EURUSD", Side.BUY, vol, sl=tk.bid - dist, magic=990_611, comment="BB"))
        w = world(tmp_path / "full")
        tick_to(w, ENTRY_AT - dt.timedelta(minutes=30))
        other_bot(w, 0.101)                                                       # over 10% at risk already
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert [o["req"].magic for o in w.broker.order_log] == [990_611]
        assert any(s.startswith("risk_cap:") and "10% of equity" in s for s in skips(w.data))
        # with a little room left, the order is cut to fit it - never larger than the tier
        w = world(tmp_path / "room")
        w.broker.margin_factor = 0.2                                              # the margin never binds here
        tick_to(w, ENTRY_AT - dt.timedelta(minutes=30))
        other_bot(w, 0.097)
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        mine = [o["req"] for o in w.broker.order_log if o["req"].magic != 990_611]
        assert len(mine) == 1 and any("cut to" in m and "the risk cap" in m for m in w.logs)
        spec = w.broker.spec("EURUSD")
        at_risk = sum((p.entry_price - p.sl) / spec.tick_size * spec.tick_value * p.volume
                      for p in w.broker.positions(None))
        assert at_risk <= 0.10 * w.broker.account().equity + 0.5
        (w.data / "weekly.json").write_text(json.dumps({"demo_risk_cap_pct": 15}))
        assert risk_cap_pct(w.data) == 15.0
        (w.data / "weekly.json").write_text(json.dumps({"demo_risk_cap_pct": 90}))
        assert risk_cap_pct(w.data) == 10.0                                         # never more than 20

    def test_the_tiers_size_the_order(self, tmp_path):
        vols = {}
        for tier in (0.5, 1.0, 2.0):
            w = world(tmp_path / str(tier), tier=tier)
            w.broker.margin_factor = 0.2                                          # the margin never binds here
            tick_to(w, ENTRY_AT)
            vols[tier] = w.broker.order_log[0]["req"].volume
            r = REC.DemoRecord(w.data).open_rows()[0]
            assert r["tier_pct"] == tier and r["risk_allowed_gbp"] == round(2000.0 * tier / 100.0, 2)
        assert vols[0.5] < vols[1.0] < vols[2.0]
        assert vols[2.0] == pytest.approx(4 * vols[0.5], abs=0.04)


# =========================================================== roster and gates --
class TestRoster:
    def test_a_demoted_idea_is_closed_at_once_without_a_restart(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        assert w.broker.positions(magic)
        R.write_roster(w.data, {}, None)                                          # the squad's pick took it back
        flat_bars(w, 2)
        assert w.broker.positions(magic) == []
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["DEMOTED"]
        flat_bars(w, 70)
        assert len(w.broker.order_log) == 1                                       # and nothing new
        # re-promoted later, the same idea version keeps its magic number
        assert promote(w.data, w.cand) == magic

    def test_halt_stops_entries_and_never_management(self, tmp_path):
        w = world(tmp_path)
        (w.data / "HALT").write_text("")
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert w.broker.order_log == [] and any("data/HALT" in s for s in skips(w.data))

    def test_the_account_gate_shuts_entries(self, tmp_path):
        w = world(tmp_path)
        w.eng.entries_allowed = lambda: False
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert w.broker.order_log == [] and any("account gate" in s for s in skips(w.data))

    def test_a_broken_roster_shuts_entries_and_keeps_positions(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        R.roster_path(w.data).write_text("{ half a file")
        flat_bars(w, 3)
        assert w.broker.positions(magic)                                          # never closed on a guess
        assert not w.eng.roster_ok

    def test_magic_numbers_are_reserved_and_never_reused(self, tmp_path):
        m = R.read_magics(tmp_path)
        got = []
        for k in range(5):
            mg, m = R.magic_for(m, f"lab-{k:010d}", 1)
            got.append(mg)
        assert got == [992_001, 992_002, 992_003, 992_004, 992_005]
        assert R.magic_for(m, "lab-0000000002", 1)[0] == 992_003                  # the same idea, the same number
        assert R.magic_for(m, "lab-0000000002", 2)[0] == 992_006                  # a new version: a new number
        assert not (set(range(R.MAGIC_FIRST, R.MAGIC_LAST + 1)) & R.OTHER_MAGICS)
        from mintel.botexec import MAGIC_BANDBREAKER, MAGIC_CROWD, MAGIC_RUNNER
        assert {MAGIC_BANDBREAKER, MAGIC_CROWD, MAGIC_RUNNER} <= R.OTHER_MAGICS


def test_the_trader_counts_lab_demo_money_in_the_daily_loss_stop(tmp_path):
    from mintel.engine.trader import Trader
    w = world(tmp_path)
    t = Trader.__new__(Trader)                     # only the method under test; nothing is started
    t.cfg = NS(magic=990_311, runner=NS(magic=990_511), bandbreaker=NS(magic=990_611), crowd=NS(magic=990_711),
               ops=NS(data_dir=str(w.data)))
    t.labdemo = w.eng
    assert R.read_roster(w.data)[w.cand.id]["magic"] in t.stop_gated_magics()


def test_a_restart_keeps_each_ideas_pause_and_day_cap(tmp_path):
    w = world(tmp_path)
    tick_to(w, ENTRY_AT)
    flat_bars(w, 65)                                                              # in and out (the hour's hold)
    slot = w.eng.slots[w.cand.id]
    assert slot.entries and slot.next_ok
    eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=lambda m: None,
                   prime_bars=3000)
    flat_bars(w, 1, eng=eng2)
    assert eng2.slots[w.cand.id].entries == slot.entries and eng2.slots[w.cand.id].next_ok == slot.next_ok


# ============================================== review rv14 (14 Oct) fixes --
def _an_empty_read(w: NS, eng: LabDemo = None, minutes: int = 1) -> None:
    """The MT5 adapter answers an error (or a terminal not yet connected) with an empty positions list."""
    real = w.broker.positions
    w.broker.positions = lambda magic=None: []
    try:
        flat_bars(w, minutes, eng=eng)
    finally:
        w.broker.positions = real


class TestAnEmptyReadIsNotAClose:
    """rv14 #1: one empty positions() read must never strand a position as closed while the broker holds it."""

    def test_the_position_is_managed_again_and_its_time_exit_booked(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        _an_empty_read(w)
        flat_bars(w, 1)
        o = w.eng.open[(w.cand.id, "EURUSD")]
        assert o.closing == "" and REC.DemoRecord(w.data).open_rows()[0]["closing"] == ""
        assert any(f"ticket {ticket} is still open at the broker: managed again" in m for m in w.logs)
        flat_bars(w, 70)                                                          # the hour's hold
        assert w.broker.positions(magic) == []
        assert [(r["ticket"], r["exit_reason"]) for r in demo_trades(w.data)] == [(ticket, "TIME")]

    def test_a_restart_before_the_terminal_connects_keeps_managing_it(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=lambda m: None,
                       prime_bars=3000)
        _an_empty_read(w, eng=eng2)                                               # restored on an empty list
        flat_bars(w, 70, eng=eng2)
        assert w.broker.positions(magic) == []
        assert [(r["ticket"], r["exit_reason"]) for r in demo_trades(w.data)] == [(ticket, "TIME")]

    def test_a_demotion_after_an_empty_read_still_closes_it(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        _an_empty_read(w)
        R.write_roster(w.data, {}, None)                                          # demoted
        flat_bars(w, 3)
        assert w.broker.positions(magic) == []
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["DEMOTED"]

    def test_an_exit_decided_during_an_empty_read_is_sent_again(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        flat_bars(w, 59)
        assert w.broker.positions(magic)
        _an_empty_read(w, minutes=3)                                              # the TIME exit falls in it
        flat_bars(w, 3)
        assert w.broker.positions(magic) == []
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["TIME"]


class TestARosterUnreadableAtARestart:
    """rv14 #2: an unread roster at a restart decides nothing, and the saved pauses and day caps are kept."""

    def test_re_adopted_positions_are_not_closed_and_the_caps_survive(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        state = w.data / "labdemo" / "state.json"
        saved = json.loads(state.read_text())["slots"][w.cand.id]
        assert saved["entries"]
        good = R.roster_path(w.data).read_text()
        R.roster_path(w.data).write_text("{ half a file")
        logs2: list = []
        eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=logs2.append,
                       prime_bars=3000)
        flat_bars(w, 3, eng=eng2)
        assert w.broker.positions(magic) and demo_trades(w.data) == []           # never closed on a guess
        assert not any("SESSION_END" in m for m in logs2)
        assert json.loads(state.read_text())["slots"][w.cand.id]["entries"] == saved["entries"]   # not wiped
        R.roster_path(w.data).write_text(good)
        flat_bars(w, 1, eng=eng2)
        assert eng2.slots[w.cand.id].entries == saved["entries"]                 # the day cap kept
        assert (w.cand.id, "EURUSD") in eng2.open and w.broker.positions(magic)
        flat_bars(w, 65, eng=eng2)
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["TIME"]


def test_a_partly_filled_close_is_booked_once_whole_with_its_r(tmp_path):
    """rv14 #3: a DONE_PARTIAL close books nothing; the rest is closed next minute and the trade booked once."""
    w = world(tmp_path)
    tick_to(w, ENTRY_AT)
    magic = R.read_roster(w.data)[w.cand.id]["magic"]
    vol = w.broker.positions(magic)[0].volume
    assert vol >= 0.02
    real_close, n = w.broker.close, {"calls": 0}

    def half_close(ticket, volume=0.0, comment=""):
        n["calls"] += 1
        return real_close(ticket, round(vol / 2, 2) if n["calls"] == 1 else volume, comment)
    w.broker.close = half_close
    flat_bars(w, 70)
    assert w.broker.positions(magic) == [] and n["calls"] == 2
    rows = demo_trades(w.data)
    assert len(rows) == 1
    r = rows[0]
    deal = w.broker.closed_deal(r["ticket"])
    assert r["exit_reason"] == "TIME" and r["idea_id"] == w.cand.id and r["risk_gbp"] > 0
    assert deal["volume"] == pytest.approx(vol) and r["net_gbp"] == round(deal["pnl"], 2)   # the whole round trip
    assert r["r"] == pytest.approx(r["net_gbp"] / r["risk_gbp"], abs=1e-4)
    assert any("filled" in m and "closed next minute" in m for m in w.logs)


def test_the_pause_after_an_exit_runs_from_the_exit_bar_as_practice(tmp_path):
    """rv14 #4: next_ok = the exit bar + COOLDOWN_BARS (Practice._book), not the minute it was booked + 5."""
    from mintel.lab import COOLDOWN_BARS
    w = world(tmp_path)
    tick_to(w, ENTRY_AT)
    seen: list = []
    real_exit = w.eng._exit

    def spy(o, reason, t):
        seen.append((reason, t))
        return real_exit(o, reason, t)
    w.eng._exit = spy
    flat_bars(w, 70)
    assert [r["exit_reason"] for r in demo_trades(w.data)] == ["TIME"]
    (reason, t_exit), = seen
    assert reason == "TIME"
    assert w.eng.slots[w.cand.id].next_ok["EURUSD"] == t_exit + COOLDOWN_BARS


class TestTheMarginLevelFloor:
    """rv14 #19: a lab entry never takes the account below risk.min_margin_level_pct (the trader's breaker)."""

    def test_one_entry_and_two_in_the_same_minute_keep_the_level(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        assert len(w.broker.order_log) == 1
        acct = w.broker.account()
        assert acct.margin > 0 and acct.margin_level >= 300.0
        assert any("cut to" in m and "the margin level" in m for m in w.logs)
        # two ideas signalling in the same minute: the second is sized on the account as the first left it -
        # here no room is left above the floor, so it is skipped (it used to take half the free margin left)
        w2 = world(tmp_path / "two")
        c2 = clock_idea("tr1")
        led = LabLedger(w2.paths.sqlite)
        led.register(c2, "made-up clock trade, trailed")
        led.close()
        promote(w2.data, c2)
        tick_to(w2, ENTRY_AT)
        assert len(w2.broker.order_log) == 1 and any(s.startswith("margin level:") for s in skips(w2.data))
        assert w2.broker.account().margin_level >= 300.0
        # with less margin a lot, both fit, and the level still holds
        w3 = world(tmp_path / "both", tier=0.5)
        w3.broker.margin_factor = 0.2
        led = LabLedger(w3.paths.sqlite)
        led.register(c2, "made-up clock trade, trailed")
        led.close()
        promote(w3.data, c2, 0.5)
        tick_to(w3, ENTRY_AT)
        assert len(w3.broker.order_log) == 2 and w3.broker.account().margin_level >= 300.0

    def test_no_room_above_the_floor_is_skipped_and_the_setting_comes_from_config(self, tmp_path):
        w = world(tmp_path)
        w.eng.min_margin_level_pct = 100_000.0
        tick_to(w, ENTRY_AT + dt.timedelta(minutes=2))
        assert w.broker.order_log == [] and any(s.startswith("margin level:") for s in skips(w.data))
        assert from_trader(w.data, w.broker, NS(mode="DEMO", risk=NS(min_margin_level_pct=250.0))) \
            .min_margin_level_pct == 250.0
        assert from_trader(w.data, w.broker, NS(mode="DEMO")).min_margin_level_pct == 300.0


def test_the_daily_loss_stop_reads_the_broker_twice_whatever_the_number_of_lab_magics(tmp_path):
    """rv14 #20: one deal-history read and one positions read, filtered by the gated magics - never two calls
    for every lab magic ever handed out."""
    from mintel.engine.trader import Trader
    magics = R.read_magics(tmp_path)
    for k in range(60):
        _m, magics = R.magic_for(magics, f"lab-{k:010d}", 1)
    R.write_roster(tmp_path, {}, magics)
    lab = sorted(R.all_magics(tmp_path))
    assert len(lab) == 60
    calls = {"deals": [], "positions": []}
    deals = [{"position": 1, "magic": 990_511, "profit": -12.5},              # the Runner
             {"position": 2, "magic": lab[7], "profit": -3.25},              # a lab idea, since demoted
             {"position": 3, "magic": lab[-1], "profit": 1.5},
             {"position": 4, "magic": 990_311, "profit": -100.0},            # Trend & Breakout's own: not here
             {"position": 5, "magic": 0, "profit": -50.0}]                   # by hand: never a bot's
    held = [NS(ticket=6, magic=990_611, profit=-2.0), NS(ticket=7, magic=lab[3], profit=-0.5),
            NS(ticket=8, magic=990_311, profit=-40.0)]

    class Broker:
        def deals_since(self, since, magic=0, closing_only=True):
            calls["deals"].append(magic)
            return [r for r in deals if not magic or r["magic"] == magic]

        def positions(self, magic=None):
            calls["positions"].append(magic)
            return [p for p in held if magic is None or p.magic == magic]

    now = dt.datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    t = Trader.__new__(Trader)                     # only the methods under test; nothing is started
    t.cfg = NS(magic=990_311, runner=NS(magic=990_511), bandbreaker=NS(magic=990_611), crowd=NS(magic=990_711),
               ops=NS(data_dir=str(tmp_path)), tracking_start_utc="")
    t.labdemo = NS(magics=lambda: ())
    t.broker = Broker()
    t.clock = lambda: now
    t._others_cache = {"at": None}
    assert len(t.stop_gated_magics()) == 63
    assert t.others_real_today() == pytest.approx(-12.5 - 3.25 + 1.5 - 2.0 - 0.5)
    assert len(calls["deals"]) == 1 and len(calls["positions"]) == 1


# ================================================= review rv14 round 2 fixes --
def _down_for(w: NS, minutes: int) -> None:
    """The trader is down: the market keeps moving and no engine steps."""
    for _ in range(minutes):
        last = w.broker._bars["EURUSD"][-1].close
        w.broker.push_bar("EURUSD", last, high=last + 0.00002, low=last - 0.00002)


class TestAClearedMarkIsNoFeedGap:
    """rv14 R1: the bars a position was not walked through while marked closing (the broker's list read empty)
    are walked when the mark is cleared, so the first bar after it is never a FEED_GAP the feed never had."""

    def test_ten_empty_reads_then_the_time_exit_as_paper(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        flat_bars(w, 5)
        _an_empty_read(w, minutes=10)                                             # longer than the 5-min gap
        flat_bars(w, 70)
        assert w.broker.positions(magic) == []
        assert [(r["ticket"], r["exit_reason"], r["closed_utc"][:16]) for r in demo_trades(w.data)] == \
            [(ticket, "TIME", "2026-10-13T10:01")]                                # was FEED_GAP at 09:17

    def test_a_restart_after_downtime_whose_first_read_is_empty(self, tmp_path):
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        flat_bars(w, 5)
        _down_for(w, 20)
        eng2 = LabDemo(w.data, w.broker, cfg_mode="DEMO", now_fn=lambda: w.clock["t"], log_fn=lambda m: None,
                       prime_bars=3000)
        _an_empty_read(w, eng=eng2)                                               # the terminal not yet connected
        flat_bars(w, 80, eng=eng2)
        assert w.broker.positions(magic) == []
        assert [(r["ticket"], r["exit_reason"], r["closed_utc"][:16]) for r in demo_trades(w.data)] == \
            [(ticket, "TIME", "2026-10-13T10:01")]                                # was FEED_GAP at 09:28

    def test_a_true_gap_in_the_bars_while_marked_is_a_feed_gap_as_paper(self, tmp_path):
        """The catch-up walks the bars the broker really sent: a hole in them is a FEED_GAP, as practice (it used
        to be missed: the first bar walked after the mark was the position's first, which the gap rule skips)."""
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        real_bars = w.broker.bars

        def holed(symbol, tf, count, *a, **k):                                   # 6 minutes with no bar at all
            got = list(real_bars(symbol, tf, count, *a, **k) or [])
            return [b for b in got if not (ENTRY_AT + dt.timedelta(minutes=5) <= b.time
                                           < ENTRY_AT + dt.timedelta(minutes=11))]
        w.broker.bars = holed
        _an_empty_read(w, minutes=12)
        flat_bars(w, 3)
        assert w.broker.positions(magic) == []
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["FEED_GAP"]

    def test_a_close_whose_answer_was_lost_is_booked_as_the_exit_decided(self, tmp_path):
        """The engine's TIME close filled, the list lagged (so it was sent again and refused) and the deal came
        late: booked TIME (the deal is our own close), never BROKER_CLOSE."""
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ghost = list(w.broker.positions(magic))
        real_pos, real_close, real_deal = w.broker.positions, w.broker.close, w.broker.closed_deal
        st = {"lag": 0, "nodeal": 0}

        def lag_close(ticket, volume=0.0, comment=""):
            r = real_close(ticket, volume, comment)
            if r.ok:
                st["lag"], st["nodeal"] = 2, 3
            return r

        def lag_positions(magic=None):
            out = real_pos(magic)
            return out + [g for g in ghost if g.ticket not in {q.ticket for q in out}] if st["lag"] else out
        w.broker.close, w.broker.positions = lag_close, lag_positions
        w.broker.closed_deal = lambda ticket: None if st["nodeal"] else real_deal(ticket)
        for _ in range(75):
            flat_bars(w, 1)
            st["lag"], st["nodeal"] = max(0, st["lag"] - 1), max(0, st["nodeal"] - 1)
        assert real_pos(magic) == [] and len(w.broker.order_log) == 1
        assert [r["exit_reason"] for r in demo_trades(w.data)] == ["TIME"]

    def test_an_exit_the_catch_up_decided_then_refused_once_is_no_feed_gap(self, tmp_path):
        """rv14 S1: the empty reads cover the TIME bar, so the catch-up decides TIME on a bar inside the spell; its
        close is refused once (a transient 10031 as the bridge comes back). The bars after the exit bar decide
        nothing (the exit is decided), so the next bar is never a FEED_GAP: booked TIME, as paper, its pause
        from the TIME bar."""
        from mintel.broker.base import OrderResult
        from mintel.lab import COOLDOWN_BARS, epoch_min
        w = world(tmp_path)
        tick_to(w, ENTRY_AT)
        magic = R.read_roster(w.data)[w.cand.id]["magic"]
        ticket = w.broker.positions(magic)[0].ticket
        flat_bars(w, 10)
        _an_empty_read(w, minutes=60)                                             # over the 10:00 TIME bar
        real_close = w.broker.close
        st = {"refuse": 1}

        def refuse_once(ticket, volume=0.0, comment=""):
            if st["refuse"]:
                st["refuse"] -= 1
                return OrderResult(False, 10031, comment="no connection")
            return real_close(ticket, volume, comment)
        w.broker.close = refuse_once
        flat_bars(w, 3)
        assert st["refuse"] == 0 and any("the TIME close was refused" in m for m in w.logs)
        assert w.broker.positions(magic) == []
        assert [(r["ticket"], r["exit_reason"], r["closed_utc"][:16]) for r in demo_trades(w.data)] == \
            [(ticket, "TIME", "2026-10-13T10:12")]                                # was FEED_GAP, the same minute
        time_bar = epoch_min(dt.datetime(2026, 10, 13, 10, 0, tzinfo=UTC))
        assert w.eng.slots[w.cand.id].next_ok["EURUSD"] == time_bar + COOLDOWN_BARS


class _PracticeFeed:
    """The lab's practice engine over the same simulated broker (bars as they are, ticks by market)."""

    def __init__(self, b):
        self.b = b

    def bars(self, *a, **k):
        return self.b.bars(*a, **k)

    def ticks(self, syms):
        return {s: self.b.tick(s) for s in syms}


def test_a_stop_the_broker_filled_pauses_as_long_as_practice(tmp_path):
    """rv14 R2: a stop the broker filled in a bar the engines have not walked yet: the pause runs from the bar
    of the broker's closing deal, as practice's t_exit - never one bar early (a signal paper refuses)."""
    from mintel.clock import to_utc
    from mintel.lab import COOLDOWN_BARS, epoch_min
    from mintel.lab.practice import Practice
    w = world(tmp_path)
    led = LabLedger(w.paths.sqlite)
    pr = Practice(w.paths, _PracticeFeed(w.broker), ledger=led, now_fn=lambda: w.clock["t"], log=lambda m: None,
                  prime_bars=3000)

    def both():
        w.clock["t"] = w.broker.now + dt.timedelta(seconds=3)
        w.eng.step(w.clock["t"])
        pr.minute(w.clock["t"])
    while w.broker.now < ENTRY_AT:
        w.broker.advance(1)
        both()
    assert w.eng.open and pr.open
    magic = R.read_roster(w.data)[w.cand.id]["magic"]
    sl = w.broker.positions(magic)[0].sl
    last = w.broker._bars["EURUSD"][-1].close
    w.broker.push_bar("EURUSD", last, high=last + 0.00002, low=sl - 0.0005)       # the broker fills the stop
    stop_bar = epoch_min(to_utc(w.broker._bars["EURUSD"][-1].time))
    both()
    for _ in range(3):
        last = w.broker._bars["EURUSD"][-1].close
        w.broker.push_bar("EURUSD", last, high=last + 0.00002, low=last - 0.00002)
        both()
    assert [r["exit_reason"] for r in demo_trades(w.data)] == ["STOP"]
    assert [r[0] for r in led.query("SELECT exit_reason FROM practice_trades")] == ["STOP"]
    demo, paper = w.eng.slots[w.cand.id].next_ok["EURUSD"], pr.slots[w.cand.id].next_ok["EURUSD"]
    assert demo == paper == stop_bar + COOLDOWN_BARS                              # was one bar early
    led.close()


def test_the_margin_level_keeps_real_headroom_to_the_entrys_own_stop(tmp_path):
    """rv14 R3: sized on the equity left with the new position at its own stop (the fill's spread and slip in
    it) and kept 2% above the floor: the boundary account that landed at 299.996% now clears 300% at once and
    still does with the price a point off its stop."""
    w = world(tmp_path, balance=4879.2)
    tick_to(w, ENTRY_AT)
    assert any("cut to" in m and "the margin level" in m for m in w.logs)
    acct = w.broker.account()
    assert acct.margin > 0 and acct.margin_level >= 300.0                        # was 299.996%
    magic = R.read_roster(w.data)[w.cand.id]["magic"]
    p = w.broker.positions(magic)[0]
    spec = w.broker.spec("EURUSD")
    half = spec.typical_spread_points * spec.point / 2.0
    last = w.broker._bars["EURUSD"][-1].close
    w.broker.push_bar("EURUSD", p.sl + half + spec.point, high=last, low=p.sl + spec.point)   # a point off it
    assert w.broker.positions(magic) and w.broker.account().margin_level >= 300.0
    # every balance the review swept stays at or above the floor, at once and a point off its own stop
    for k, bal in enumerate((1000.0, 1858.9, 2604.9, 3612.0, 5439.0)):
        w = world(tmp_path / f"b{k}", balance=bal)
        tick_to(w, ENTRY_AT)
        a = w.broker.account()
        q = w.broker.positions(R.read_roster(w.data)[w.cand.id]["magic"])[0]
        at_stop = (q.entry_price - q.sl) / spec.tick_size * spec.tick_value * q.volume
        assert a.margin_level >= 300.0 and (a.balance - at_stop) / a.margin * 100.0 >= 300.0, bal
