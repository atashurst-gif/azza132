"""Rapid Momentum Rider engine against a scripted fake broker.

The prices are SYNTHETIC seeded paths (mintel/rider/synth.py); the broker
is a fake that behaves like MetaTrader where it matters here: positions
carry a magic number, the stop sits at the broker and closes the position
itself, a closing deal reports profit plus commission, and the entry
commission is in the deal history. Nothing here is a performance result."""
import datetime as dt
import json
import logging
import random

import pytest

from mintel.broker.base import AccountInfo, OrderRequest, OrderResult, Position, Side, Tick
from mintel.rider import MAGIC, synth
from mintel.rider.bars import BarSeries
from mintel.rider.config import RiderConfig
from mintel.rider.engine import RiderEngine
from mintel.rider.pipvalue import lots_for_gbp_per_pip
from mintel.rider.run import main as run_main

UTC = dt.timezone.utc
COMM_SIDE = 3.5 * 0.74          # GBP per lot per side (USD 3.50 at 0.74)


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeBroker:
    def __init__(self, ticks_by_symbol, clock):
        self.ticks_by = {s: sorted(ts, key=lambda t: t.time) for s, ts in ticks_by_symbol.items()}
        self.clock = clock
        self.specs = {s: synth.make_spec(s) for s in ticks_by_symbol}
        self.positions_ = {}
        self.sent, self.modified, self.closed_calls = [], [], []
        self.deals, self.entry_comm = {}, {}
        self.trade_allowed = True
        self.connected = True
        self.fail_modify = False
        self.fail_close = False
        self.honour_stops = True        # False: the broker holds a stop but never executes it (a fault)
        self.reads = 0                  # how many times positions() was read
        self.seq = 5000
        self.events = []
        self._checked = clock.now

    # -------------------------------------------------------------- basics --
    def connect(self): return True
    def is_connected(self): return self.connected
    def shutdown(self): pass
    def account(self): return AccountInfo(1, "demo", "GBP", 1000.0, 1000.0, 0.0, 1000.0, 0.0, 30, self.trade_allowed, True)
    def symbols(self): return list(self.ticks_by) + ["XAUUSD"]
    def spec(self, s): return self.specs.get(s)
    def ensure_selected(self, s): return True
    def calendar(self, start, end, currencies=()): return list(self.events)

    def _live(self, s):
        return [t for t in self.ticks_by.get(s, []) if t.time <= self.clock.now]

    def tick(self, s):
        live = self._live(s)
        return live[-1] if live else None

    def ticks_range(self, s, start, end):
        return [t for t in self.ticks_by.get(s, []) if start <= t.time <= min(end, self.clock.now)]

    def bars(self, s, tf, count, end=None):
        bs = BarSeries(tf.seconds, keep=count + 5)
        for t in self._live(s):
            bs.add(t.time.timestamp(), (t.bid + t.ask) / 2, t.ask - t.bid)
        return bs.closed(self.clock.now.timestamp())[-count:]

    # ------------------------------------------------------------- trading --
    def positions(self, magic=None):
        self.reads += 1
        return [p for p in self.positions_.values() if magic is None or p.magic == magic]

    def send(self, req: OrderRequest):
        self.sent.append(req)
        t = self.tick(req.symbol)
        self.seq += 1
        px = t.ask if req.side is Side.BUY else t.bid
        self.positions_[self.seq] = Position(self.seq, req.symbol, req.side, req.volume, px, req.sl, req.tp,
                                             self.clock.now, magic=req.magic, comment=req.comment)
        self.entry_comm[self.seq] = -COMM_SIDE * req.volume
        return OrderResult(True, 10009, ticket=self.seq, price=px, filled_volume=req.volume, requested_volume=req.volume)

    def modify_stops(self, ticket, sl, tp):
        p = self.positions_.get(ticket)
        t = self.tick(p.symbol) if p else None
        if self.fail_modify or p is None:
            return OrderResult(False, 10016, ticket=ticket)
        self.modified.append((ticket, sl, t.bid, t.ask, self.clock.now))
        p.sl = sl
        return OrderResult(True, 10009, ticket=ticket)

    def _settle(self, ticket, price, reason):
        p = self.positions_.pop(ticket)
        spec = self.specs[p.symbol]
        pnl = spec.money((price - p.entry_price) * p.side.sign, p.volume) - COMM_SIDE * p.volume
        self.deals[ticket] = {"exit_price": price, "pnl": pnl, "volume": p.volume, "time": self.clock.now,
                              "reason": reason}

    def close(self, ticket, volume=0.0, comment=""):
        self.closed_calls.append((ticket, comment))
        if self.fail_close or ticket not in self.positions_:
            return OrderResult(False, 10006, ticket=ticket)
        p = self.positions_[ticket]
        t = self.tick(p.symbol)
        px = t.bid if p.side is Side.BUY else t.ask
        self._settle(ticket, px, comment)
        return OrderResult(True, 10009, ticket=ticket, price=px, filled_volume=p.volume)

    def advance(self):
        """The broker's own stops, on every tick since the last check."""
        for ticket, p in list(self.positions_.items()):
            if not p.sl or not self.honour_stops:
                continue
            for t in self.ticks_by.get(p.symbol, []):
                if t.time <= self._checked or t.time > self.clock.now:
                    continue
                if (p.side is Side.BUY and t.bid <= p.sl) or (p.side is Side.SELL and t.ask >= p.sl):
                    fill = min(p.sl, t.bid) if p.side is Side.BUY else max(p.sl, t.ask)
                    self._settle(ticket, fill, "[sl]")
                    break
        self._checked = self.clock.now

    def closed_deal(self, ticket):
        return self.deals.get(ticket)

    def deals_since(self, since, magic=0, closing_only=True):
        return [{"position": k, "commission": v, "is_entry": True} for k, v in self.entry_comm.items()]


# ------------------------------------------------------------------ paths --

def wave_path(seed=3, waves=((1, 8.0, 180),), drop=(-30.0, 30), gap_quiet=90.0):
    """Warm quiet hour, then each wave: a trend, a sharp drop back, a quiet
    pause. Returns (ticks, pivot)."""
    rng = random.Random(seed)
    segs = [synth.quiet(rng, 3600.0, 0.0)]
    x = segs[-1][-1][1]
    for d, ppm, secs in waves:
        segs.append(synth.trend(rng, secs, x, d * ppm))
        x = segs[-1][-1][1]
        segs.append(synth.trend(rng, drop[1], x, drop[0] * d))
        x = segs[-1][-1][1]
        segs.append(synth.quiet(rng, gap_quiet, x, anchor=x))
        x = segs[-1][-1][1]
    path = synth.join(*segs)
    start = dt.datetime(2026, 10, 6, 6, 30, tzinfo=UTC)
    return synth.ticks_from_path("EURUSD", start, path, 1.0850), start + dt.timedelta(seconds=segs[0][-1][0])


def make(tmp_path, ticks_by, start, mode="LIVE", broker_cls=None, **kw):
    clock = Clock(start)
    broker = (broker_cls or FakeBroker)(ticks_by, clock)
    cfg = RiderConfig(mode=mode, **kw)
    eng = RiderEngine(cfg, broker, tmp_path, clock=clock, heartbeat=False)
    return eng, broker, clock


def run(eng, broker, clock, until, hook=None, every=1.0):
    while clock.now <= until:
        broker.advance()
        notes = eng.step()
        if hook is not None and hook(clock.now, notes) is False:
            return
        clock.now += dt.timedelta(seconds=every)


def last_time(ticks_by):
    return max(ts[-1].time for ts in ticks_by.values())


# ------------------------------------------------------------------ tests --

class TestLiveEntryAndManagement:
    @pytest.fixture
    def done(self, tmp_path):
        ticks, pivot = wave_path()
        tb = {"EURUSD": ticks}
        eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
        run(eng, broker, clock, last_time(tb))
        return eng, broker, tmp_path

    def test_entry_sends_magic_with_the_stop_and_gbp_per_pip_volume(self, done):
        eng, broker, _ = done
        assert broker.sent, "the clean wave must produce an order"
        req = broker.sent[0]
        want, _, _ = lots_for_gbp_per_pip(synth.make_spec("EURUSD"), 1.0)
        assert req.magic == MAGIC == 990811 and req.comment.startswith("RIDER")
        assert req.volume == pytest.approx(want) == pytest.approx(0.14)
        assert req.sl > 0 and req.tp == 0.0
        row = eng.journal.trade(broker.seq if len(broker.sent) == 1 else 5001)
        assert row["mode"] == "LIVE" and row["initial_stop"] == pytest.approx(req.sl)
        assert req.sl < row["entry"] if req.side is Side.BUY else req.sl > row["entry"]
        snap = json.loads(row["state_json"])
        assert snap["categories"] and snap["families"]["velocity"] and snap["order_flow_available"] is False
        for k in ("latency_signal_send_ms", "latency_send_ack_ms", "latency_signal_fill_ms", "signal_utc", "send_utc",
                  "ack_utc"):
            assert row[k] is not None

    def test_flowlock_moves_the_broker_stop_one_way_only(self, done):
        eng, broker, _ = done
        first = broker.sent[0]
        t1 = 5001
        moves = [m for m in broker.modified if m[0] == t1]
        assert moves, "FlowLock X should have moved the stop on a clean run"
        sls = [first.sl] + [m[1] for m in moves]
        if first.side is Side.BUY:
            assert all(b > a for a, b in zip(sls, sls[1:]))
            assert all(sl < bid for _, sl, bid, ask, _ in moves)
        else:
            assert all(b < a for a, b in zip(sls, sls[1:]))
            assert all(sl > ask for _, sl, bid, ask, _ in moves)
        changes = eng.journal.stop_changes(t1)
        assert [c["new_stop"] for c in changes] == [m[1] for m in moves]

    def test_the_exit_is_booked_from_the_brokers_deal(self, done):
        eng, broker, _ = done
        closed = [r for r in eng.journal.closed_since(dt.datetime(2026, 1, 1, tzinfo=UTC))]
        assert closed
        r = closed[0]
        deal = broker.deals[r["ticket"]]
        assert r["money_source"] == "broker"
        assert r["net_money"] == pytest.approx(deal["pnl"] + broker.entry_comm[r["ticket"]], abs=0.01)
        assert r["exit_price"] == pytest.approx(deal["exit_price"])
        assert r["mfe_pips"] > 0 and r["mfe_capture"] is not None and r["explanation"]
        assert "pips" in r["explanation"] and "£" in r["explanation"]
        assert "INITIAL" in r["flow_states"]
        assert not eng.positions

    def test_status_file(self, done):
        eng, broker, tmp = done
        st = json.loads((tmp / "rider-status.json").read_text())
        assert st["mode"] == "LIVE" and st["magic"] == 990811 and st["user_pip_value_gbp"] == 1.0
        names = {c["name"] for c in st["health"]["checks"]}
        assert names == {"mt5_alive", "broker_connected", "algo_trading", "fresh_ticks", "symbols", "spread_valid",
                         "execution", "positions_reconciled", "flowlock_alive", "database_writable", "disk_memory"}
        row = st["scanner"][0]
        assert set(row) >= {"market", "direction", "score", "state", "reason", "cost_ratio", "headroom_atr"}
        assert set(st["today"]) >= {"trades", "won", "lost", "win_rate", "net_pips", "net_money", "best_trade"}
        assert st["today"]["trades"] >= 1 and st["stories"]
        assert "never faked" in st["order_flow"]
        # why no trade (tests/test_live_diagnostics.py): the last hour of scans, the last trade and trigger
        why = st["why_no_trade"]
        assert why["scans"] > 0 and why["entries"] >= 1 and why["summary"]
        assert why["last_trade"].startswith("last trade: EURUSD") and why["last_trigger"].startswith("last trigger: ")
        assert why["markets"]["EURUSD"]["tick_age_seconds"] is not None


def test_today_is_the_uk_day_not_the_brokers(tmp_path):
    """Aaron, 9 Oct: "I don't want last night 11pm trade showing, just
    today's". The broker's day starts at 22:00 UK in summer; TODAY on the
    Rider's status starts at 00:00 UK (23:00 UTC in BST, 00:00 UTC in GMT)."""
    ticks, _pivot = wave_path()
    now = dt.datetime(2026, 10, 9, 9, 30, tzinfo=UTC)
    eng, broker, clock = make(tmp_path, {"EURUSD": ticks}, now)
    for ticket, closed, money in ((1, "2026-10-08T22:00:00+00:00", -5.0),      # 23:00 UK last night
                                  (2, "2026-10-08T23:30:00+00:00", 2.5),       # 00:30 UK today
                                  (3, "2026-10-09T08:50:00+00:00", 1.25)):
        eng.journal.insert_trade({"ticket": ticket, "mode": "LIVE", "symbol": "EURUSD", "side": "BUY",
                                  "opened_utc": closed, "closed_utc": closed, "net_money": money,
                                  "realised_pips": money, "money_source": "broker", "exit_reason": "STOP"})
    t = eng.today(now)
    assert t["from_utc"] == "2026-10-08T23:00:00+00:00"
    assert t["trades"] == 2 and t["net_money"] == pytest.approx(3.75)
    assert eng.status(now)["today"]["trades"] == 2
    assert eng.day_start(dt.datetime(2026, 12, 9, 9, 30, tzinfo=UTC)).isoformat() == "2026-12-09T00:00:00+00:00"
    assert eng.day_start(dt.datetime(2026, 10, 8, 23, 10, tzinfo=UTC)).isoformat() == "2026-10-08T23:00:00+00:00"


def test_restart_restores_the_position_and_manages_it(tmp_path):
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    ticket = next(iter(eng.positions))
    eng2 = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    assert ticket in eng2.positions and eng2.positions[ticket].flow.stop == pytest.approx(broker.positions_[ticket].sl)
    sent_before = len(broker.sent)
    before = len([m for m in broker.modified if m[0] == ticket])
    run(eng2, broker, clock, last_time(tb))
    assert len([m for m in broker.modified if m[0] == ticket]) > before      # managed on after the restart
    assert eng2.journal.trade(ticket)["closed_utc"] is not None
    assert not any(r.symbol == "EURUSD" for r in broker.sent[sent_before:sent_before + 1]) or \
        eng2.journal.trade(ticket)["closed_utc"] is not None    # no second order while it was open
    assert sum(1 for p in broker.positions_.values() if p.magic == MAGIC) <= 1


def test_an_orphan_with_our_magic_is_closed_and_other_bots_are_untouched(tmp_path):
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    start = pivot - dt.timedelta(seconds=300)
    clock = Clock(start)
    broker = FakeBroker(tb, clock)
    t0 = broker.tick("EURUSD")
    broker.positions_[777] = Position(777, "EURUSD", Side.BUY, 0.14, t0.ask, t0.ask - 0.0010, 0.0, start, magic=MAGIC)
    broker.positions_[778] = Position(778, "EURUSD", Side.BUY, 0.50, t0.ask, t0.ask - 0.0010, 0.0, start, magic=990711)
    eng = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    assert 777 not in eng.positions                          # never adopted
    eng.step()
    assert 777 not in broker.positions_ and 777 in broker.deals
    assert 778 in broker.positions_                          # another bot's position: never touched
    row = eng.journal.trade(777)
    assert row["exit_reason"] == "ORPHAN_CLOSED" and row["money_source"] == "broker"


def test_a_failed_health_check_stops_new_entries_but_not_management(tmp_path):
    e_ticks, pivot = wave_path(seed=3)
    g_ticks, _ = synth.scenario("trend", "GBPUSD", seed=4, direction=1, warm_minutes=63.0, pips_per_minute=8.0,
                                start=e_ticks[0].time)
    tb = {"EURUSD": e_ticks, "GBPUSD": g_ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    state = {"flip": None}

    def hook(now, notes):
        if state["flip"] is None and eng.positions:
            broker.trade_allowed = False                     # algo trading switched off in MetaTrader
            state["flip"] = now
            state["sent"] = len(broker.sent)
            state["mods"] = len(broker.modified)
    run(eng, broker, clock, pivot + dt.timedelta(seconds=420), hook=hook)
    assert state["flip"] is not None
    assert len(broker.sent) == state["sent"]                 # no new entries ...
    assert len(broker.modified) > state["mods"] or not eng.positions   # ... the open one is still managed
    assert eng.entries_allowed is False
    st = json.loads((tmp_path / "rider-status.json").read_text())
    assert "new entries paused" in st["health"]["summary"] and "algo trading" in st["health"]["summary"]
    gbp_rows = [r for r in eng.last_rows if r.symbol == "GBPUSD"]
    assert gbp_rows


def test_reentry_only_on_a_fresh_trigger_after_the_cooldown(tmp_path):
    ticks, pivot = wave_path(seed=6, waves=((1, 10.0, 150), (1, 10.0, 150), (1, 10.0, 150)))
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), cooldown_seconds=20.0)
    open_counts = []

    def hook(now, notes):
        open_counts.append(sum(1 for p in broker.positions_.values() if p.magic == MAGIC and p.symbol == "EURUSD"))
    run(eng, broker, clock, last_time(tb), hook=hook)
    assert max(open_counts) == 1                              # never two positions on one market
    rows = sorted(eng.journal.closed_since(dt.datetime(2026, 1, 1, tzinfo=UTC)), key=lambda r: r["opened_utc"])
    assert len(rows) >= 2, "momentum comes in waves: a fresh wave may be ridden again"
    ids = [r["trigger_id"] for r in rows]
    assert ids == sorted(set(ids))                            # each entry its own fresh trigger
    for a, b in zip(rows, rows[1:]):
        gap = (dt.datetime.fromisoformat(b["signal_utc"]) - dt.datetime.fromisoformat(a["closed_utc"])).total_seconds()
        assert gap >= 20.0 - 1e-6                             # never inside the cooldown
    vols = {r["volume"] for r in rows}
    assert vols == {0.14}                                     # same size after a loss or a win: never martingale


def test_paper_mode_never_sends_and_a_stop_fills_at_the_stop_or_worse(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER. Once the position is open the
    price is driven through its stop (the wave alone ends on PROTECT_LEVEL,
    which never reached the stop check)."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER")
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    pos = next(iter(eng.positions.values()))
    drive_through(broker, clock, pos.side, pos.flow.stop, pips=5)
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=10))
    assert broker.sent == [] and broker.modified == [] and not eng.positions
    rows = eng.journal.closed_since(dt.datetime(2026, 1, 1, tzinfo=UTC))
    assert rows and all(r["mode"] == "PAPER" and r["money_source"] == "paper" for r in rows)
    assert [r["exit_reason"] for r in rows] == ["STOP"]
    spec = synth.make_spec("EURUSD")
    slip = eng.cfg.slippage_pips * 0.0001
    for r in rows:
        d = 1 if r["side"] == "BUY" else -1
        gross = spec.money((r["exit_price"] - r["entry"]) * d, r["volume"])
        assert r["net_money"] == pytest.approx(gross - r["commission"], abs=0.01)      # commission is counted
        assert (r["exit_price"] - r["stop"]) * d <= -slip + 1e-9                      # the stop plus slippage, never better
    assert all(t >= 810_000_001 for t in (r["ticket"] for r in rows))


def test_a_paper_stop_hit_on_a_gap_fills_at_the_gapped_price_as_the_replay_does(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER. One tick jumps 10 pips through the
    stop. Before the fix the paper book closed AT the stop; now it closes at
    the price of the tick that hit it, less the slippage - the replay's
    CostModel.stop_fill."""
    from mintel.rider.research.costs import CostModel
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER")
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    ticket, pos = next(iter(eng.positions.items()))
    stop, side, d = pos.flow.stop, pos.side, pos.side.sign
    mark = round(stop - d * 10 * 0.0001, 5)                  # the closing side: the bid for a long, the ask for a short
    bid, ask = (mark, round(mark + 0.00002, 5)) if d > 0 else (round(mark - 0.00002, 5), mark)
    gap = Tick("EURUSD", clock.now + dt.timedelta(seconds=1), bid, ask)
    broker.ticks_by["EURUSD"] = [t for t in broker.ticks_by["EURUSD"] if t.time <= clock.now] + [gap]
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    assert not eng.positions
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP" and row["money_source"] == "paper"
    want = CostModel(slippage_pips=eng.cfg.slippage_pips).stop_fill(side, stop, gap, 0.0001)
    assert row["exit_price"] == pytest.approx(want, abs=1e-9)
    assert (row["exit_price"] - stop) * d == pytest.approx(-(10 + eng.cfg.slippage_pips) * 0.0001, abs=1e-9)


def test_ticks_sharing_a_millisecond_reach_the_core_once_as_in_the_replay(tmp_path):
    """Two ticks in the same millisecond with different prices, then many
    passes with no newer tick: each pass reads the history again from that
    millisecond, and before the fix both ticks were fed again every pass.
    Now the core sees each tick once - the same count as feeding the stored
    ticks once, which is what the replay does. A third tick in the same
    millisecond that arrives late is still fed, once."""
    from mintel.rider.core import RiderCore
    t0 = dt.datetime(2026, 10, 6, 8, 0, tzinfo=UTC)
    t1 = t0 + dt.timedelta(seconds=1)
    ticks = [Tick("EURUSD", t0, 1.10000, 1.10002), Tick("EURUSD", t1, 1.10003, 1.10005),
             Tick("EURUSD", t1, 1.10004, 1.10006)]
    eng, broker, clock = make(tmp_path, {"EURUSD": list(ticks)}, t0)

    def passes(n):
        for _ in range(n):
            eng.step()
            clock.now += dt.timedelta(seconds=1)

    def replayed(ts):
        ref = RiderCore(RiderConfig())
        ref.set_spec("EURUSD", synth.make_spec("EURUSD"))
        return sum(ref.on_tick("EURUSD", x) for x in ts)
    passes(6)
    sym = eng.core.syms["EURUSD"]
    assert sym.ticks_seen == replayed(ticks) == 3
    assert len(sym.buffer.ticks) == 3
    late = Tick("EURUSD", t1, 1.10003, 1.10005)              # same millisecond, the first one's prices again
    broker.ticks_by["EURUSD"].append(late)
    ticks.append(late)
    passes(4)
    assert sym.ticks_seen == replayed(ticks) == 4
    newer = Tick("EURUSD", clock.now, 1.10006, 1.10008)
    broker.ticks_by["EURUSD"].append(newer)
    ticks.append(newer)
    passes(4)
    assert sym.ticks_seen == replayed(ticks) == 5 and len(sym.buffer.ticks) == 5


class LaggingHistory(FakeBroker):
    """The tick history stops ``lag`` before the clock; the latest price does not."""
    lag = dt.timedelta(milliseconds=300)

    def ticks_range(self, s, start, end):
        return [t for t in self.ticks_by.get(s, []) if start <= t.time <= min(end, self.clock.now - self.lag)]


def test_a_tick_history_behind_the_latest_price_still_feeds_every_tick_once_in_order(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER, 200 one-second passes, a history
    300 ms behind the latest price. Before the fix the latest price was fed
    ahead of the history and the ticks in between were skipped on the next
    pass (the core got 360 of the 395 stored ticks). Now it gets every one once,
    in order - what the replay feeds - and the latest price is still the one
    used for prices."""
    ticks, pivot = wave_path()
    clock = Clock(pivot - dt.timedelta(seconds=300))
    broker = LaggingHistory({"EURUSD": ticks}, clock)
    eng = RiderEngine(RiderConfig(mode="PAPER"), broker, tmp_path, clock=clock, heartbeat=False)
    fed = []
    orig = eng.core.on_tick

    def spy(s, x):
        ok = orig(s, x)
        if ok:
            fed.append(x)
        return ok
    eng.core.on_tick = spy
    for _ in range(200):
        eng.step()
        assert eng.latest["EURUSD"] == broker.tick("EURUSD")
        clock.now += dt.timedelta(seconds=1)
    held_to = clock.now - dt.timedelta(seconds=1) - broker.lag          # the newest tick the last pass could read
    want = [x for x in ticks if fed[0].time <= x.time <= held_to]
    assert len(want) > 350
    assert fed == want and eng.core.syms["EURUSD"].ticks_seen == len(want)


def test_a_tick_history_that_stays_empty_falls_back_to_the_latest_price(tmp_path):
    """A history that never returns anything must not starve the core: after
    TICK_HISTORY_WAIT_SECONDS the latest price is fed, as with no history."""
    from mintel.rider.engine import TICK_HISTORY_WAIT_SECONDS
    ticks, pivot = wave_path()
    eng, broker, clock = make(tmp_path, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=300), mode="PAPER")
    broker.ticks_range = lambda s, start, end: []
    seen = []
    for _ in range(30):
        eng.step()
        seen.append(eng.core.syms["EURUSD"].ticks_seen)
        clock.now += dt.timedelta(seconds=1)
    wait = int(TICK_HISTORY_WAIT_SECONDS)
    assert seen[wait] == 1                                    # held back while the history may still catch up
    assert seen[-1] >= 30 - wait - 3                          # then the latest price, every pass


def test_a_paper_stop_reached_by_the_latest_price_before_the_history_has_it(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER, a history 300 ms behind. A tick 2
    pips short of the stop, then 0.4 s later one 10 pips through it. The next
    pass's history holds only the first, so the paper book's own tick check
    does not see the gap; FlowLock X does, on the latest price, and the trade
    closes there at the stop or worse less the slippage (the second paper
    stop close in RiderEngine._manage_one), as the replay's CostModel.stop_fill."""
    from mintel.rider.research.costs import CostModel
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER", broker_cls=LaggingHistory)
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    ticket, pos = next(iter(eng.positions.items()))
    stop, side, d = pos.flow.stop, pos.side, pos.side.sign

    def at(ms, pips_through):
        mark = round(stop - d * pips_through * 0.0001, 5)    # the closing side: the bid for a long, the ask for a short
        bid, ask = (mark, round(mark + 0.00002, 5)) if d > 0 else (round(mark - 0.00002, 5), mark)
        return Tick("EURUSD", clock.now + dt.timedelta(milliseconds=ms), bid, ask)
    calm, gap = at(500, -2), at(900, 10)
    broker.ticks_by["EURUSD"] = [t for t in broker.ticks_by["EURUSD"] if t.time <= clock.now] + [calm, gap]
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    assert calm in eng.new_ticks["EURUSD"] and gap not in eng.new_ticks["EURUSD"]
    assert not eng.positions
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP" and row["money_source"] == "paper"
    want = CostModel(slippage_pips=eng.cfg.slippage_pips).stop_fill(side, stop, gap, 0.0001)
    assert row["exit_price"] == pytest.approx(want, abs=1e-9)


def tick_at(side, stop, pips_through, when, sym="EURUSD"):
    """A tick at ``when`` whose closing side (the bid for a long, the ask for
    a short) is ``pips_through`` pips through ``stop``."""
    d = 1 if side is Side.BUY else -1
    mark = round(stop - d * pips_through * 0.0001, 5)
    bid, ask = (mark, round(mark + 0.00002, 5)) if d > 0 else (round(mark - 0.00002, 5), mark)
    return Tick(sym, when, bid, ask)


def slip_in(broker, x, sym="EURUSD"):
    """``x`` added to the broker's tick history, in time order."""
    broker.ticks_by[sym] = sorted(broker.ticks_by[sym] + [x], key=lambda t: t.time)


def test_a_paper_trade_is_not_stopped_by_a_tick_from_before_the_price_it_was_opened_on(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER, a history 300 ms behind. The entry
    acts on the latest price; a tick X from before that price (348 ms before
    it, 621 ms before the order), 1 pip through the planned stop, is not in
    the history yet. Before the fix the next pass checked X against the stop
    and closed the trade (STOP, -5.8 pips). The replay checks a trade only
    from its fill on, so X must not stop it."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER", broker_cls=LaggingHistory)
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    ticket, pos = next(iter(eng.positions.items()))
    opened_on, fed_to = eng.latest["EURUSD"].time, eng._last_tick_time["EURUSD"]
    assert fed_to < opened_on                                  # the history is behind the price the entry used
    x = tick_at(pos.side, pos.flow.stop, 1, fed_to + (opened_on - fed_to) / 2)
    slip_in(broker, x)
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    assert x in eng.new_ticks["EURUSD"]                       # the history brought X on this pass
    assert ticket in eng.positions and eng.journal.trade(ticket)["closed_utc"] is None


@pytest.mark.parametrize("through_old_stop", [False, True])
def test_a_paper_tick_from_before_a_stop_move_meets_the_stop_in_force_at_its_time(tmp_path, through_old_stop):
    """SYNTHETIC wave_path(seed=3), PAPER, a history 300 ms behind. A stop
    move is made on the latest price; a tick X from before that price is not
    in the history yet. X at the NEW stop (not through the old one): before
    the fix the next pass closed the trade on it (STOP, +0.7 pips); now it
    stays open, as in the replay, where a move counts only from the tick it
    is made on. X 1 pip through the OLD stop still closes it, filled against
    the old stop."""
    from mintel.rider.research.costs import CostModel
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER", broker_cls=LaggingHistory)

    def moved_ahead_of_the_history(now, notes):
        moved = any(": stop " in n and " -> " in n for n in notes)
        return False if moved and eng._last_tick_time["EURUSD"] < eng.latest["EURUSD"].time else None
    run(eng, broker, clock, last_time(tb), hook=moved_ahead_of_the_history)
    assert eng.positions
    ticket, pos = next(iter(eng.positions.items()))
    move = eng.journal.stop_changes(ticket)[-1]
    old, new = move["old_stop"], move["new_stop"]
    assert pos.flow.stop == pytest.approx(new)
    moved_on, fed_to = eng.latest["EURUSD"].time, eng._last_tick_time["EURUSD"]
    assert fed_to < moved_on
    when = fed_to + (moved_on - fed_to) / 2
    x = tick_at(pos.side, old, 1, when) if through_old_stop else tick_at(pos.side, new, 0, when)
    slip_in(broker, x)
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    assert x in eng.new_ticks["EURUSD"]
    if not through_old_stop:
        assert ticket in eng.positions and eng.journal.trade(ticket)["closed_utc"] is None
        return
    assert not eng.positions
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP" and row["money_source"] == "paper"
    want = CostModel(slippage_pips=eng.cfg.slippage_pips).stop_fill(pos.side, old, x, 0.0001)
    assert row["exit_price"] == pytest.approx(want, abs=1e-9)


def test_a_refused_stop_move_is_rolled_back_and_retried(tmp_path):
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    seen = {"refused": 0, "checked": 0}

    def hook(now, notes):
        if eng.positions and seen["checked"] == 0:
            broker.fail_modify = True
            seen["checked"] = 1
            seen["until"] = now + dt.timedelta(seconds=40)
        if seen.get("until") and now <= seen["until"]:
            for p in eng.positions.values():
                assert p.flow.stop == pytest.approx(broker.positions_[p.ticket].sl)   # never ahead of the broker
            seen["refused"] += sum(1 for n in notes if "refused stop" in n)
        elif seen.get("until"):
            broker.fail_modify = False
    run(eng, broker, clock, last_time(tb), hook=hook)
    assert seen["refused"] >= 1 and broker.modified                 # refused, kept, then retried and taken


def test_a_refused_close_keeps_the_position_and_the_broker_stop_still_protects(tmp_path):
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    broker.fail_close = True
    run(eng, broker, clock, last_time(tb))
    rows = eng.journal.closed_since(dt.datetime(2026, 1, 1, tzinfo=UTC))
    assert rows and rows[0]["money_source"] == "broker"
    assert rows[0]["exit_reason"] in ("STOP", "BROKER_CLOSED")
    assert not eng.positions


def test_the_pip_value_setting_is_never_changed(tmp_path):
    p = RiderConfig.default_path(tmp_path)
    p.write_text(json.dumps({"mode": "LIVE", "user_pip_value_gbp": 1.0}))
    before = p.read_bytes()
    cfg = RiderConfig.load(p)
    ticks, pivot = wave_path(seed=6, waves=((1, 10.0, 150), (-1, 10.0, 150)))
    tb = {"EURUSD": ticks}
    clock = Clock(pivot - dt.timedelta(seconds=300))
    broker = FakeBroker(tb, clock)
    eng = RiderEngine(cfg, broker, tmp_path, clock=clock, heartbeat=False)
    with pytest.raises(AttributeError):
        eng.pip_value_gbp = 5.0
    run(eng, broker, clock, last_time(tb))
    assert cfg.user_pip_value_gbp == 1.0 and eng.pip_value_gbp == 1.0 and p.read_bytes() == before
    assert broker.sent and {r.volume for r in broker.sent} == {0.14}


def test_off_mode_exits_at_once(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    cfgp = data / "config.json"
    cfgp.write_text(json.dumps({"ops": {"data_dir": str(data), "log_dir": str(tmp_path / "logs")}}))
    RiderConfig.save_minimal(data / "rider.json", "OFF")
    before = list(logging.root.handlers)
    try:
        assert run_main(["--config", str(cfgp)]) == 0
    finally:
        for h in list(logging.root.handlers):
            if h not in before:                               # run.main's log file handler: not left behind
                logging.root.removeHandler(h)
                h.close()
    assert not (data / "rider.pid").exists()


# ------------------------------------------------- stop moves: the gate --

def test_the_stop_move_gate_needs_a_real_step_and_a_pause_and_a_refusal_waits_too():
    """RiderCore.stop_move_due: at least 0.3 pip AND one spread, at least 5 s
    after the last move sent (accepted or refused); only the first move to
    net breakeven skips the pause. The pacing survives a restart."""
    from mintel.rider.core import RiderCore, RiderPosition
    from mintel.rider.flowlock_x import FlowState
    cfg = RiderConfig()
    assert (cfg.stop_min_step_pips, cfg.stop_min_step_spreads, cfg.stop_min_modify_seconds) == (0.3, 1.0, 5.0)
    core = RiderCore(cfg)
    core.set_spec("EURUSD", synth.make_spec("EURUSD"))
    t0 = dt.datetime(2026, 10, 6, 8, 0, tzinfo=UTC)
    pos = RiderPosition(1, "EURUSD", Side.BUY, 0.14, 1.10000, t0,
                        core.flowlock.start(1, 1.10000, 1.09900, 0.00008, t0.timestamp()), pip=0.0001)

    def tk(spread_pips):
        return Tick("EURUSD", t0, 1.10050, round(1.10050 + spread_pips * 0.0001, 6))

    def at(s):
        return t0 + dt.timedelta(seconds=s)
    assert not core.stop_move_due(pos, 1.09902, tk(0.1), at(1))           # 0.2 pip: too small
    assert core.stop_move_due(pos, 1.09903, tk(0.1), at(1))               # 0.3 pip
    assert not core.stop_move_due(pos, 1.09903, tk(0.5), at(1))           # under one spread
    assert not core.stop_move_due(pos, 1.09890, tk(0.1), at(1))           # wider: never
    core.stop_move_sent(pos, at(1))
    assert core.confirm_stop(pos, 1.09910, at(1))
    assert not core.stop_move_due(pos, 1.09930, tk(0.1), at(4))           # 3 s after the last move
    assert core.stop_move_due(pos, 1.09930, tk(0.1), at(6))
    core.stop_move_sent(pos, at(6))                                       # ... and the broker refused it
    assert pos.flow.stop == 1.09910
    pos.flow.costs_covered = True
    assert not core.stop_move_due(pos, 1.10008, tk(0.1), at(9))           # after a refusal even breakeven waits
    assert core.stop_move_due(pos, 1.09930, tk(0.1), at(11))
    core.stop_move_sent(pos, at(12))
    assert core.confirm_stop(pos, 1.09950, at(12))
    assert core.stop_move_due(pos, 1.10008, tk(0.1), at(13))              # the first move to breakeven: at once
    core.stop_move_sent(pos, at(13))
    assert core.confirm_stop(pos, 1.10008, at(13))
    assert not core.stop_move_due(pos, 1.10020, tk(0.1), at(14))          # past breakeven: the pause again
    back = FlowState.from_dict(json.loads(json.dumps(pos.flow.to_dict())))
    assert back.last_attempt_at == back.last_modify_at == at(13).timestamp()


def test_stop_moves_at_the_broker_are_paced_and_carry_no_take_profit(tmp_path):
    """SYNTHETIC wave_path(seed=3). Before the gate ticket 5001 got 11 moves
    in 103 s, the smallest 0.1 pip, some 1 s apart. Now every move gains at
    least 0.3 pip and one spread and comes at least 5 s after the last, and
    no positions read goes before a move (tp is 0.0: no take-profit)."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    calls = []
    orig = eng.executor.modify_stop

    def spy(ticket, stop, tp=None):
        n0 = broker.reads
        ok = orig(ticket, stop, tp)
        calls.append((tp, broker.reads - n0))
        return ok
    eng.executor.modify_stop = spy
    run(eng, broker, clock, last_time(tb))
    assert calls and all(tp == 0.0 and reads == 0 for tp, reads in calls)
    cfg, pip = eng.cfg, 0.0001
    tickets = sorted({m[0] for m in broker.modified})
    assert tickets
    for t in tickets:
        row = eng.journal.trade(t)
        d = 1 if row["side"] == "BUY" else -1
        prev_sl, prev_at = row["initial_stop"], None
        for _, sl, bid, ask, when in [m for m in broker.modified if m[0] == t]:
            assert (sl - prev_sl) * d >= max(cfg.stop_min_step_pips * pip, cfg.stop_min_step_spreads * (ask - bid)) - 1e-10
            if prev_at is not None:
                breakeven = (row["entry"] - prev_sl) * d > 0
                assert (when - prev_at).total_seconds() >= cfg.stop_min_modify_seconds or breakeven
            prev_sl, prev_at = sl, when


def scripted_day(path, symbol="EURUSD"):
    """[(seconds after 08:00, bid, ask)] on 2026-10-06 -> (DayData with no seed bars, t0 ms)."""
    from mintel.rider.research.replay import DayData
    from mintel.rider.research.tickstore import DayTicks, day_start_ms
    day = "2026-10-06"
    d0 = day_start_ms(day)
    t0 = d0 + 8 * 3_600_000
    ticks = DayTicks(symbol)
    for sec, b, a in path:
        ticks.times.append(t0 + int(round(sec * 1000)))
        ticks.bids.append(b)
        ticks.asks.append(a)
    return DayData(day, [symbol], {symbol: synth.make_spec(symbol)}, [], d0 - 600_000, d0, d0 + 86_400_000,
                   d0 + 86_400_000, {symbol: ticks}, {}), t0


def test_the_live_engine_and_the_replay_ask_the_same_core_gate(tmp_path, monkeypatch):
    from mintel.rider.core import EntryIntent, RiderCore
    from mintel.rider.research.costs import CostModel
    from mintel.rider.research.replay import DayReplay, ZRecord
    from mintel.rider.research.tickstore import ms_to_dt
    path = [(-5, 1.10000, 1.10002)] + [(0.5 * i, round(1.10000 + 0.000015 * i, 6), round(1.10002 + 0.000015 * i, 6))
                                       for i in range(0, 121)]
    data, t0 = scripted_day(path)
    it = EntryIntent("EURUSD", Side.BUY, 1.09950, "scripted", {}, "breakout", 70.0, 1, 0.0, ms_to_dt(t0), 1.10002,
                     0.0005, 0.00002, 0.00008, 0.002, 0.1, 0.6, 2.0, 5.2)

    def replay():
        return DayReplay(RiderConfig(), data, CostModel(slippage_pips=0.2, latency_ms=250), "T").run_fixed(
            [it], ZRecord(["EURUSD"]))
    assert replay().counters.get("stop_moves", 0) > 0                     # the rising path does move the stop
    asked = []

    def never(self, pos, new_stop, tick, now):
        asked.append(pos.ticket)
        return False
    monkeypatch.setattr(RiderCore, "stop_move_due", never)
    out = replay()
    assert asked and out.counters.get("stop_moves", 0) == 0               # the replay asks the core's gate ...
    n_replay = len(asked)
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    run(eng, broker, clock, last_time(tb))
    assert len(asked) > n_replay and broker.sent and broker.modified == []   # ... and so does the live engine


# --------------------------------------------- LIVE: the broker's stop --

def open_live(tmp_path):
    """A LIVE engine on the SYNTHETIC wave with one position just opened."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    return eng, broker, clock, next(iter(eng.positions))


def drive_through(broker, clock, side, stop, pips=39, sym="EURUSD"):
    """From the next second on, the price runs 1, 2, ... ``pips`` pips through ``stop``."""
    keep = [t for t in broker.ticks_by[sym] if t.time <= clock.now]
    d = 1 if side is Side.BUY else -1
    for i in range(1, pips + 1):
        mark = round(stop - d * i * 0.0001, 5)               # the closing side: the bid for a long, the ask for a short
        bid, ask = (mark, round(mark + 0.00002, 5)) if d > 0 else (round(mark - 0.00002, 5), mark)
        keep.append(Tick(sym, clock.now + dt.timedelta(seconds=i), bid, ask))
    broker.ticks_by[sym] = keep


def test_a_missing_broker_stop_is_sent_again_after_a_restart(tmp_path):
    """The review's case: broker sl 0, a restart, then the price runs 1 to 39
    pips through the recorded stop. Before the fix nothing was sent and the
    trade stayed open with no stop at all."""
    eng, broker, clock, ticket = open_live(tmp_path)
    recorded = eng.positions[ticket].flow.stop
    side = eng.positions[ticket].side
    broker.positions_[ticket].sl = 0.0                       # removed by hand, not attached, or reported as 0
    eng2 = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    assert eng2.positions[ticket].flow.stop == pytest.approx(recorded)     # kept: never "adopted" as 0
    drive_through(broker, clock, side, recorded)
    n_mod = len(broker.modified)
    run(eng2, broker, clock, clock.now + dt.timedelta(seconds=40))
    assert [(m[0], m[1]) for m in broker.modified[n_mod:]][:1] == [(ticket, recorded)]   # sent again at once
    assert eng2.journal.events("STOP_RESTORED")
    assert ticket not in broker.positions_ and not eng2.positions          # the broker's stop then took it
    row = eng2.journal.trade(ticket)
    assert row["closed_utc"] is not None and row["money_source"] == "broker"
    d = 1 if side is Side.BUY else -1
    assert (row["exit_price"] - recorded) * d >= -1.5 * 0.0001             # at the stop, not 39 pips through


def test_no_broker_stop_and_the_price_already_through_it_is_closed_at_once(tmp_path):
    eng, broker, clock, ticket = open_live(tmp_path)
    recorded = eng.positions[ticket].flow.stop
    side = eng.positions[ticket].side
    broker.positions_[ticket].sl = 0.0
    drive_through(broker, clock, side, recorded)
    clock.now += dt.timedelta(seconds=5)                     # the bot was down while the price ran through
    eng2 = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    n_mod = len(broker.modified)
    broker.advance()
    eng2.step()
    assert ticket not in broker.positions_ and len(broker.modified) == n_mod   # closed, no stop sent through the price
    row = eng2.journal.trade(ticket)
    assert row["exit_reason"] == "NO_BROKER_STOP" and row["money_source"] == "broker"
    assert "safety stop" in row["explanation"]
    assert eng2.journal.events("NO_BROKER_STOP")


def test_a_broker_that_will_not_hold_the_stop_gets_the_trade_closed(tmp_path):
    eng, broker, clock, ticket = open_live(tmp_path)
    broker.positions_[ticket].sl = 0.0                       # e.g. an order that filled without its stop
    broker.fail_modify = True
    broker.advance()
    notes = eng.step()
    assert ticket not in broker.positions_ and not eng.positions
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "NO_BROKER_STOP" and row["money_source"] == "broker"
    assert any("refused to hold the stop" in n for n in notes)


@pytest.mark.parametrize("tighter", [False, True])
def test_a_restart_adopts_only_a_tighter_broker_stop(tmp_path, tighter):
    eng, broker, clock, ticket = open_live(tmp_path)
    pos = eng.positions[ticket]
    d = pos.side.sign
    recorded = pos.flow.stop
    tk = broker.tick("EURUSD")
    room = ((tk.bid if d > 0 else tk.ask) - recorded) * d
    shift = min(1.0 * 0.0001, room / 2) if tighter else -3.0 * 0.0001
    broker_sl = round(recorded + d * shift, 5)
    broker.positions_[ticket].sl = broker_sl
    eng2 = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    want = broker_sl if tighter else recorded
    assert eng2.positions[ticket].flow.stop == pytest.approx(want)         # never a looser one
    broker.advance()
    eng2.step()
    assert broker.positions_[ticket].sl == pytest.approx(want)             # the broker holds at least that
    assert bool(eng2.journal.events("STOP_RESTORED")) is (not tighter)


def test_a_stop_the_broker_does_not_execute_is_closed_at_market(tmp_path):
    eng, broker, clock, ticket = open_live(tmp_path)
    broker.honour_stops = False                              # the broker shows the stop but never acts on it
    pos = eng.positions[ticket]
    drive_through(broker, clock, pos.side, pos.flow.stop)
    passes = []

    def hook(now, notes):
        passes.append(now)
        return False if not eng.positions else None
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=40), hook=hook)
    assert ticket not in broker.positions_ and any(c[0] == ticket for c in broker.closed_calls)
    assert len(passes) <= 3                                  # a pass after FlowLock X first saw the stop reached
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP" and row["money_source"] == "broker"


@pytest.mark.parametrize("pips_through", [10, -5])
def test_a_position_gone_with_no_deal_is_estimated_at_the_worse_of_the_stop_and_the_price(tmp_path, pips_through):
    """SYNTHETIC wave_path(seed=3), LIVE. The position leaves the broker's
    list and its deal history stays empty. Before the fix the estimate was
    booked AT the stop with the price 10 pips through it (10 pips better than
    any real fill) and at the price with the price above the stop. Now it is
    the worse of the two, and stays labelled an estimate."""
    from mintel.rider.engine import MISSING_SECONDS
    eng, broker, clock, ticket = open_live(tmp_path)
    pos = eng.positions[ticket]
    stop, d = pos.flow.stop, pos.side.sign
    del broker.positions_[ticket]                              # gone, and no closing deal comes in
    mark = round(stop - d * pips_through * 0.0001, 5)        # the closing side: the bid for a long, the ask for a short
    bid, ask = (mark, round(mark + 0.00002, 5)) if d > 0 else (round(mark - 0.00002, 5), mark)
    keep = [t for t in broker.ticks_by["EURUSD"] if t.time <= clock.now]
    broker.ticks_by["EURUSD"] = keep + [Tick("EURUSD", clock.now + dt.timedelta(seconds=i), bid, ask)
                                        for i in range(1, int(MISSING_SECONDS) + 10)]
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=MISSING_SECONDS + 5),
        hook=lambda now, n: False if not eng.positions else None)
    assert not eng.positions
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "BROKER_CLOSED?" and row["money_source"] == "estimate"
    assert row["exit_price"] == pytest.approx(mark if pips_through > 0 else stop, abs=1e-9)
    assert (row["exit_price"] - stop) * d <= 1e-9                             # never better than the stop


@pytest.mark.parametrize("pips_through", [10, -5])
def test_a_live_row_gone_with_no_deal_found_in_paper_is_estimated_and_later_corrected(tmp_path, pips_through):
    """SYNTHETIC wave_path(seed=3). A LIVE position leaves the broker with no
    deal; the bot restarts in PAPER. Before the fix the row was booked AT the
    stop with the price 10 pips through it (-4.5 pips instead of -14.5) and
    never corrected, because the estimates were only checked in LIVE. Now it
    is the worse of the stop and the price, and the broker's figure replaces
    it once the deal comes in. One look is not proof it is gone: as in LIVE,
    the row is estimated only once it has been missing for MISSING_SECONDS."""
    from mintel.rider.engine import MISSING_SECONDS
    eng, broker, clock, ticket = open_live(tmp_path)
    pos = eng.positions[ticket]
    stop, side, d = pos.flow.stop, pos.side, pos.side.sign
    volume = broker.positions_[ticket].volume
    del broker.positions_[ticket]                              # gone, and no closing deal yet
    keep = [t for t in broker.ticks_by["EURUSD"] if t.time <= clock.now]
    gone = [tick_at(side, stop, pips_through, clock.now + dt.timedelta(seconds=i)) for i in range(1, 200)]
    broker.ticks_by["EURUSD"] = keep + gone
    clock.now += dt.timedelta(seconds=2)
    eng2 = RiderEngine(RiderConfig(mode="PAPER"), broker, tmp_path, clock=clock, heartbeat=False)
    first = clock.now
    eng2.step()
    while eng2.journal.trade(ticket)["closed_utc"] is None and clock.now < first + dt.timedelta(seconds=90):
        assert eng2.leftovers_outstanding()                   # kept open, and looked at again on every pass
        clock.now += dt.timedelta(seconds=1)
        eng2.step()
    assert (clock.now - first).total_seconds() >= MISSING_SECONDS
    row = eng2.journal.trade(ticket)
    mark = gone[0].bid if d > 0 else gone[0].ask
    assert row["closed_utc"] is not None
    assert row["exit_reason"] == "BROKER_CLOSED?" and row["money_source"] == "estimate"
    assert row["exit_price"] == pytest.approx(mark if pips_through > 0 else stop, abs=1e-9)
    assert (row["exit_price"] - stop) * d <= 1e-9                             # never better than the stop
    fill = round(stop - d * 0.0001, 5)                         # the broker's own figure, when it comes in
    pnl = synth.make_spec("EURUSD").money((fill - row["entry"]) * d, volume) - COMM_SIDE * volume
    broker.deals[ticket] = {"exit_price": fill, "pnl": pnl, "volume": volume, "time": clock.now, "reason": "[sl]"}
    clock.now += dt.timedelta(seconds=61)
    notes = eng2.step()
    row = eng2.journal.trade(ticket)
    assert row["money_source"] == "broker" and row["exit_reason"] == "BROKER_CLOSED"
    assert row["exit_price"] == pytest.approx(fill, abs=1e-9)
    assert row["net_money"] == pytest.approx(pnl + broker.entry_comm[ticket], abs=0.01)
    assert any("broker's figure" in n for n in notes)


# ------------------------------------------- review fixes (9 Oct, part 2) --

def test_a_paper_close_on_an_earlier_stop_is_booked_with_that_stop_and_what_was_seen_up_to_it(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER, a history 300 ms behind. A stop
    move is made on the latest price; a tick X from before that price, 1 pip
    through the OLD stop, closes the trade against the old stop. Before the
    fix the row held the later stop (so the close looked like a failed stop),
    the story was written from it, and the best price included the latest
    price the move was made on - after X. Now the row and the story hold the
    stop X met, the note says the later move does not count, the best price
    is the one seen up to X, and FlowLock X's own stop is untouched."""
    from mintel.rider.engine import explain_trade
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER", broker_cls=LaggingHistory)
    best = []

    def moved_ahead_of_the_history(now, notes):
        if eng.positions:
            best.append(next(iter(eng.positions.values())).flow.best)
        moved = any(": stop " in n and " -> " in n for n in notes)
        return False if moved and eng._last_tick_time["EURUSD"] < eng.latest["EURUSD"].time else None
    run(eng, broker, clock, last_time(tb), hook=moved_ahead_of_the_history)
    assert eng.positions and len(best) >= 2
    ticket, pos = next(iter(eng.positions.items()))
    move = eng.journal.stop_changes(ticket)[-1]
    old, new = move["old_stop"], move["new_stop"]
    d, pip = pos.side.sign, 0.0001
    assert (best[-1] - best[-2]) * d > 0                       # the move's price was a new best, after X
    moved_on, fed_to = eng.latest["EURUSD"].time, eng._last_tick_time["EURUSD"]
    x = tick_at(pos.side, old, 1, fed_to + (moved_on - fed_to) / 2)
    slip_in(broker, x)
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    assert not eng.positions and pos.flow.stop == pytest.approx(new)    # FlowLock X's stop is never touched
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP" and row["stop"] == pytest.approx(old)
    assert "does not count" in row["exit_detail"] and str(new) in row["exit_detail"]
    assert row["mfe_pips"] == pytest.approx(round(max(0.0, (best[-2] - row["entry"]) * d) / pip, 2))
    assert row["explanation"] == explain_trade("EURUSD", pos.side, pos.family, row["flow_states"].split(","), "STOP",
                                               row["realised_pips"], row["net_money"], "paper", "PAPER",
                                               row["initial_stop"], old)


def _estimate_row(eng, ticket, closed_at, **kw):
    """A LIVE trade booked as an estimate: -14.5 pips, gone with no deal (SYNTHETIC figures)."""
    from mintel.rider.engine import explain_trade
    row = {"ticket": ticket, "mode": "LIVE", "symbol": "EURUSD", "side": "BUY", "volume": 0.14, "pip": 0.0001,
           "gbp_per_pip": 1.04, "opened_utc": (closed_at - dt.timedelta(minutes=3)).isoformat(),
           "closed_utc": closed_at.isoformat(), "entry": 1.08541, "initial_stop": 1.08441, "stop": 1.08441,
           "exit_price": 1.08396, "exit_reason": "BROKER_CLOSED?", "realised_pips": -14.5, "net_money": -15.08,
           "money_source": "estimate", "entry_family": "breakout", "flow_states": "INITIAL",
           "explanation": explain_trade("EURUSD", Side.BUY, "breakout", ["INITIAL"], "BROKER_CLOSED?", -14.5, -15.08,
                                        "estimate", "LIVE", 1.08441, 1.08441)}
    row.update(kw)
    eng.journal.insert_trade(row)
    return row


def test_the_brokers_figure_rewrites_the_story_too(tmp_path):
    """SYNTHETIC. An estimate of -14.5 pips; the broker's deal says -4.7.
    Before the fix exit, pips and money took the broker's figure but the
    story still said -14.5 pips, estimated. Now the story is written again
    from the broker's figure."""
    ticks, pivot = wave_path()
    eng, broker, clock = make(tmp_path, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=300))
    _estimate_row(eng, 4242, clock.now)
    pnl = synth.make_spec("EURUSD").money(-0.00047, 0.14) - COMM_SIDE * 0.14
    broker.deals[4242] = {"exit_price": 1.08494, "pnl": pnl, "volume": 0.14, "time": clock.now, "reason": "[sl]"}
    eng._correct_estimates(clock.now)
    row = eng.journal.trade(4242)
    assert row["money_source"] == "broker" and row["realised_pips"] == pytest.approx(-4.7)
    assert "-4.7 pips" in row["explanation"] and "-14.5" not in row["explanation"]
    assert "estimated" not in row["explanation"] and f"£{abs(row['net_money']):.2f}" in row["explanation"]


def test_a_leftover_closed_before_its_deal_is_readable_is_booked_at_the_close_price(tmp_path):
    """SYNTHETIC wave_path(seed=3). A LIVE position is open when the bot is
    switched to PAPER; the close goes through but its deal is not readable
    yet. Before the fix the row was booked at the ENTRY price (0.0 pips, no
    money, no '?'). Now it is booked at the price the broker closed at,
    marked '?', with an estimated figure, and the broker's figure replaces
    it once the deal comes in."""
    eng, broker, clock, ticket = open_live(tmp_path)
    p = broker.positions_[ticket]
    clock.now += dt.timedelta(seconds=20)                     # the price has moved on since the entry
    broker.closed_deal = lambda t: None                       # the closing deal is not readable yet
    eng2 = RiderEngine(RiderConfig(mode="PAPER"), broker, tmp_path, clock=clock, heartbeat=False)
    eng2.step()
    assert ticket not in broker.positions_
    fill = broker.deals[ticket]["exit_price"]                 # the price the broker closed it at
    d = p.side.sign
    assert abs(fill - p.entry_price) > 1e-9
    row = eng2.journal.trade(ticket)
    assert row["closed_utc"] is not None and row["exit_reason"] == "SWITCHED TO PAPER?"
    assert row["money_source"] == "estimate" and row["exit_price"] == pytest.approx(fill, abs=1e-9)
    assert row["realised_pips"] == pytest.approx(round((fill - p.entry_price) * d / 0.0001, 2))
    want = synth.make_spec("EURUSD").money((fill - p.entry_price) * d, p.volume) - 2 * eng2.core.commission_account_per_lot_side() * p.volume
    assert row["net_money"] == pytest.approx(want, abs=0.01)
    assert "estimated" in row["explanation"]
    del broker.closed_deal                                    # the deal comes in
    clock.now += dt.timedelta(seconds=61)
    eng2.step()
    row = eng2.journal.trade(ticket)
    assert row["exit_reason"] == "SWITCHED TO PAPER" and row["money_source"] == "broker"
    assert row["net_money"] == pytest.approx(broker.deals[ticket]["pnl"] + broker.entry_comm[ticket], abs=0.01)
    assert "estimated" not in row["explanation"]


def _locked(*_a, **_k):
    import sqlite3
    raise sqlite3.OperationalError("database is locked")


@pytest.mark.parametrize("mode", ["LIVE", "PAPER"])
def test_a_database_that_cannot_be_written_never_stops_management(tmp_path, mode):
    """SYNTHETIC wave_path(seed=3). Every write to rider.sqlite fails
    ('database is locked') while a LIVE estimate's deal comes in (and, in
    PAPER, a real position is left from LIVE). Before the fix the estimate
    check (and the leftover close) raised, every pass stopped before the open
    position was managed, and no stop was moved. Now each pass manages the
    position, stop moves still go out, and the bookkeeping is tried again."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode=mode)
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    assert eng.positions
    _estimate_row(eng, 4242, clock.now)
    broker.deals[4242] = {"exit_price": 1.08494, "pnl": -6.0, "volume": 0.14, "time": clock.now, "reason": "[sl]"}
    if mode == "PAPER":                                       # a real position left from LIVE, with its row
        t0 = broker.tick("EURUSD")
        broker.positions_[4243] = Position(4243, "EURUSD", Side.BUY, 0.14, t0.ask, round(t0.ask - 0.0010, 5), 0.0,
                                           clock.now, magic=MAGIC)
        _estimate_row(eng, 4243, clock.now, closed_utc=None, exit_price=None, exit_reason=None, money_source=None,
                      net_money=None, realised_pips=None, entry=t0.ask, stop=round(t0.ask - 0.0010, 5))
    eng.journal._exec = _locked                               # every write fails from here on
    eng._minute = ""                                          # the minute's bookkeeping is due now
    managed, moves = [], []
    orig_manage, orig_modify = eng._manage, eng.executor.modify_stop
    eng._manage = lambda now: (managed.append(now), orig_manage(now))[1]
    eng.executor.modify_stop = lambda *a, **k: (moves.append(a), orig_modify(*a, **k))[1]
    passes, notes = [], []

    def hook(now, n):
        passes.append(now)
        notes.extend(n)
        return None if len(passes) < 120 else False
    run(eng, broker, clock, last_time(tb), hook=hook)          # before the fix: sqlite3.OperationalError
    assert len(managed) == len(passes) >= 60
    assert moves                                              # stop moves still went out
    assert eng.journal.trade(4242)["money_source"] == "estimate"            # nothing written ...
    assert any("could not record" in x for x in notes)       # ... said so, and tried again
    if mode == "PAPER":
        assert 4243 not in broker.positions_                  # the real leftover was still closed at the broker


# ------------------------------------- OFF: what LIVE left is closed first --

def _main_off(tmp_path, broker, monkeypatch, *extra):
    """run.main with data/rider.json saying OFF, in ``tmp_path`` (the engine's
    data folder), with ``broker`` as the MetaTrader connection."""
    import mintel.run
    cfgp = tmp_path / "config.json"
    cfgp.write_text(json.dumps({"ops": {"data_dir": str(tmp_path), "log_dir": str(tmp_path / "logs")}}))
    RiderConfig.save_minimal(tmp_path / "rider.json", "OFF")
    monkeypatch.setattr(mintel.run, "build_broker", lambda cfg: broker)
    before = list(logging.root.handlers)
    try:
        return run_main(["--config", str(cfgp), *extra])
    finally:
        for h in list(logging.root.handlers):
            if h not in before:
                logging.root.removeHandler(h)
                h.close()


def test_a_switch_to_off_closes_what_live_left_then_exits(tmp_path, monkeypatch):
    """SYNTHETIC wave_path(seed=3). A LIVE trade is open, plus a stray
    position under 990811 with no record, when the Rider is switched OFF.
    Before the fix the OFF start logged 'mode is OFF' and exited: both stayed
    at the broker with nothing managing them and the row never closed. Now
    both are closed at the broker and booked with the broker's figure,
    another bot's position is untouched, and the process exits."""
    eng, broker, clock, ticket = open_live(tmp_path)
    eng.journal.close()                                       # the LIVE process has been stopped
    t0 = broker.tick("EURUSD")
    broker.positions_[777] = Position(777, "EURUSD", Side.BUY, 0.14, t0.ask, round(t0.ask - 0.0010, 5), 0.0,
                                      clock.now, magic=MAGIC)
    broker.positions_[778] = Position(778, "EURUSD", Side.BUY, 0.50, t0.ask, round(t0.ask - 0.0010, 5), 0.0,
                                      clock.now, magic=990711)
    assert _main_off(tmp_path, broker, monkeypatch) == 0
    assert not any(p.magic == MAGIC for p in broker.positions_.values())
    assert 778 in broker.positions_                           # another bot's: never touched
    from mintel.rider.journal import RiderJournal
    j = RiderJournal(tmp_path / "rider.sqlite")
    try:
        for t in (ticket, 777):
            row = j.trade(t)
            assert row["closed_utc"] is not None and row["exit_reason"] == "SWITCHED OFF"
            assert row["money_source"] == "broker" and row["exit_price"] == pytest.approx(broker.deals[t]["exit_price"])
        assert not [r for r in j.open_rows() if r["mode"] == "LIVE"]
    finally:
        j.close()
    assert not (tmp_path / "rider.pid").exists()


def test_off_keeps_trying_a_refused_close_for_a_while_then_exits_leaving_the_broker_stop(tmp_path, monkeypatch):
    import mintel.rider.engine as reng
    import mintel.rider.run as rrun
    monkeypatch.setattr(rrun, "OFF_PASS_SECONDS", 0.0)
    monkeypatch.setattr(reng, "CLOSE_RETRY_SECONDS", 0.0)     # (a minute between tries in real use)
    monkeypatch.setattr(reng, "fx_market_open", lambda now: True)
    eng, broker, clock, ticket = open_live(tmp_path)
    eng.journal.close()
    sl = broker.positions_[ticket].sl
    broker.fail_close = True
    assert _main_off(tmp_path, broker, monkeypatch, "--cycles", "3") == 1
    assert ticket in broker.positions_ and broker.positions_[ticket].sl == sl     # still protected by its stop
    assert len([c for c in broker.closed_calls if c[0] == ticket]) == 3           # tried on every pass
    assert not (tmp_path / "rider.pid").exists()


class Unreachable(FakeBroker):
    def connect(self): return False


@pytest.mark.parametrize("reachable", [True, False])
def test_off_with_nothing_left_from_live_exits_at_once(tmp_path, monkeypatch, reachable):
    import mintel.rider.run as rrun
    monkeypatch.setattr(rrun, "OFF_RECHECK_SECONDS", 0.01)
    ticks, pivot = wave_path()
    clock = Clock(pivot)
    broker = (FakeBroker if reachable else Unreachable)({"EURUSD": ticks}, clock)
    assert _main_off(tmp_path, broker, monkeypatch) == 0
    assert broker.closed_calls == [] and broker.reads == (2 if reachable else 0)     # an empty list is read twice
    assert not (tmp_path / "rider.sqlite").exists() and not (tmp_path / "rider.pid").exists()


def test_off_with_a_live_trade_recorded_and_no_metatrader_says_so(tmp_path, monkeypatch):
    import mintel.rider.run as rrun
    monkeypatch.setattr(rrun, "OFF_WIND_DOWN_SECONDS", 0.05)
    monkeypatch.setattr(rrun, "OFF_CONNECT_PAUSE_SECONDS", 0.01)
    eng, broker, clock, ticket = open_live(tmp_path)
    eng.journal.close()
    off = Unreachable(broker.ticks_by, clock)
    assert _main_off(tmp_path, off, monkeypatch) == 1         # not "nothing to do": the trade may still be open
    beat = json.loads((tmp_path / "heartbeats" / "rider.heartbeat.json").read_text())
    assert "MetaTrader" in beat["detail"]["waiting"]           # it reported while it waited (the watchdog's check)
    from mintel.rider.journal import RiderJournal
    j = RiderJournal(tmp_path / "rider.sqlite")
    try:
        assert j.trade(ticket)["closed_utc"] is None
    finally:
        j.close()


# ------------------------------------------- review fixes (9 Oct, part 3) --

def go_blind(broker, n):
    """The broker's next ``n`` position lists come back empty, as the MT5
    adapter answers when MetaTrader's positions_get() gives no answer (None)."""
    real = broker.positions
    left = {"n": n}

    def positions(magic=None):
        if left["n"] > 0:
            left["n"] -= 1
            broker.reads += 1
            return []
        return real(magic)
    broker.positions = positions


def stray(broker, clock, ticket=777, magic=MAGIC):
    """A position under ``magic`` at the broker with no row here, its stop 30 pips away."""
    t0 = broker.tick("EURUSD")
    broker.positions_[ticket] = Position(ticket, "EURUSD", Side.BUY, 0.14, t0.ask, round(t0.ask - 0.0030, 5), 0.0,
                                         clock.now, magic=magic)


def test_one_empty_answer_from_the_broker_never_books_a_leftover_as_gone(tmp_path):
    """SYNTHETIC wave_path(seed=3). A LIVE trade is open when the bot starts
    in PAPER, and the broker's first list comes back empty. Before the fix
    the row was booked at once as BROKER_CLOSED? with a made-up figure and
    PAPER never looked again: the real position stayed at the broker with
    nothing managing it. Now the row stays open, the next read finds the
    position, and it is closed and booked with the broker's figure."""
    eng, broker, clock, ticket = open_live(tmp_path)
    go_blind(broker, 1)
    clock.now += dt.timedelta(seconds=2)
    eng2 = RiderEngine(RiderConfig(mode="PAPER"), broker, tmp_path, clock=clock, heartbeat=False)
    eng2.step()
    assert ticket in broker.positions_ and eng2.journal.trade(ticket)["closed_utc"] is None
    assert eng2.leftovers_outstanding()
    clock.now += dt.timedelta(seconds=1)
    eng2.step()
    assert ticket not in broker.positions_
    row = eng2.journal.trade(ticket)
    assert row["exit_reason"] == "SWITCHED TO PAPER" and row["money_source"] == "broker"
    assert not eng2.leftovers_outstanding()


def test_off_after_one_empty_answer_still_closes_what_live_left(tmp_path, monkeypatch):
    """SYNTHETIC wave_path(seed=3). As above, through the OFF start: its own
    first look and the engine's first look both get an empty list. Before
    the fix OFF booked the row as gone and exited 0 with the real position
    still at the broker."""
    import mintel.rider.run as rrun
    monkeypatch.setattr(rrun, "OFF_PASS_SECONDS", 0.0)
    eng, broker, clock, ticket = open_live(tmp_path)
    eng.journal.close()
    go_blind(broker, 2)
    assert _main_off(tmp_path, broker, monkeypatch) == 0
    assert ticket not in broker.positions_ and ticket in broker.deals
    from mintel.rider.journal import RiderJournal
    j = RiderJournal(tmp_path / "rider.sqlite")
    try:
        row = j.trade(ticket)
        assert row["exit_reason"] == "SWITCHED OFF" and row["money_source"] == "broker"
    finally:
        j.close()


def test_off_reads_an_empty_broker_list_twice_before_deciding_nothing_is_left(tmp_path, monkeypatch):
    """SYNTHETIC. Nothing recorded, one stray position under 990811 at the
    broker, and the first list comes back empty. Before the fix OFF exited 0
    at once and left it there; now it looks again a few seconds later, finds
    it, closes it and books it."""
    import mintel.rider.run as rrun
    monkeypatch.setattr(rrun, "OFF_RECHECK_SECONDS", 0.01, raising=False)
    monkeypatch.setattr(rrun, "OFF_PASS_SECONDS", 0.0)
    ticks, pivot = wave_path()
    clock = Clock(pivot)
    broker = FakeBroker({"EURUSD": ticks}, clock)
    stray(broker, clock)
    go_blind(broker, 1)
    assert _main_off(tmp_path, broker, monkeypatch) == 0
    assert 777 not in broker.positions_ and 777 in broker.deals


def test_paper_reads_the_brokers_list_again_once_a_minute(tmp_path):
    """SYNTHETIC wave_path(seed=3). Nothing is left from LIVE when PAPER
    starts; then a position under 990811 shows up (one an empty answer hid).
    Before the fix PAPER never read the broker's list again once it had found
    nothing (it only looked while a LIVE row was open), so the position stayed
    there unmanaged. Now the list is read once a minute - not on every pass -
    and the position is closed and booked."""
    ticks, pivot = wave_path()
    eng, broker, clock = make(tmp_path, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=300), mode="PAPER")
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=5))
    assert not eng.leftovers_outstanding()
    reads = broker.reads
    stray(broker, clock)
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=61))
    assert 777 not in broker.positions_ and 777 in broker.deals
    assert 1 <= broker.reads - reads <= 2                       # once a minute
    row = eng.journal.trade(777)
    assert row["exit_reason"] == "SWITCHED TO PAPER" and row["money_source"] == "broker"


@pytest.mark.parametrize("mode", ["LIVE", "PAPER"])
def test_a_refused_orphan_or_leftover_close_is_tried_once_a_minute_not_on_every_pass(tmp_path, mode):
    """SYNTHETIC wave_path(seed=3). A position under 990811 with no row here
    (LIVE: an orphan; PAPER: left from LIVE), and the broker refuses to close
    it. Before the fix it was sent again on every pass, each time with an
    ORPHAN_REFUSED event: 120 passes, 120 close requests, 120 events. Now
    once a minute: 2 of each. It keeps its broker stop meanwhile, and is
    closed once the broker takes the close."""
    ticks, pivot = wave_path()
    start = pivot - dt.timedelta(seconds=300)
    clock = Clock(start)
    broker = FakeBroker({"EURUSD": ticks}, clock)
    stray(broker, clock)
    broker.fail_close = True
    eng = RiderEngine(RiderConfig(mode=mode), broker, tmp_path, clock=clock, heartbeat=False)
    run(eng, broker, clock, start + dt.timedelta(seconds=119))
    assert len([c for c in broker.closed_calls if c[0] == 777]) == 2
    assert len(eng.journal.events("ORPHAN_REFUSED")) == 2
    assert 777 in broker.positions_ and broker.positions_[777].sl > 0
    broker.fail_close = False
    run(eng, broker, clock, clock.now + dt.timedelta(seconds=60))
    assert 777 not in broker.positions_
    assert eng.journal.trade(777)["money_source"] == "broker"


def test_a_refused_leftover_close_is_not_retried_while_the_fx_market_is_closed(tmp_path):
    """SYNTHETIC. PAPER on a Saturday with a leftover the broker will not
    close: it is tried once, not again all weekend, and again once the
    market opens."""
    ticks, _ = wave_path()
    sat = dt.datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
    clock = Clock(sat)
    broker = FakeBroker({"EURUSD": ticks}, clock)
    stray(broker, clock)
    broker.fail_close = True
    eng = RiderEngine(RiderConfig(mode="PAPER"), broker, tmp_path, clock=clock, heartbeat=False)
    run(eng, broker, clock, sat + dt.timedelta(seconds=300))
    assert len([c for c in broker.closed_calls if c[0] == 777]) == 1
    clock.now = dt.datetime(2026, 10, 12, 8, 0, tzinfo=UTC)          # Monday morning
    eng.step()
    assert len([c for c in broker.closed_calls if c[0] == 777]) == 2


def _lock_inserts(journal):
    """Every new trade row fails ('database is locked') while ``state['on']``."""
    import sqlite3
    real = journal.insert_trade
    state = {"on": True}

    def insert(row):
        if state["on"]:
            raise sqlite3.OperationalError("database is locked")
        return real(row)
    journal.insert_trade = insert
    return state


@pytest.mark.parametrize("unlocked", ["while_open", "after_the_close"])
def test_a_trade_whose_row_could_not_be_written_is_written_later(tmp_path, unlocked):
    """SYNTHETIC wave_path(seed=3), LIVE. rider.sqlite is locked when the
    entry fills. Before the fix the trade was managed in memory but its row
    was never written: it closed with no record at all (and a restart while
    it was open would have closed it as an ORPHAN). Now the row is written as
    soon as the database takes it: on the next pass while the trade is open,
    or with its close by the next minute's bookkeeping once it has closed."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    lock = _lock_inserts(eng.journal)
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    ticket = next(iter(eng.positions))
    assert eng.journal.trade(ticket) is None
    if unlocked == "while_open":
        lock["on"] = False
        clock.now += dt.timedelta(seconds=1)
        broker.advance()
        eng.step()
        row = eng.journal.trade(ticket)
        assert row is not None and row["closed_utc"] is None and row["mode"] == "LIVE"
        run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if not eng.positions else None)
    else:
        run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if not eng.positions else None)
        assert not eng.positions and eng.journal.trade(ticket) is None
        lock["on"] = False
        clock.now += dt.timedelta(seconds=61)
        broker.advance()
        eng.step()
    row = eng.journal.trade(ticket)
    assert row["closed_utc"] is not None and row["money_source"] == "broker" and row["exit_reason"]
    assert row["entry_family"] and row["volume"] == pytest.approx(0.14)


def test_an_orphan_close_whose_row_could_not_be_written_is_recorded_next_minute(tmp_path):
    """SYNTHETIC. LIVE closes an orphan while rider.sqlite is locked. Before
    the fix the booking was dropped; now it is written next minute."""
    ticks, pivot = wave_path()
    start = pivot - dt.timedelta(seconds=300)
    clock = Clock(start)
    broker = FakeBroker({"EURUSD": ticks}, clock)
    stray(broker, clock)
    eng = RiderEngine(RiderConfig(mode="LIVE"), broker, tmp_path, clock=clock, heartbeat=False)
    lock = _lock_inserts(eng.journal)
    eng.step()
    assert 777 not in broker.positions_ and eng.journal.trade(777) is None
    lock["on"] = False
    clock.now += dt.timedelta(seconds=60)
    notes = eng.step()
    row = eng.journal.trade(777)
    assert row["exit_reason"] == "ORPHAN_CLOSED" and row["money_source"] == "broker"
    assert any("now recorded" in n for n in notes)


def test_a_paper_gap_through_the_stop_counts_in_the_mae_as_in_the_replay(tmp_path):
    """SYNTHETIC wave_path(seed=3), PAPER. One tick jumps 10 pips through the
    stop. Before the fix the MAE left out that closing tick (the stop check
    runs before FlowLock X marks a price), so the row showed a loss bigger
    than its worst point. Now the closing tick's closing-side price counts,
    as the replay's _mark: the MAE is the loss less the slippage."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), mode="PAPER")
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    ticket, pos = next(iter(eng.positions.items()))
    stop, side, d, pip = pos.flow.stop, pos.side, pos.side.sign, 0.0001
    worst_before = max(0.0, (pos.entry - pos.flow.worst) * d) / pip
    gap = tick_at(side, stop, 10, clock.now + dt.timedelta(seconds=1))
    broker.ticks_by["EURUSD"] = [t for t in broker.ticks_by["EURUSD"] if t.time <= clock.now] + [gap]
    clock.now += dt.timedelta(seconds=1)
    eng.step()
    row = eng.journal.trade(ticket)
    assert row["exit_reason"] == "STOP"
    mark = gap.bid if d > 0 else gap.ask
    want = round((pos.entry - mark) * d / pip, 2)
    assert row["mae_pips"] == pytest.approx(want, abs=0.011)
    assert row["mae_pips"] == pytest.approx(-row["realised_pips"] - eng.cfg.slippage_pips, abs=0.011)
    assert worst_before < want - 5                                 # the worst seen before was well short of it
