"""Band Breaker: the noise band, the half-hour checks, the paper positions,
and the same engine LIVE on the simulated broker."""
import datetime as dt
import json
from zoneinfo import ZoneInfo

import pytest

from mintel.botexec import MAGIC_BANDBREAKER
from mintel.broker.base import Bar, OrderRequest, Side, TF, Tick
from mintel.broker.sim import SimBroker
from mintel.bandbreaker import STRATEGY_ID, STRATEGY_LABEL
from mintel.bandbreaker.engine import BandBreaker, BandBreakerConfig, ny
from mintel.config import Config

NY = ZoneInfo("America/New_York")
TODAY = dt.date(2026, 10, 7)                       # a Wednesday


def at(day: dt.date, hh: int, mm: int) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY).astimezone(dt.timezone.utc)


class Spec:
    """US500-like: point 0.01, 1 index point = 1.00 per lot."""
    point = 0.01
    volume_min = 0.01
    volume_step = 0.01

    def money(self, dist, volume):
        return dist * volume

    def money_per_lot(self, dist):
        return abs(dist)

    def normalise_price(self, p):
        return round(p, 2)

    def normalise_volume(self, v):
        return max(0.0, round(int(v / 0.01 + 1e-9) * 0.01, 2))


def sessions(n_days: int, level: float = 5000.0, wander: float = 0.004, today_path=None):
    """M30 bars for n_days prior sessions (each wandering `wander` of the open
    by every half hour, so sigma is flat), then today's bars from today_path:
    a list of (HH:MM open-time NY, open, high, low, close)."""
    bars = []
    day = TODAY - dt.timedelta(days=1)
    made = 0
    while made < n_days:
        if day.weekday() < 5:
            o = level
            t = dt.datetime(day.year, day.month, day.day, 9, 30, tzinfo=NY)
            k = 0
            while t.time() < dt.time(16, 0):
                c = o * (1 + wander * (-1 if k % 2 == 0 else 1))     # the last bar (k=12) closes under the open
                bars.append(Bar(t.astimezone(dt.timezone.utc), o, max(o, c), min(o, c), c, 100.0))
                t += dt.timedelta(minutes=30)
                k += 1
            made += 1
        day -= dt.timedelta(days=1)
    bars.sort(key=lambda b: b.time)
    for hhmm, o, h, l, c in (today_path or []):
        hh, mm = map(int, hhmm.split(":"))
        bars.append(Bar(at(TODAY, hh, mm), o, h, l, c, 100.0))
    return bars


def m1_flat(price: float, upto_hh: int, upto_mm: int):
    out = []
    t = at(TODAY, 9, 30)
    end = at(TODAY, upto_hh, upto_mm)
    while t < end:
        out.append(Bar(t, price, price, price, price, 100.0))
        t += dt.timedelta(minutes=1)
    return out


def engine(tmp_path, **kw):
    kw.setdefault("markets", ("US500",))
    return BandBreaker(tmp_path, BandBreakerConfig(**kw), spec_fn=lambda s: Spec(), clock=lambda: at(TODAY, 10, 0))


class TestTheBand:
    def test_sigma_is_the_average_move_from_the_open_and_the_band_sits_on_open_and_prev_close(self, tmp_path):
        e = engine(tmp_path)
        bars = sessions(14, today_path=[("09:30", 5010, 5012, 5008, 5011)])
        b = e.compute_bands(bars, TODAY)
        assert b is not None and b.sessions_used == 14
        assert b.day_open == 5010 and b.prev_close == pytest.approx(5000 * (1 - 0.004))
        assert b.sigma["10:00"] == pytest.approx(0.004) and b.sigma["15:30"] == pytest.approx(0.004)
        ub, lb, s = b.at("10:30")
        assert ub == pytest.approx(5010 * 1.004) and lb == pytest.approx(4980 * 0.996)

    def test_no_band_before_todays_open_or_with_too_little_history(self, tmp_path):
        e = engine(tmp_path)
        assert e.compute_bands(sessions(14), TODAY) is None               # no 09:30 bar yet today
        assert e.compute_bands(sessions(2, today_path=[("09:30", 5000, 5000, 5000, 5000)]), TODAY) is None

    def test_only_the_last_14_sessions_count(self, tmp_path):
        e = engine(tmp_path)
        b = e.compute_bands(sessions(20, today_path=[("09:30", 5000, 5000, 5000, 5000)]), TODAY)
        assert b.sessions_used == 14

    def test_vwap_uses_only_todays_session_bars(self):
        bars = m1_flat(5000.0, 10, 0) + [Bar(at(TODAY - dt.timedelta(days=1), 12, 0), 9000, 9000, 9000, 9000, 100.0)]
        assert BandBreaker.vwap(bars, TODAY, dt.time(9, 30)) == pytest.approx(5000.0)


class TestTheTrade:
    def _run(self, e, now, price, m30_today, m1_upto, m1_price=None):
        bars = {TF.M30: sessions(14, today_path=m30_today), TF.M1: m1_flat(m1_price or price, *m1_upto)}
        return e.step(now, lambda s: Tick(s, now, price - 0.25, price + 0.25),
                      lambda s, tf, n: bars[tf][-n:] if tf is TF.M30 else bars[tf])

    def test_a_half_hour_close_above_the_band_is_a_long_with_the_stop_at_the_band_or_vwap(self, tmp_path):
        e = engine(tmp_path)
        # open 5000, noise 0.4% -> upper band 5020. The 10:00-10:30 bar closes at 5030.
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        notes = self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        assert any("US500 BUY" in n for n in notes), notes
        p = e.open["US500"]
        assert p.entry == pytest.approx(5030.25 + 0.01)                  # ask plus one point of slippage
        # VWAP (flat 5030 bars) sits above the bid, so it cannot be the stop: the band is
        assert p.stop == pytest.approx(4960.08)
        assert p.volume == pytest.approx(10.0 / (p.entry - p.stop), abs=0.01)

    def test_inside_the_band_is_noise_no_trade(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5012, 4995, 5010), ("10:00", 5010, 5015, 5005, 5012)]
        notes = self._run(e, at(TODAY, 10, 30), 5012.0, today, (10, 30))
        assert not e.open and not any("BUY" in n or "SELL" in n for n in notes)
        assert "band" in e.notes_by_market["US500"]

    def test_a_close_below_the_band_is_a_short(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5002, 4960, 4965), ("10:00", 4965, 4970, 4955, 4960)]
        self._run(e, at(TODAY, 10, 30), 4960.0, today, (10, 30))
        assert e.open["US500"].side is Side.SELL

    def test_the_stop_closes_at_the_stop_and_trails_only_in_favour(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        p = e.open["US500"]
        stop0 = p.stop
        # a minute later VWAP has risen to 5040 and price is 5045: the stop follows VWAP up
        notes = self._run(e, at(TODAY, 10, 31), 5045.0, today, (10, 31), m1_price=5040.0)
        assert p.stop == pytest.approx(5040.0) and p.stop > stop0
        hi = p.stop
        # VWAP cannot pull it back down
        self._run(e, at(TODAY, 10, 32), 5042.0, today + [("10:30", 5030, 5046, 5029, 5042)], (10, 32), m1_price=5036.0)
        assert p.stop == hi
        # the bid touches the stop: closed AT the stop
        notes = self._run(e, at(TODAY, 10, 33), hi + 0.25 - 0.5, today, (10, 33))
        assert any("TRAIL_STOP" in n for n in notes) and not e.open
        row = e.closed_since(at(TODAY, 9, 0))[0]
        assert row["exit_price"] == pytest.approx(hi) and row["exit_reason"] == "TRAIL_STOP"

    def test_flat_five_minutes_before_the_close(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        notes = self._run(e, at(TODAY, 15, 55), 5045.0, today, (15, 55))
        assert any("FLAT_BEFORE_CLOSE" in n for n in notes) and not e.open
        assert e.closed_since(at(TODAY, 9, 0))[0]["net_pnl"] > 0

    def test_at_most_three_entries_a_day_and_a_reversal_counts(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        assert e.open["US500"].side is Side.BUY
        down = today + [("10:30", 5030, 5031, 4950, 4960)]
        notes = self._run(e, at(TODAY, 11, 0), 4960.0, down, (11, 0))
        # the long is stopped first (the stop is a stop), then the close under the band is a short
        assert any("BAND_STOP" in n for n in notes) and e.open["US500"].side is Side.SELL
        up = down + [("11:00", 4960, 5040, 4959, 5035)]
        notes = self._run(e, at(TODAY, 11, 30), 5035.0, up, (11, 30))
        assert e.open["US500"].side is Side.BUY and e.entries_today[("US500", TODAY.isoformat())] == 3
        down2 = up + [("11:30", 5035, 5036, 4950, 4955)]
        notes = self._run(e, at(TODAY, 12, 0), 4955.0, down2, (12, 0))
        assert not e.open and "no more entries" in e.notes_by_market["US500"]

    def test_outside_the_session_nothing_opens_and_anything_open_is_closed(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        notes = self._run(e, at(TODAY, 8, 30), 5030.0, today, (10, 30))
        assert not e.open and e.notes_by_market["US500"] == "outside the New York session"
        self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        assert e.open
        notes = self._run(e, at(TODAY, 16, 1), 5045.0, today, (16, 0))
        assert any("SESSION_CLOSE" in n for n in notes) and not e.open

    def test_a_restart_keeps_the_open_position(self, tmp_path):
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        self._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        e.close()
        e2 = engine(tmp_path)
        assert "US500" in e2.open and e2.open["US500"].side is Side.BUY

    def test_off_does_nothing(self, tmp_path):
        e = engine(tmp_path, enabled=False)
        assert e.step(at(TODAY, 10, 30), lambda s: None, lambda s, tf, n: []) == []
        assert e.status()["mode"] == "OFF"


class TestWhatThePageSees:
    def test_status_tab_and_never_in_overall(self, tmp_path):
        from types import SimpleNamespace
        from mintel.ops.attribution import build_strategies
        from mintel.ops.dashboard import render_status
        e = engine(tmp_path)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        bars = {TF.M30: sessions(14, today_path=today), TF.M1: m1_flat(5030.0, 10, 30)}
        fetch = lambda s, tf, n: bars[tf]                          # noqa: E731
        e.step(at(TODAY, 10, 30), lambda s: Tick(s, at(TODAY, 10, 30), 5029.75, 5030.25), fetch)
        e._last_status = 0.0
        e.step(at(TODAY, 15, 55), lambda s: Tick(s, at(TODAY, 15, 55), 5044.75, 5045.25), fetch)
        st = json.loads((tmp_path / "bandbreaker-status.json").read_text())
        assert st["label"] == STRATEGY_LABEL and st["mode"] == "PAPER"
        journal = SimpleNamespace(closed_trades=lambda **kw: [], open_trades=lambda: [])
        out = build_strategies(journal, [], at(TODAY, 0, 0), "GBP", tmp_path, None, now=at(TODAY, 16, 0))
        assert out[STRATEGY_ID]["trades"] == 1 and out[STRATEGY_ID]["net_today"] > 0
        assert out["overall"]["net_today"] == 0.0
        page = render_status({"status": {"bot": "RUNNING"}, "health": {}, "thinking": [], "results": {}, "positions": [],
                              "strategies": out}, STRATEGY_ID)
        assert "Band Breaker" in page and "BAND BREAKER" in page and "New York" in page


class TestInsideTheTrader:
    def test_the_trader_starts_it_with_the_configured_markets(self, tmp_path):
        from mintel.config import Config
        from mintel.engine.trader import Trader
        from mintel.broker.sim import SimBroker
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.bandbreaker.markets = ("US500",)
        broker = SimBroker(["US500", "EURUSD"], start=at(TODAY, 10, 0) - dt.timedelta(days=2), history_bars=500)
        broker.connect()
        tr = Trader(broker, cfg, enable_model=False)
        assert tr.bandbreaker is not None and tr.bandbreaker.cfg.markets == ("US500",)
        assert tr.bandbreaker.cfg.risk_money == 10.0


class LiveRig:
    """The engine in LIVE on a SimBroker. The band fixtures sit around the
    sim's own US500 price so the broker accepts every stop, and each pass
    moves the sim's quote to the price under test before the engine looks."""

    def __init__(self, tmp_path, wrap=None, **kw):
        self.tmp_path = tmp_path
        self.sim = SimBroker(["US500", "EURUSD"], start=at(TODAY, 10, 29), history_bars=50)
        self.sim.connect()
        self.L = float(round(self.sim.tick("US500").mid))
        self.broker = wrap(self.sim) if wrap else self.sim
        self.kw = kw
        self.e = self._build()

    def _build(self):
        kw = dict(self.kw)
        kw.setdefault("markets", ("US500",))
        kw.setdefault("mode", "LIVE")
        gate = kw.pop("entries_allowed", None)
        return BandBreaker(self.tmp_path, BandBreakerConfig(**kw), spec_fn=self.sim.spec, clock=lambda: at(TODAY, 10, 0),
                           broker=self.broker, entries_allowed=gate)

    def restart(self):
        self.e.close()
        self.e = self._build()
        return self.e

    def up(self):
        """Today's bars: the 10:00-10:30 bar closes above the upper band (open x 1.004)."""
        L = self.L
        return [("09:30", L, L + 30, L - 5, L + 25), ("10:00", L + 25, L + 35, L + 20, L + 30)]

    def quote(self, now, price):
        self.sim.now = now - dt.timedelta(seconds=60)
        self.sim.push_bar("US500", price, high=price + 0.5, low=price - 0.5)

    def run(self, now, price, m30_today, m1_upto, m1_price=None):
        self.quote(now, price)
        bars = {TF.M30: sessions(14, level=self.L, today_path=m30_today), TF.M1: m1_flat(m1_price or price, *m1_upto)}
        return self.e.step(now, self.sim.tick, lambda s, tf, n: bars[tf][-n:] if tf is TF.M30 else bars[tf])

    def mine(self):
        return self.sim.positions(MAGIC_BANDBREAKER)


class TestLive:
    def test_a_live_entry_carries_the_magic_and_the_stop_sits_at_the_broker(self, tmp_path):
        rig = LiveRig(tmp_path)
        notes = rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert any("US500 BUY" in n and "live ticket" in n for n in notes), notes
        mine = rig.mine()
        assert len(mine) == 1
        q, p = mine[0], rig.e.open["US500"]
        assert q.ticket == p.ticket and q.magic == MAGIC_BANDBREAKER and q.comment == "BB"
        assert q.sl == pytest.approx(p.stop) and q.tp == 0.0 and p.stop < p.entry
        assert p.entry == pytest.approx(rig.sim.tick("US500").ask)          # the broker's fill is the entry
        assert not rig.sim.positions(Config().magic)                         # Trend & Breakout never sees it
        row = rig.e.db.execute("SELECT mode FROM trades WHERE ticket=?", (p.ticket,)).fetchone()
        assert row["mode"] == "LIVE" and p.mode == "LIVE"
        assert rig.e.checks_since(at(TODAY, 0, 0))[-1]["decision"] == f"entered buy (live, ticket {p.ticket})"
        st = rig.e.status(at(TODAY, 10, 31))
        assert st["mode"] == "LIVE" and st["live"] is True and st["magic"] == MAGIC_BANDBREAKER
        assert st["executor_error"] == "" and st["refused"] == 0 and st["open"][0]["mode"] == "LIVE"

    def test_a_trailed_stop_is_modified_at_the_broker_and_a_refused_move_is_retried(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        stop0 = p.stop
        rig.sim.fault.fail_modify = True
        notes = rig.run(at(TODAY, 10, 31), rig.L + 45, rig.up(), (10, 31), m1_price=rig.L + 40)
        assert any("refused" in n and "will retry" in n for n in notes), notes
        assert p.stop == stop0 and rig.mine()[0].sl == pytest.approx(stop0)   # rolled back to what the broker holds
        rig.sim.fault.fail_modify = False
        rig.run(at(TODAY, 10, 32), rig.L + 45, rig.up(), (10, 32), m1_price=rig.L + 40)
        assert p.stop == pytest.approx(rig.L + 40) and rig.mine()[0].sl == pytest.approx(rig.L + 40)

    def test_an_engine_exit_closes_at_the_broker_and_the_brokers_figure_is_the_result(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ticket = rig.e.open["US500"].ticket
        notes = rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        assert any("FLAT_BEFORE_CLOSE" in n for n in notes) and not rig.e.open and not rig.mine()
        deal = rig.sim.closed_deal(ticket)
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        assert row["ticket"] == ticket and row["exit_reason"] == "FLAT_BEFORE_CLOSE" and row["mode"] == "LIVE"
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["net_pnl"] > 0
        assert row["exit_price"] == pytest.approx(deal["exit_price"])

    def test_a_position_the_broker_closed_is_finalised_once_from_its_deal(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        ticket = p.ticket
        # the next bar trades through the stop: the broker's stop takes it before the engine looks
        notes = rig.run(at(TODAY, 10, 31), p.stop - 5, rig.up(), (10, 31))
        deal = rig.sim.closed_deal(ticket)
        assert deal is not None and deal["reason"] == "SL"
        assert any("closed by the broker" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()
        rows = rig.e.closed_since(at(TODAY, 9, 0))
        assert len(rows) == 1 and rows[0]["ticket"] == ticket and rows[0]["exit_reason"] == "STOP"
        assert rows[0]["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and rows[0]["net_pnl"] < 0
        assert rows[0]["exit_price"] == pytest.approx(deal["exit_price"])
        before = dict(rows[0])
        rig.run(at(TODAY, 10, 32), rig.L + 29, rig.up(), (10, 32))
        assert rig.e.closed_since(at(TODAY, 9, 0)) == [before]       # finalised exactly once

    def test_a_hand_close_at_the_broker_is_not_labelled_a_stop(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ticket = rig.e.open["US500"].ticket
        rig.sim.close(ticket)                                   # closed by hand in the terminal (the sim's deal says MANUAL)
        notes = rig.run(at(TODAY, 10, 31), rig.L + 29, rig.up(), (10, 31))
        assert any("closed by the broker (MANUAL)" in n for n in notes), notes
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        assert row["ticket"] == ticket and row["exit_reason"] == "MANUAL"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))
        # an empty word from the broker still falls back on the only level the trade had
        assert BandBreaker._broker_reason(rig.e._trade_from_row(dict(row)), "") == "STOP"
        assert BandBreaker._broker_reason(rig.e._trade_from_row(dict(row)), "[sl 5162.5]") == "STOP"
        assert BandBreaker._broker_reason(rig.e._trade_from_row(dict(row)), "[tp 5300.0]") == "TARGET"

    def test_a_reversal_closes_at_the_broker_then_opens_the_other_way(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        t1 = rig.e.open["US500"].ticket
        # the 10:30 bar closes through the lower band, but by 11:00 the price is back above the stop:
        # the stop has not been hit, so this is the reversal path, not the stop
        down = rig.up() + [("10:30", rig.L + 30, rig.L + 31, rig.L - 50, rig.L - 45)]
        notes = rig.run(at(TODAY, 11, 0), rig.L - 30, down, (11, 0))
        assert any("REVERSAL" in n for n in notes), notes
        p = rig.e.open["US500"]
        assert p.side is Side.SELL and p.ticket != t1
        mine = rig.mine()
        assert [q.ticket for q in mine] == [p.ticket] and mine[0].side is Side.SELL and mine[0].sl == pytest.approx(p.stop)
        rows = rig.e.closed_since(at(TODAY, 9, 0))
        assert rows[0]["ticket"] == t1 and rows[0]["exit_reason"] == "REVERSAL"
        assert rows[0]["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(t1)["pnl"], 2))
        assert rig.e.entries_today[("US500", TODAY.isoformat())] == 2

    def test_a_refused_order_records_nothing_and_the_bot_keeps_running(self, tmp_path):
        class Refuses:
            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def send(self, req):
                raise RuntimeError("market closed")

        rig = LiveRig(tmp_path, wrap=Refuses)
        notes = rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert any("order refused" in n and "market closed" in n for n in notes), notes
        assert not rig.e.open and not rig.sim.positions() and not rig.sim.order_log
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        checks = rig.e.checks_since(at(TODAY, 0, 0))
        assert len(checks) == 1 and checks[0]["decision"] == "order refused: send failed: market closed"
        assert rig.e.entries_today.get(("US500", TODAY.isoformat()), 0) == 0
        st = rig.e.status(at(TODAY, 10, 31))
        assert st["refused"] == 1 and "market closed" in st["executor_error"]
        # that half hour is spent: no second attempt, and the next pass runs on as normal
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert len(rig.e.checks_since(at(TODAY, 0, 0))) == 1 and not any("skipped" in n for n in notes)
        assert "band" in rig.e.notes_by_market["US500"]

    def test_a_refused_close_keeps_the_position_and_tries_again(self, tmp_path):
        class Flaky:
            def __init__(self, inner):
                self._inner = inner
                self.refuse = 0

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def close(self, ticket, volume=0.0, comment=""):
                if self.refuse > 0:
                    self.refuse -= 1
                    raise RuntimeError("busy")
                return self._inner.close(ticket, volume, comment)

        rig = LiveRig(tmp_path, wrap=Flaky)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        rig.broker.refuse = 1
        notes = rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        assert any("refused" in n and "will retry" in n for n in notes), notes
        assert rig.e.open and len(rig.mine()) == 1
        notes = rig.run(at(TODAY, 15, 56), rig.L + 45, rig.up(), (15, 56))
        assert any("FLAT_BEFORE_CLOSE" in n for n in notes) and not rig.e.open and not rig.mine()

    def test_a_restart_keeps_a_live_position_still_at_the_broker(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ticket = rig.e.open["US500"].ticket
        rig.restart()
        assert rig.e.open["US500"].ticket == ticket and rig.e.open["US500"].mode == "LIVE" and len(rig.mine()) == 1
        rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        assert not rig.mine() and rig.e.closed_since(at(TODAY, 9, 0))[0]["net_pnl"] > 0

    def test_a_restart_finalises_a_position_the_broker_closed_while_the_bot_was_down(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        ticket = p.ticket
        rig.e.close()
        rig.quote(at(TODAY, 10, 40), p.stop - 5)                # the stop is hit while the bot is down
        assert rig.sim.closed_deal(ticket)["reason"] == "SL"
        rig.restart()
        assert not rig.e.open
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        assert row["ticket"] == ticket and row["exit_reason"] == "STOP"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))
        notes = rig.run(at(TODAY, 10, 31), rig.L + 30, rig.up(), (10, 31))
        assert any("closed by the broker" in n for n in notes), notes

    def test_a_restart_closes_an_orphan_with_our_magic_and_leaves_other_bots_alone(self, tmp_path):
        rig = LiveRig(tmp_path)
        t = rig.sim.tick("US500")
        ours = rig.sim.send(OrderRequest("US500", Side.BUY, 0.1, sl=t.bid - 30.0, comment="BB", magic=MAGIC_BANDBREAKER))
        theirs = rig.sim.send(OrderRequest("US500", Side.BUY, 0.1, sl=t.bid - 30.0, comment="MI", magic=Config().magic))
        assert ours.ok and theirs.ok
        rig.restart()
        notes = rig.run(at(TODAY, 10, 30), rig.L, [("09:30", rig.L, rig.L + 2, rig.L - 2, rig.L)], (10, 30))
        assert any("orphan closed" in n and str(ours.ticket) in n for n in notes), notes
        assert not rig.mine() and not rig.e.open
        assert [p.ticket for p in rig.sim.positions(Config().magic)] == [theirs.ticket]
        rows = rig.e.closed_since(at(TODAY - dt.timedelta(days=1), 0, 0))
        assert len(rows) == 1 and rows[0]["ticket"] == ours.ticket and rows[0]["exit_reason"] == "ORPHAN_CLOSED"
        assert rows[0]["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ours.ticket)["pnl"], 2))

    def test_the_account_gate_blocks_a_new_entry_but_never_the_exit(self, tmp_path):
        gate = {"open": False}
        rig = LiveRig(tmp_path, entries_allowed=lambda: gate["open"])
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert not rig.e.open and not rig.sim.order_log
        assert rig.e.notes_by_market["US500"] == "entries paused by the account gate"
        assert "entries paused by the account gate" in rig.e.checks_since(at(TODAY, 0, 0))[-1]["decision"]
        gate["open"] = True
        up2 = rig.up() + [("10:30", rig.L + 30, rig.L + 40, rig.L + 29, rig.L + 38)]
        rig.run(at(TODAY, 11, 0), rig.L + 38, up2, (11, 0))
        assert rig.e.open["US500"].side is Side.BUY
        gate["open"] = False                                     # managing what is open is never gated
        notes = rig.run(at(TODAY, 15, 55), rig.L + 45, up2, (15, 55))
        assert any("FLAT_BEFORE_CLOSE" in n for n in notes) and not rig.e.open and not rig.mine()

    def test_the_gate_holds_in_paper_too(self, tmp_path):
        e = BandBreaker(tmp_path, BandBreakerConfig(markets=("US500",)), spec_fn=lambda s: Spec(),
                        clock=lambda: at(TODAY, 10, 0), entries_allowed=lambda: False)
        today = [("09:30", 5000, 5030, 4995, 5025), ("10:00", 5025, 5035, 5020, 5030)]
        TestTheTrade()._run(e, at(TODAY, 10, 30), 5030.0, today, (10, 30))
        assert not e.open and e.notes_by_market["US500"] == "entries paused by the account gate"

    def test_mode_off_sends_nothing_and_says_so(self, tmp_path):
        rig = LiveRig(tmp_path, mode="OFF")
        assert rig.e.executor is None
        notes = rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert notes == [] and not rig.sim.order_log and not rig.e.checks_since(at(TODAY, 0, 0))
        st = rig.e.status(at(TODAY, 10, 31))
        assert st["mode"] == "OFF" and st["status"] == "OFF" and st["live"] is False

    def test_a_live_row_is_left_alone_by_a_paper_run(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ticket = rig.e.open["US500"].ticket
        rig.e.close()
        rig.kw["mode"] = "PAPER"
        rig.restart()
        quiet = [("09:30", rig.L, rig.L + 2, rig.L - 2, rig.L), ("10:00", rig.L, rig.L + 3, rig.L - 3, rig.L + 1)]
        notes = rig.run(at(TODAY, 10, 31), rig.L + 1, quiet, (10, 31))
        assert not rig.e.open and len(rig.mine()) == 1 and rig.mine()[0].ticket == ticket   # still at the broker, untouched
        assert rig.e.status(at(TODAY, 10, 31))["unmanaged"][0]["ticket"] == ticket
        assert any("opened in LIVE" in n for n in notes), notes

    # ---- the review's findings: accounting across faults and restarts --------
    def test_an_orphan_on_the_same_market_never_displaces_the_managed_position(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ours = rig.e.open["US500"].ticket
        t = rig.sim.tick("US500")
        stray = rig.sim.send(OrderRequest("US500", Side.BUY, 0.1, sl=t.bid - 30.0, comment="BB", magic=MAGIC_BANDBREAKER))
        assert stray.ok and stray.ticket != ours
        rig.restart()
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert any("orphan closed" in n and str(stray.ticket) in n for n in notes), notes
        assert rig.e.open["US500"].ticket == ours                           # the legitimate position is still managed
        assert [q.ticket for q in rig.mine()] == [ours]
        rows = rig.e.closed_since(at(TODAY, 9, 0))
        assert [r["ticket"] for r in rows] == [stray.ticket] and rows[0]["exit_reason"] == "ORPHAN_CLOSED"
        assert rows[0]["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(stray.ticket)["pnl"], 2))
        # the next check sees the position and does not open a second one
        up2 = rig.up() + [("10:30", rig.L + 30, rig.L + 40, rig.L + 29, rig.L + 38)]
        rig.run(at(TODAY, 11, 0), rig.L + 38, up2, (11, 0))
        assert rig.e.checks_since(at(TODAY, 0, 0))[-1]["decision"] == "already buy"
        assert [q.ticket for q in rig.mine()] == [ours] and rig.e.open["US500"].ticket == ours
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades WHERE closed_utc IS NULL").fetchone()[0] == 1

    def test_a_second_open_row_on_one_market_is_closed_and_booked_at_restart(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        first = rig.e.open["US500"].ticket
        # the record an earlier fault leaves behind: a second open LIVE row whose position is at the broker
        t = rig.sim.tick("US500")
        extra = rig.sim.send(OrderRequest("US500", Side.BUY, 0.1, sl=t.bid - 30.0, comment="BB", magic=MAGIC_BANDBREAKER))
        rig.e.db.execute("INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, opened_utc, stop, mode, session_date, peak_r) "
                         "VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                         (extra.ticket, "US500", "BUY", 0.1, extra.price, t.bid - 30.0, at(TODAY, 10, 30, ).isoformat(),
                          t.bid - 30.0, "LIVE", TODAY.isoformat()))
        rig.e.db.commit()
        rig.restart()
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert any("second open row" in n and str(extra.ticket) in n and "closed at the broker" in n for n in notes), notes
        assert rig.e.open["US500"].ticket == first and [q.ticket for q in rig.mine()] == [first]
        row = rig.e.db.execute("SELECT * FROM trades WHERE ticket=?", (extra.ticket,)).fetchone()
        assert row["exit_reason"] == "DUPLICATE_CLOSED" and row["closed_utc"]
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(extra.ticket)["pnl"], 2))
        assert rig.e.status(at(TODAY, 10, 31))["unmanaged"] == []

    def test_an_order_whose_outcome_is_unknown_is_swept_within_a_minute(self, tmp_path):
        class LosesTheReadAfterSend:
            """The bridge times out on the positions read right after the first fill."""
            def __init__(self, inner):
                self._inner = inner
                self.armed = False
                self.faults = 1

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def send(self, req):
                if self.faults:
                    self.faults -= 1
                    self.armed = True
                return self._inner.send(req)

            def positions(self, magic=None):
                if self.armed:
                    self.armed = False
                    raise RuntimeError("bridge timed out")
                return self._inner.positions(magic)

        rig = LiveRig(tmp_path, wrap=LosesTheReadAfterSend)
        notes = rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert any("order sent, state unknown" in n and "bridge timed out" in n for n in notes), notes
        assert not any("skipped this pass" in n for n in notes)
        assert not rig.e.open and len(rig.mine()) == 1                       # filled at the broker, nothing here yet
        filled = rig.mine()[0].ticket
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        assert rig.e.checks_since(at(TODAY, 0, 0))[-1]["decision"] == "order sent, state unknown: bridge timed out"
        assert rig.e.status(at(TODAY, 10, 30))["refused"] == 1
        # the next pass sweeps the broker's list: the fill is closed and booked, never left to run unmanaged
        notes = rig.run(at(TODAY, 10, 30, ) + dt.timedelta(seconds=2), rig.L + 31, rig.up(), (10, 30))
        assert any("orphan closed" in n and str(filled) in n for n in notes), notes
        assert not rig.mine() and not rig.e.open
        row = rig.e.db.execute("SELECT * FROM trades WHERE ticket=?", (filled,)).fetchone()
        assert row["exit_reason"] == "ORPHAN_CLOSED" and row["mode"] == "LIVE"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(filled)["pnl"], 2))
        assert rig.e.entries_today[("US500", TODAY.isoformat())] == 1
        # the 11:00 check runs as normal: one position, with its row
        up2 = rig.up() + [("10:30", rig.L + 30, rig.L + 40, rig.L + 29, rig.L + 38)]
        rig.run(at(TODAY, 11, 0), rig.L + 38, up2, (11, 0))
        assert [q.ticket for q in rig.mine()] == [rig.e.open["US500"].ticket] and rig.e.open["US500"].ticket != filled
        assert rig.e.entries_today[("US500", TODAY.isoformat())] == 2

    def test_a_restart_minutes_after_a_check_does_not_send_the_same_order_twice(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        first = p.ticket
        rig.e.close()
        rig.quote(at(TODAY, 10, 32), p.stop - 5)                # the broker's stop takes it, then Aaron restarts the bot
        rig.restart()
        assert rig.e.last_check["US500"] == f"{TODAY.isoformat()} 10:30"
        notes = rig.run(at(TODAY, 10, 33), rig.L + 30, rig.up(), (10, 33))
        assert not any("BUY" in n and "live ticket" in n for n in notes), notes
        assert not rig.mine() and not rig.e.open and not [o for o in rig.sim.order_log if o.get("ticket") != first]
        checks = [c for c in rig.e.checks_since(at(TODAY, 0, 0)) if c["check_ny"] == "10:30"]
        assert len(checks) == 1 and checks[0]["decision"] == f"entered buy (live, ticket {first})"
        # the next half hour is still taken as normal
        up2 = rig.up() + [("10:30", rig.L + 30, rig.L + 40, rig.L + 29, rig.L + 38)]
        rig.run(at(TODAY, 11, 0), rig.L + 38, up2, (11, 0))
        assert rig.e.checks_since(at(TODAY, 0, 0))[-1]["check_ny"] == "11:00" and rig.e.open

    def test_one_empty_positions_read_does_not_close_the_trade(self, tmp_path):
        class Blinks:
            def __init__(self, inner):
                self._inner = inner
                self.blink = 0

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def positions(self, magic=None):
                if self.blink > 0:
                    self.blink -= 1
                    return []                                   # what the adapter answers when MT5 returns None
                return self._inner.positions(magic)

        rig = LiveRig(tmp_path, wrap=Blinks)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        rig.broker.blink = 1
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert any("not in the broker's list" in n and "kept" in n for n in notes), notes
        assert rig.e.open["US500"] is p and len(rig.mine()) == 1
        assert rig.e.db.execute("SELECT closed_utc FROM trades WHERE ticket=?", (p.ticket,)).fetchone()[0] is None
        notes = rig.run(at(TODAY, 10, 32), rig.L + 32, rig.up(), (10, 32))
        assert any("back in the broker's list" in n for n in notes), notes
        assert p.missing_passes == 0 and rig.e.open["US500"] is p
        rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))             # still managed: flattened on time
        assert not rig.mine() and not rig.e.open
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        assert row["exit_reason"] == "FLAT_BEFORE_CLOSE" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(p.ticket)["pnl"], 2))

    def test_a_ticket_gone_with_no_deal_is_estimated_only_after_a_while_and_corrected_later(self, tmp_path):
        class Hides:
            def __init__(self, inner):
                self._inner = inner
                self.hide = False

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def positions(self, magic=None):
                return [] if self.hide else self._inner.positions(magic)

            def closed_deal(self, ticket):
                return None if self.hide else self._inner.closed_deal(ticket)

        rig = LiveRig(tmp_path, wrap=Hides)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        rig.broker.hide = True
        rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        notes = rig.run(at(TODAY, 10, 31) + dt.timedelta(seconds=30), rig.L + 31, rig.up(), (10, 31))
        assert rig.e.open["US500"] is p and not any("booked" in n for n in notes)   # two reads, 30 s: not yet
        notes = rig.run(at(TODAY, 10, 32), rig.L + 33, rig.up(), (10, 32))
        assert any("booked STOP?" in n and "(estimated)" in n for n in notes), notes
        assert not rig.e.open
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        assert row["ticket"] == p.ticket and row["exit_reason"] == "STOP?"
        assert row["exit_price"] == pytest.approx(rig.sim.tick("US500").bid)       # the last mark
        # the position was there all along: when the broker's list comes back the
        # sweep closes it and the estimate is replaced by the broker's figure
        rig.broker.hide = False
        notes = rig.run(at(TODAY, 10, 33), rig.L + 34, rig.up(), (10, 33))
        assert any("orphan closed" in n and "booked STOP?" in n and "broker's figure" in n for n in notes), notes
        assert not rig.mine()
        row = rig.e.db.execute("SELECT * FROM trades WHERE ticket=?", (p.ticket,)).fetchone()
        assert row["exit_reason"] == "ORPHAN_CLOSED"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(p.ticket)["pnl"], 2))
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1

    def test_a_stop_tightened_at_the_broker_is_never_moved_back(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        rig.run(at(TODAY, 10, 31), rig.L + 45, rig.up(), (10, 31), m1_price=rig.L + 40)
        assert p.stop == pytest.approx(rig.L + 40)
        by_hand = rig.L + 41.5
        assert rig.sim.modify_stops(p.ticket, by_hand, 0.0).ok               # tightened in the terminal
        n_mods = len(rig.sim.modify_log)
        notes = rig.run(at(TODAY, 10, 32), rig.L + 45, rig.up(), (10, 32), m1_price=rig.L + 41)
        assert any("better stop" in n and "adopted" in n for n in notes), notes
        assert rig.mine()[0].sl == pytest.approx(by_hand) and p.stop == pytest.approx(by_hand)
        assert len(rig.sim.modify_log) == n_mods                              # nothing sent against the trade
        assert rig.e.db.execute("SELECT stop FROM trades WHERE ticket=?", (p.ticket,)).fetchone()[0] == pytest.approx(by_hand)
        # the trail still moves it in the trade's favour
        rig.run(at(TODAY, 10, 33), rig.L + 50, rig.up(), (10, 33), m1_price=rig.L + 43)
        assert rig.mine()[0].sl == pytest.approx(rig.L + 43) and p.stop == pytest.approx(rig.L + 43)
        # and a restart adopts the broker's stop too
        assert rig.sim.modify_stops(p.ticket, rig.L + 44.5, 0.0).ok
        rig.restart()
        assert rig.e.open["US500"].stop == pytest.approx(rig.L + 44.5)

    def test_a_live_ticket_equal_to_a_paper_ticket_never_corrupts_the_paper_row(self, tmp_path):
        rig = LiveRig(tmp_path)
        coming = rig.sim._next_ticket + 1                                     # the number the broker will hand out
        rig.e.db.execute("INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, opened_utc, closed_utc, stop, "
                         "exit_price, net_pnl, exit_reason, mode, session_date, peak_r) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                         (coming, "US500", "SELL", 0.5, 5000.0, 5010.0, at(TODAY - dt.timedelta(days=1), 10, 30).isoformat(),
                          at(TODAY - dt.timedelta(days=1), 15, 55).isoformat(), 5010.0, 4990.0, 5.0, "FLAT_BEFORE_CLOSE", "PAPER",
                          (TODAY - dt.timedelta(days=1)).isoformat()))
        rig.e.db.commit()
        paper = dict(rig.e.db.execute("SELECT * FROM trades WHERE ticket=?", (coming,)).fetchone())
        notes = rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        assert any("order refused" in n and f"ticket {coming} already on record" in n and "closed again at once" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()                               # fail closed: no position without a row of its own
        assert rig.sim.closed_deal(coming) is not None
        assert dict(rig.e.db.execute("SELECT * FROM trades WHERE ticket=?", (coming,)).fetchone()) == paper
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
        assert rig.e.checks_since(at(TODAY, 0, 0))[-1]["decision"] == f"order refused: ticket {coming} already on record"
        assert rig.e.status(at(TODAY, 10, 30))["refused"] == 1
        # the sweep has nothing to do afterwards
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert not any("orphan" in n for n in notes) and not rig.mine()

    def test_paper_tickets_never_follow_a_live_ticket(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        live_ticket = rig.e.open["US500"].ticket
        rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        rig.e.close()
        assert live_ticket < 700_000_000
        rig.kw["mode"] = "PAPER"
        e = rig.restart()
        assert e.executor._next == 700_000_001                                 # the paper range, not the broker's
        rig.e.db.execute("UPDATE trades SET mode='PAPER', ticket=700000005 WHERE ticket=?", (live_ticket,))
        rig.e.db.commit()
        e = rig.restart()
        assert e.executor._next == 700_000_006

    def test_an_engine_close_whose_deal_has_not_come_back_is_booked_from_the_fill(self, tmp_path):
        class NoHistory:
            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def closed_deal(self, ticket):
                return None

        rig = LiveRig(tmp_path, wrap=NoHistory)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        p = rig.e.open["US500"]
        notes = rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        assert any("closed FLAT_BEFORE_CLOSE?" in n for n in notes), notes
        assert not rig.mine() and not rig.e.open
        row = rig.e.closed_since(at(TODAY, 9, 0))[0]
        deal = rig.sim.closed_deal(p.ticket)                                   # what the broker really has
        assert row["exit_reason"] == "FLAT_BEFORE_CLOSE?" and row["exit_price"] == pytest.approx(deal["exit_price"])
        assert row["net_pnl"] == pytest.approx(round(rig.sim.spec("US500").money(deal["exit_price"] - p.entry, p.volume), 2))

    def test_a_restart_that_cannot_read_the_broker_keeps_the_rows_and_says_so(self, tmp_path):
        class Down:
            def __init__(self, inner):
                self._inner = inner
                self.down = False

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def positions(self, magic=None):
                if self.down:
                    raise RuntimeError("bridge unreachable")
                return self._inner.positions(magic)

        rig = LiveRig(tmp_path, wrap=Down)
        rig.run(at(TODAY, 10, 30), rig.L + 30, rig.up(), (10, 30))
        ticket = rig.e.open["US500"].ticket
        rig.broker.down = True
        rig.restart()
        assert rig.e.open["US500"].ticket == ticket
        assert rig.e.db.execute("SELECT closed_utc FROM trades WHERE ticket=?", (ticket,)).fetchone()[0] is None
        rig.broker.down = False
        notes = rig.run(at(TODAY, 10, 31), rig.L + 31, rig.up(), (10, 31))
        assert any("could not read the broker's positions at start" in n and "bridge unreachable" in n for n in notes), notes
        assert rig.e.open["US500"].ticket == ticket and len(rig.mine()) == 1
        rig.run(at(TODAY, 15, 55), rig.L + 45, rig.up(), (15, 55))
        assert not rig.mine() and rig.e.closed_since(at(TODAY, 9, 0))[0]["exit_reason"] == "FLAT_BEFORE_CLOSE"
