"""Band Breaker: the noise band, the half-hour checks, the paper positions."""
import datetime as dt
import json
from zoneinfo import ZoneInfo

import pytest

from mintel.broker.base import Bar, Side, TF, Tick
from mintel.bandbreaker import STRATEGY_ID, STRATEGY_LABEL
from mintel.bandbreaker.engine import BandBreaker, BandBreakerConfig, ny

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
