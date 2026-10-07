"""How far could a trade have gone: the replay must lean pessimistic."""
import datetime as dt

from mintel.broker.base import Bar
from mintel.ops.potential import analyse, render, run, since_utc

T0 = dt.datetime(2026, 10, 6, 13, 0, tzinfo=dt.timezone.utc)


def bars(path, spread=0.0):
    """path: list of (low, high, close) per minute, from the entry minute."""
    return [Bar(T0 + dt.timedelta(minutes=i), c, h, l, c, 100.0, 0.0, spread) for i, (l, h, c) in enumerate(path)]


def trade(**kw):
    t = dict(ticket=1, symbol="US30", side="BUY", entry=100.0, stop=90.0, opened_utc=T0.isoformat(),
             tactic="MOMENTUM_CONTINUATION", realised_r=2.0, mfe_r=2.0)
    t.update(kw)
    return t


def test_a_winner_that_kept_going_shows_how_far():
    p = analyse(trade(), bars([(99, 101, 100), (100, 120, 118), (115, 150, 148), (140, 141, 140)]), 0.01)
    assert p.reach_r == 5.0 and not p.stopped and p.reach_minutes == 2
    # trailing 1 R (10) behind the best price: out at the 150 high minus 10 = 4 R
    assert p.ride_r == 4.0


def test_a_bar_touching_the_stop_counts_as_stopped_first():
    p = analyse(trade(), bars([(99, 101, 100), (89, 130, 125)]), 0.01)
    assert p.stopped and p.reach_r == 0.0 and p.ride_r == -1.0


def test_the_wide_stop_survives_a_dip_the_normal_one_does_not():
    p = analyse(trade(), bars([(99, 101, 100), (88, 101, 95), (95, 140, 139)]), 0.01)
    assert p.stopped and p.reach_r == 0.0                           # the dip bar is stopped before its high
    assert p.reach_wide_r == 4.0 and p.ride_wide_r == 3.9


def test_shorts_pay_the_spread():
    t = trade(side="SELL", entry=100.0, stop=110.0)
    p = analyse(t, bars([(99, 101, 100), (80, 100, 81)], spread=100.0), 0.01)     # 1.00 spread
    assert p.reach_r == 1.9                                          # (100 - (80 + 1)) / 10


def test_render_and_missing_history():
    res = run([trade()], lambda sym, end, n: [], lambda s: 0.01, 8)
    assert "No trades with price history" in render(res, 8)
    res = run([trade()], lambda sym, end, n: bars([(99, 101, 100), (100, 130, 129)]), lambda s: 0.01, 8)
    text = render(res, 8)
    assert "THE BIG WINNERS" in text and "US30" in text


def test_the_since_date_is_read_as_utc():
    assert since_utc("2026-09-17") == dt.datetime(2026, 9, 17, tzinfo=dt.timezone.utc)
    assert since_utc("2026-09-17T10:00+01:00").hour == 9
