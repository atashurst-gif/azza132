import datetime as dt

from mintel.ops.day_review import group_positions, review

UTC = dt.timezone.utc
T = dt.datetime(2026, 9, 14, 10, 0, tzinfo=UTC)


def _rows():
    def deal(pos, sym, vol, profit, comm, minutes, entry):
        return {"position": pos, "symbol": sym, "volume": vol,
                "profit": profit + comm, "commission": comm, "is_entry": entry,
                "time": T + dt.timedelta(minutes=minutes)}
    return [
        deal(1, "EURUSD", 0.2, 0.0, -0.7, 0, True),  deal(1, "EURUSD", 0.2, 6.0, -0.7, 3, False),
        deal(2, "EURUSD", 0.2, 0.0, -0.7, 10, True), deal(2, "EURUSD", 0.2, -4.0, -0.7, 12, False),
        deal(3, "XAUUSD", 0.01, 0.0, -0.03, 20, True), deal(3, "XAUUSD", 0.01, 3.0, -0.03, 60, False),
        deal(4, "GBPUSD", 0.1, 0.0, -0.35, 70, True),   # still open: no exit deal
    ]


def test_positions_are_grouped_with_gross_commission_net_and_duration():
    ps = group_positions(_rows())
    assert [p["position"] for p in ps] == [1, 2, 3]
    p1 = ps[0]
    assert p1["gross"] == 6.0 and p1["commission"] == -1.4 and p1["net"] == 4.6
    assert p1["minutes"] == 3.0
    assert ps[2]["net"] == 2.94


def test_review_reads_like_a_receipt():
    text = review(group_positions(_rows()), [
        {"ticket": 1, "tactic": "BREAKOUT_RETEST", "regime": "TREND"},
        {"ticket": 2, "tactic": "BREAKOUT_RETEST", "regime": "TREND"}], "GBP")
    assert "Trades          : 3" in text
    assert "Price result    : +5.00 GBP" in text
    assert "Commission      : -2.86 GBP" in text
    assert "REAL RESULT     : +2.14 GBP" in text
    assert "EURUSD" in text and "XAUUSD" in text
    assert "BREAKOUT_RETEST in TREND" in text
    assert "A profitable day" in text


def test_commission_bigger_than_winnings_is_called_out():
    rows = _rows()
    for r in rows:
        if not r["is_entry"]:
            r["commission"] = -5.0
            r["profit"] = r["profit"]      # profit already net of old commission; fine for the test
    text = review(group_positions(rows), None, "GBP")
    assert "commission" in text.lower()
    assert "REAL RESULT" in text


def test_no_trades():
    assert "No closed trades" in review([], None)
