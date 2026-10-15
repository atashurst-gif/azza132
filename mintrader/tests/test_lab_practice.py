"""Practice: the roster's ideas on live prices with no money.

What must hold: the same simulator fills and closes every practice trade (an
entry at the real ask or bid plus slippage, the stop first, the time exit, the
21:55 UK flat); a feed silent for more than 5 minutes with a position open closes
it at the first bar back (FEED_GAP); a restart resumes the open positions and
walks the bars missed; nothing happens on a UK weekend; the status file (the
watchdog's sign of life) is written and no heartbeat file ever is; the practice
figures in summary.json are refreshed and nothing else in it; every family can
signal on the newest bar of a day still being traded (as it would in the past
test), which is the only bar practice acts on.

EVERY FIGURE HERE IS MADE-UP TEST DATA (made-up prices through a made-up bridge).
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from mintel.broker import wire
from mintel.broker.base import Bar, Tick
from mintel.lab import (PRACTICE, LabPaths, LabSettings, day_min, epoch_min, minute_to_dt, sim, synth)
from mintel.lab import families as FAM
from mintel.lab.cut import CutState, grid_hash, save_cut
from mintel.lab.families import get as family
from mintel.lab.ledger import LabLedger
from mintel.lab.practice import Practice
from mintel.lab.prices import ALLOWED, PriceFeed
from mintel.lab.status import status_fresh
from mintel.lab.store import BarStore, write_universe

UTC = dt.timezone.utc
LIVE_DAY = "2026-10-13"                                   # a Tuesday (BST: the UK day starts 23:00 UTC)
SYMS = ("EURUSD", "GBPUSD")


class FakeBridge:
    """A MADE-UP MetaTrader bridge. It answers the wire calls a BridgeBroker makes from
    made-up minute prices, served up to the fake clock's minute (the forming bar
    included, as MetaTrader does), and records EVERY method it is asked. It also holds
    a made-up account (deals and positions) that nothing the lab does may change."""

    def __init__(self, clock, cols: dict, entries: dict, hole=None):
        self.clock = clock
        self.cols = cols
        self.entries = entries
        self.hole = hole or (0, 0)                        # [from, to) minutes with no bars at all
        self.calls: list = []
        self.built = 0
        self.deals = [{"ticket": 5001, "symbol": "EURUSD", "profit": 12.5, "magic": 990311}]
        self.positions = [{"ticket": 1001, "symbol": "GBPUSD", "volume": 0.1, "magic": 990311}]

    def factory(self, **kw):
        self.built += 1
        return self

    def _rows(self, sym):
        now_min = epoch_min(self.clock())
        t, o, h, l, c, s, v = self.cols[sym]
        pt = self.entries[sym].spec.point
        out = []
        for i in range(len(t)):
            if t[i] > now_min:
                break
            if self.hole[0] <= t[i] < self.hole[1]:
                continue
            out.append((t[i], o[i] * pt, h[i] * pt, l[i] * pt, c[i] * pt, s[i], v[i]))
        return out

    def _rpc(self, method, retry=True, **args):
        self.calls.append(method)
        if method == "ping":
            return {"ok": True, "mt5_connected": True, "backend": "sim (made up)"}
        if method == "symbols":
            return list(self.cols)
        if method == "spec":
            return wire.spec_out(self.entries[args["symbol"]].spec)
        if method == "server_time":
            return self.clock().replace(tzinfo=None).isoformat()
        if method == "bars":
            rows = self._rows(args["symbol"])[-int(args["count"]):]
            return [wire.bar_out(Bar(minute_to_dt(r[0]), r[1], r[2], r[3], r[4], r[6], 0.0, r[5])) for r in rows]
        if method in ("tick", "ticks"):
            out = {}
            for sym in args.get("symbols") or [args.get("symbol")]:
                now_min = epoch_min(self.clock())
                rows = [r for r in self._rows(sym) if r[0] < now_min]
                if not rows:
                    out[sym] = None
                    continue
                pt = self.entries[sym].spec.point
                bid = rows[-1][4]
                out[sym] = wire.tick_out(Tick(sym, self.clock(), bid, bid + rows[-1][5] * pt))
            return out if method == "ticks" else out[args["symbol"]]
        # anything else would be the lab doing something it must never do
        self.deals.append({"ticket": 0, "made_by": method})
        raise AssertionError(f"the lab asked the bridge for {method!r}")


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def build(tmp_path, *, hole=None, cands=None):
    """A MADE-UP lab: five stored weekdays of two majors, then a live day served by the
    fake bridge; a roster of clock trades (the control family: it trades at a known
    minute, so the test knows exactly what must happen)."""
    paths = LabPaths(tmp_path / "data", tmp_path / "logs")
    store = BarStore(paths)
    ents = {s: synth.entry_for(s) for s in SYMS}
    days = synth.weekdays("2026-10-06", 6)
    assert days[-1] == LIVE_DAY
    live_from = day_min(LIVE_DAY)
    cols = {}
    for s, e in ents.items():
        t, o, h, l, c, sp, v = synth.gen_columns(e, days, seed=17)
        k = t.index(live_from)
        store.merge_columns(s, t[:k], o[:k], h[:k], l[:k], c[:k], sp[:k], v[:k], point=e.spec.point,
                            digits=e.spec.digits, source="SYNTHETIC",
                            now=dt.datetime(2026, 10, 13, 6, tzinfo=UTC))
        cols[s] = (t[k - 1440:], o[k - 1440:], h[k - 1440:], l[k - 1440:], c[k - 1440:], sp[k - 1440:], v[k - 1440:])
    write_universe(paths, list(ents.values()))
    save_cut(paths, CutState(cut_day="2026-10-09", stretch_id="F-2026-10", set_utc="2026-10-01T22:10:00+00:00",
                             grid_hash=grid_hash([])))
    led = LabLedger(paths.sqlite)
    ctl = family("CONTROL_TIME_OF_DAY")
    cands = cands or [c for c in ctl.grid() if c.markets == "FX_MAJOR" and (
        (c.p("hour_uk") == 10 and c.p("side") == "BUY" and c.exit == "time1h")
        or (c.p("hour_uk") == 13 and c.p("side") == "SELL" and c.exit == "time4h"))]
    for k, c in enumerate(cands):
        led.register(c, ctl.name(c))
        led.execute("UPDATE candidates SET stage=? WHERE id=?", (PRACTICE, c.id))
        led.execute("INSERT INTO roster(candidate_id, slot, since_utc, symbols_json) VALUES (?,?,?,?)",
                    (c.id, k + 1, "2026-10-12T22:10:00+00:00", json.dumps(list(SYMS))))
    clock = Clock(dt.datetime(2026, 10, 13, 6, 0, 3, tzinfo=UTC))
    bridge = FakeBridge(clock, {s: cols[s] for s in SYMS}, ents, hole=hole)
    feed = PriceFeed(factory=bridge.factory)
    feed.ping()
    eng = Practice(paths, feed, ledger=led, now_fn=clock, log=lambda m: None, prime_bars=3000)
    return paths, led, clock, bridge, eng, cands


def run_minutes(eng, clock, start: dt.datetime, end: dt.datetime, every_status: int = 15):
    t = start
    k = 0
    while t <= end:
        clock.t = t
        eng.minute(t)
        if k % every_status == 0:
            eng.write(t)
        t += dt.timedelta(minutes=1)
        k += 1


def at(hh, mm):
    return dt.datetime(2026, 10, 13, hh, mm, 3, tzinfo=UTC)


def trades_of(led):
    return [dict(r) for r in led.query("SELECT * FROM practice_trades ORDER BY ticket")]


# --------------------------------------------------------------- a practice day ---
def test_a_practice_day_trades_the_roster_with_the_simulators_own_rules(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    buy, sell = (c for c in sorted(cands, key=lambda c: c.p("hour_uk")))
    run_minutes(eng, clock, at(7, 0), at(16, 30))
    rows = trades_of(led)
    assert len(rows) == 4                                            # each clock trade on each market, once
    assert [r["ticket"] for r in rows] == [970000001, 970000002, 970000003, 970000004]
    for r in rows:
        assert r["mode"] == "PRACTICE" and r["fill_source"] == "tick" and r["exit_reason"] in ("TIME", "STOP")
        assert r["source"] == "SYNTHETIC"                            # the bridge said it is simulated
        assert r["risk_gbp"] == 13.0 and r["stop"] is not None
        assert r["commission_gbp"] > 0 and r["r"] == pytest.approx(r["net_gbp"] / 13.0)
    b = [r for r in rows if r["candidate_id"] == buy.id]
    s = [r for r in rows if r["candidate_id"] == sell.id]
    # 10:00 UK is 09:00 UTC: the signal at that bar's close, the fill on the next minute's tick,
    # out after an hour at the bar's close
    assert {r["opened_utc"] for r in b} == {"2026-10-13T09:01:00+00:00"}
    assert {r["opened_utc"] for r in s} == {"2026-10-13T12:01:00+00:00"}
    for group, out_at in ((b, "2026-10-13T10:01:00+00:00"), (s, "2026-10-13T16:01:00+00:00")):
        for r in group:
            if r["exit_reason"] == "TIME":
                assert r["closed_utc"] == out_at                     # the hold, to the minute
            else:                                                     # the stop came first, at or through it
                assert r["closed_utc"] < out_at
                assert (r["exit_price"] <= r["stop"]) if r["side"] > 0 else (r["exit_price"] >= r["stop"])
    assert {r["exit_reason"] for r in rows} == {"TIME", "STOP"}       # (these made-up prices give both)
    assert all(r["side"] == 1 for r in b) and all(r["side"] == -1 for r in s)
    # a buy pays the ask plus slippage: the tick's ask was the last close plus the spread
    tick = json.loads(b[0]["tick_at_signal_json"])
    assert b[0]["entry"] > tick["ask"]
    assert led.query("SELECT * FROM practice_open") == []
    days = {(r["candidate_id"], r["day"]): (r["net_gbp"], r["trades"]) for r in led.query("SELECT * FROM practice_days")}
    assert days[(buy.id, LIVE_DAY)][1] == 2 and days[(sell.id, LIVE_DAY)][1] == 2
    assert days[(buy.id, LIVE_DAY)][0] == pytest.approx(sum(r["net_gbp"] for r in b))
    # the bridge was asked for prices and nothing else; the made-up account is untouched
    assert set(bridge.calls) <= ALLOWED and bridge.built == 1
    assert bridge.deals == [{"ticket": 5001, "symbol": "EURUSD", "profit": 12.5, "magic": 990311}]
    # the status file, and no heartbeat
    st = json.loads(paths.status.read_text())
    assert st["feed_ok"] is True and st["synthetic"] is True and st["mode"] == "PAPER"
    assert {r["id"] for r in st["roster"]} == {buy.id, sell.id}
    assert not (paths.data_dir / "heartbeats").exists() or not any(
        "lab" in f.name for f in (paths.data_dir / "heartbeats").iterdir())


def test_the_caps_hold_one_position_a_market_and_six_entries_a_day(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    slot = next(iter(eng.slots.values())) if eng.slots else None
    eng.load_roster()
    slot = next(iter(eng.slots.values()))
    slot.entries[f"EURUSD|{LIVE_DAY}"] = 6                           # already six today on EURUSD
    run_minutes(eng, clock, at(8, 55), at(9, 5))
    opened = {(r["candidate_id"], r["symbol"]) for r in led.query("SELECT * FROM practice_open")}
    assert (slot.cid, "EURUSD") not in opened and eng.skipped.get("day_cap", 0) >= 1


def test_a_restart_resumes_the_open_positions_and_walks_the_missed_bars(tmp_path):
    a = tmp_path / "a"
    paths, led, clock, bridge, eng, cands = build(a)
    run_minutes(eng, clock, at(7, 0), at(16, 30))
    whole = [(r["candidate_id"], r["symbol"], r["opened_utc"], r["closed_utc"], round(r["net_gbp"], 6))
             for r in trades_of(led)]
    led.close()
    b = tmp_path / "b"
    paths, led, clock, bridge, eng, cands = build(b)
    run_minutes(eng, clock, at(7, 0), at(9, 15))
    assert len(led.query("SELECT * FROM practice_open")) == 2         # the buys are open
    led.close()
    # the process dies at 09:15 and comes back at 09:45: the bars in between are walked through
    # (one of the two is stopped out at 09:21 while it is down, exactly as it was without the restart)
    led2 = LabLedger(paths.sqlite)
    feed = PriceFeed(factory=bridge.factory)
    feed.ping()
    eng2 = Practice(paths, feed, ledger=led2, now_fn=clock, log=lambda m: None, prime_bars=3000)
    run_minutes(eng2, clock, at(9, 45), at(16, 30))
    again = [(r["candidate_id"], r["symbol"], r["opened_utc"], r["closed_utc"], round(r["net_gbp"], 6))
             for r in trades_of(led2)]
    assert again == whole                                            # the same trades, to the penny
    assert set(bridge.calls) <= ALLOWED


def test_a_restart_without_the_state_file_rebuilds_from_the_row(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    run_minutes(eng, clock, at(7, 0), at(9, 15))
    (paths.root / "practice_state.json").unlink()
    eng2 = Practice(paths, PriceFeed(factory=bridge.factory), ledger=led, now_fn=clock, log=lambda m: None,
                    prime_bars=3000)
    run_minutes(eng2, clock, at(9, 16), at(10, 30))
    rows = trades_of(led)
    assert len(rows) == 2 and {r["exit_reason"] for r in rows} == {"TIME", "STOP"}
    assert [r["closed_utc"] for r in rows if r["exit_reason"] == "TIME"] == ["2026-10-13T10:01:00+00:00"]


def test_a_silent_feed_closes_an_open_position_at_the_first_bar_back(tmp_path):
    hole = (day_min(LIVE_DAY) + 9 * 60 + 20, day_min(LIVE_DAY) + 9 * 60 + 35)      # no bars 09:20-09:34 UTC
    paths, led, clock, bridge, eng, cands = build(tmp_path, hole=hole)
    run_minutes(eng, clock, at(7, 0), at(10, 30))
    rows = [r for r in trades_of(led) if r["opened_utc"] == "2026-10-13T09:01:00+00:00"]
    assert len(rows) == 2 and {r["exit_reason"] for r in rows} == {"FEED_GAP"}
    assert {r["closed_utc"] for r in rows} == {"2026-10-13T09:36:00+00:00"}       # 09:35's close


def test_nothing_happens_on_a_uk_weekend(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    sat = dt.datetime(2026, 10, 17, 10, 0, 3, tzinfo=UTC)
    clock.t = sat
    done = eng.minute(sat)
    eng.write(sat)
    assert done["weekend"] is True and bridge.calls == ["ping"]
    st = json.loads(paths.status.read_text())
    assert "weekend" in st["resting"]
    assert status_fresh(paths.status, clock=lambda: sat + dt.timedelta(seconds=60))() is True


def test_the_status_file_is_the_watchdogs_sign_of_life(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    probe_now = [at(9, 0)]
    probe = status_fresh(paths.status, 180, clock=lambda: probe_now[0])
    assert probe() is False                                         # nothing written yet
    run_minutes(eng, clock, at(8, 58), at(9, 2), every_status=1)
    probe_now[0] = at(9, 4)
    assert probe() is True
    probe_now[0] = at(9, 6)
    assert probe() is False                                         # over 3 minutes old: not answering
    st = json.loads(paths.status.read_text())
    assert set(st) >= {"updated_utc", "mode", "roster", "open_positions", "last_bar_utc", "feed_ok",
                       "feed_age_s", "last_error"}
    for p in st["open_positions"]:
        assert set(p) <= {"id", "name", "symbol", "side", "volume", "entry", "stop", "target", "opened_utc",
                          "r_now"}


def test_the_practice_refresh_changes_only_the_practice_figures(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    buy = next(c for c in cands if c.p("hour_uk") == 10)
    before = {"schema": 1, "written_utc": "2026-10-12T22:40:00+00:00", "synthetic": True, "funnel": {"ideas": 632},
              "candidates": [{"id": buy.id, "stage": PRACTICE, "headline": "kept", "figures": {}, "practice": None},
                             {"id": "lab-other", "stage": "ON THE BENCH", "figures": {"x": 1}}]}
    paths.summary.write_text(json.dumps(before))
    run_minutes(eng, clock, at(8, 59), at(10, 30), every_status=200)
    eng.write(at(10, 30))
    after = json.loads(paths.summary.read_text())
    assert after["funnel"] == before["funnel"] and after["candidates"][1] == before["candidates"][1]
    c = after["candidates"][0]
    assert c["headline"] == "kept" and c["figures"]["trades"] == 2 and c["figures"]["practice"] is True
    assert c["figures"]["source"].startswith("data/lab/lab.sqlite practice_trades")
    assert len(c["practice_trades"]) == 2 and c["practice"]["slot"] in (1, 2)
    assert after["written_utc"] != before["written_utc"]


def test_switching_the_lab_off_stops_practice(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    LabSettings.from_dict({"mode": "OFF"}).save(paths.data_dir)
    assert eng.run(sleep=lambda s: None, max_ticks=5) == 0
    assert json.loads(paths.status.read_text())["mode"] == "OFF"


# --------------------------------------------- live equals the past test ---
def _cols_of(ser, k):
    return [list(getattr(ser, n)[:k]) for n in ("t", "o", "h", "l", "c", "s", "v")]


@pytest.mark.parametrize("code", [c for c in FAM.FAMILIES if c != "NEWS_MOVE"])
def test_every_family_signals_on_the_newest_bar_of_a_live_day(code):
    """Practice acts on the newest finished bar only. Every signal the past test would
    take at a bar must also be there when that bar is the newest of a day still being
    traded. (This found the clock trade's live-day fault, fixed on 12 Oct.)"""
    ent = synth.entry_for("EURUSD")
    days = synth.weekdays("2026-07-01", 7)
    cols = synth.gen_columns(ent, days, seed=21)
    ser = synth.series_from_columns(ent, list(cols))
    fam = FAM.get(code)
    sample = [c for c in fam.grid() if c.markets in ("FX_MAJOR", "FX_MINOR")][::7][:5]
    checked = 0
    for cand in sample:
        p = sim.family_params(cand)
        want = []
        for dv in ser.dayviews():
            lo, hi = dv.span(cand.session)
            if hi - lo < 2:
                continue
            want += [s.i for s in fam.signals(dv, p, (lo, hi), None) if lo <= s.i < hi]
        for i in want[:8]:
            short = synth.series_from_columns(ent, _cols_of(ser, i + 1))
            dv = short.dayviews()[-1]
            lo, hi = dv.span(cand.session)
            got = [s.i for s in fam.signals(dv, p, (lo, hi), None)] if lo <= i < hi else []
            assert i in got, (code, cand.id, i)
            checked += 1
    assert checked > 0


# ------------------------------------------------- every idea practises (13 Oct) ---
# Aaron, 13 Oct: "I just want all in practice unless any earn the right [to real money]". With practise_all on
# (data/lab.json, the default) every idea in the ledger practises, binned ones included, each on its own.
def _register(led, cands, stage):
    for c in cands:
        led.register(c, FAM.get(c.family).name(c))
        led.execute("UPDATE candidates SET stage=? WHERE id=?", (stage, c.id))


def test_practise_all_puts_binned_and_untested_ideas_on_the_roster_and_they_trade(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    ctl = family("CONTROL_TIME_OF_DAY")
    binned = next(c for c in ctl.grid() if c.markets == "FX_MAJOR" and c.p("hour_uk") == 10
                  and c.p("side") == "SELL" and c.exit == "time1h")
    idea = next(c for c in ctl.grid() if c.markets == "FX_MAJOR" and c.p("hour_uk") == 10
                and c.p("side") == "BUY" and c.exit == "time4h")
    _register(led, [binned], "BINNED")
    _register(led, [idea], "IDEA")
    events = led.one("SELECT COUNT(*) FROM events")[0]
    assert LabSettings().practise_all is True
    eng.load_roster()
    assert set(eng.slots) == {c.id for c in cands} | {binned.id, idea.id}
    assert eng.slots[binned.id].symbols == list(SYMS)            # every stored market of its kind
    assert list(eng.slots)[:2] == [c.id for c in cands]           # the nightly's roster first, in its order
    run_minutes(eng, clock, at(8, 55), at(10, 30))
    got = {r["candidate_id"] for r in trades_of(led)}
    assert binned.id in got and idea.id in got                    # practised on live prices, no money
    # the stage history is the rule's and is left alone: no row moved, no event written
    assert led.candidate(binned.id)["stage"] == "BINNED" and led.candidate(idea.id)["stage"] == "IDEA"
    assert led.one("SELECT COUNT(*) FROM events")[0] == events
    st = eng.status(at(10, 30))
    assert st["practising"] == 4 and st["practise_all"] is True
    assert set(bridge.calls) <= ALLOWED                           # still prices only


def test_with_practise_all_off_only_the_nightlys_roster_practises(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    ctl = family("CONTROL_TIME_OF_DAY")
    other = next(c for c in ctl.grid() if c.markets == "FX_MAJOR" and c.p("hour_uk") == 15)
    _register(led, [other], "BINNED")
    eng.settings = LabSettings.from_dict({"practise_all": False})
    assert LabSettings.from_dict({"practise_all": "off"}).practise_all is False
    eng.load_roster()
    assert set(eng.slots) == {c.id for c in cands}


def _many(tmp_path):
    """The made-up lab with many ideas on its two majors: every major-pair clock trade, breakout and
    back-to-the-average idea (88), stage BINNED for all but the roster's two."""
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    more = [c for code in ("CONTROL_TIME_OF_DAY", "RANGE_BREAK", "VWAP_FADE")
            for c in FAM.get(code).grid() if c.markets == "FX_MAJOR" and c.id not in {x.id for x in cands}]
    _register(led, more, "BINNED")
    return paths, led, clock, bridge, eng, cands + more


def test_features_are_worked_out_once_per_market_per_bar_and_signals_are_shared(tmp_path, monkeypatch):
    from mintel.lab import features as F
    from mintel.lab.store import SymbolSeries
    paths, led, clock, bridge, eng, cands = _many(tmp_path)
    assert len(cands) > 80
    pushed: dict = {}
    real_push = F.Streaming.push

    def counting(self, t, *a):
        pushed[(id(self), t)] = pushed.get((id(self), t), 0) + 1
        return real_push(self, t, *a)

    monkeypatch.setattr(F.Streaming, "push", counting)
    run_minutes(eng, clock, at(7, 0), at(7, 1))                    # primed
    before = {s: ser.pushes for s, ser in eng.series.items()}
    pushed.clear()
    calls0 = eng.signal_calls
    run_minutes(eng, clock, at(7, 2), at(16, 0))
    minutes = 16 * 60 - (7 * 60 + 2) + 1
    # each market's features: one push of each bar, however many ideas watch it
    assert {s: ser.pushes - before[s] for s, ser in eng.series.items()} == {s: minutes for s in SYMS}
    assert set(pushed.values()) == {1} and len(pushed) == minutes * len(SYMS)
    # a family's signals run once per (settings, session) a market a bar, never once per idea
    groups = {sl.sig_key for sl in eng.slots.values()}
    assert len(groups) < len(eng.slots) / 2
    assert eng.signal_calls - calls0 <= len(groups) * len(SYMS) * minutes
    # the grown higher-timeframe bars and spreads are exactly what the batch code builds from scratch
    for ser in eng.series.values():
        for m in (5, 15, 60):
            a, b = ser.tf(m), SymbolSeries.tf(ser, m)
            assert (list(a.t), list(a.c), list(a.end_i)) == (list(b.t), list(b.c), list(b.end_i))
        eff, assumed = ser.spread_arrays()
        ser._memo.clear()
        eff2, assumed2 = SymbolSeries.spread_arrays(ser)
        assert list(eff) == list(eff2) and bytes(assumed) == bytes(assumed2)


def test_sharing_the_signals_changes_no_trade(tmp_path):
    """The same day with every idea computing its own signals gives the very same trades."""
    whole = []
    for k, share in enumerate((True, False)):
        paths, led, clock, bridge, eng, cands = _many(tmp_path / str(k))
        eng.load_roster()
        if not share:
            for sl in eng.slots.values():
                sl.sig_key = (sl.cid,)
        run_minutes(eng, clock, at(7, 0), at(16, 30), every_status=600)
        whole.append([(r["candidate_id"], r["symbol"], r["opened_utc"], r["closed_utc"], round(r["net_gbp"], 6))
                      for r in trades_of(led)])
        led.close()
    assert whole[0] == whole[1] and len(whole[0]) > 20


def test_the_per_idea_caps_hold_and_there_is_no_cap_across_the_lab(tmp_path):
    paths, led, clock, bridge, eng, cands = _many(tmp_path)
    most = 0
    t = at(7, 0)
    while t <= at(16, 30):
        clock.t = t
        eng.minute(t)
        per: dict = {}
        for (cid, sym) in eng.open:
            per[cid] = per.get(cid, 0) + 1
        assert max(per.values(), default=0) <= 3                    # three open per idea
        most = max(most, len(eng.open))
        t += dt.timedelta(minutes=1)
    assert most > 12                                                # the old 12 across the lab is gone
    rows = trades_of(led)
    by: dict = {}
    for r in rows:
        by.setdefault((r["candidate_id"], r["symbol"]), []).append(r)
    for key, rs in by.items():
        rs.sort(key=lambda r: r["opened_utc"])
        assert len(rs) <= 6                                          # six entries a market a UK day
        for a, b in zip(rs, rs[1:]):
            gap = (dt.datetime.fromisoformat(b["opened_utc"]) - dt.datetime.fromisoformat(a["closed_utc"]))
            assert gap >= dt.timedelta(minutes=5), key              # one at a time, then the five-bar pause


@pytest.mark.parametrize("code", list(FAM.FAMILIES))
def test_a_familys_signals_read_only_its_own_settings_and_the_session(code):
    """Practice shares one signals call among ideas that differ only in their exit, markets or news rule
    (``practice.signal_key``). That is exact only while no family's ``signals`` reads those from ``p``, and only
    a family marked ``reads_news`` reads the news array: this reads each family's source to keep it so."""
    import inspect
    import re
    fam = FAM.get(code)
    src = inspect.getsource(type(fam).signals)
    body = src.split("\n", 1)[1]                                      # below the def line
    assert not re.search(r"""p\[\s*["'](exit|markets|news)["']\s*\]""", body), code
    assert not re.search(r"""p\.get\(""", body), code
    if not fam.reads_news:
        assert not re.search(r"\bnews\b", body), code


# ------------------------------------- a session's first bar, as the past test (14 Oct) ---
# A breakout idea's settings and session must change what it trades live exactly as they do in the past test.
# MADE-UP minutes: EURUSD flat at 1.10000 with a 100-point range every minute (a 5-minute swing of about 100
# points, so the costs pass), a lone spike high at 04:00 UTC that only the 240-minute high still sees at 07:00,
# and five planted breakouts on 13 Oct: a close above for one minute, the next minute opening there and falling
# back (so the stop is hit at once, by the tick fill and the past test's next-open fill alike).
P0 = 110000
BREAKS = {(7, 0): 80, (9, 30): 80, (13, 0): 400, (17, 0): 80, (20, 0): 150}      # UTC on 13 Oct -> close above


def crafted_columns():
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    for day in ("2026-10-12", LIVE_DAY):
        for m in range(day_min(day), day_min(day) + 1440):
            t.append(m), o.append(P0), h.append(P0 + 50), l.append(P0 - 50), c.append(P0), s.append(10), v.append(100)
    base = day_min(LIVE_DAY)
    h[t.index(base + 4 * 60)] = P0 + 300
    for (hh, mm), up in BREAKS.items():
        k = t.index(base + hh * 60 + mm)
        c[k], h[k] = P0 + up, P0 + up + 20
        o[k + 1], h[k + 1] = P0 + up, P0 + up
    return t, o, h, l, c, s, v


def breakout_ideas():
    """Breakout ideas on the majors that differ ONLY in their lookback (120 / 240) and session (London / NY)."""
    return {(c.session, c.p("lookback_min")): c for c in family("RANGE_BREAK").grid()
            if c.markets == "FX_MAJOR" and c.exit == "t2" and c.news == "IGNORE" and c.p("lookback_min") in (120, 240)}


def test_breakouts_that_differ_in_lookback_or_session_trade_differently_live_and_as_the_past_test(tmp_path):
    """London opens at 07:00 UTC (BST): the 07:00 breakout is the London session's FIRST bar. The past test
    takes it; practice dropped it (a session one bar old was read as a session too short to trade), so live a
    London breakout could never fire on the open. Each idea here must enter exactly where the past test does on
    the same bars, and the four must not trade alike."""
    paths = LabPaths(tmp_path / "data", tmp_path / "logs")
    store = BarStore(paths)
    ent = synth.entry_for("EURUSD")
    cols = crafted_columns()
    k = cols[0].index(day_min(LIVE_DAY))
    store.merge_columns("EURUSD", *(x[:k] for x in cols), point=ent.spec.point, digits=ent.spec.digits,
                        source="SYNTHETIC", now=dt.datetime(2026, 10, 13, 6, tzinfo=UTC))
    write_universe(paths, [ent])
    ideas = breakout_ideas()
    led = LabLedger(paths.sqlite)
    _register(led, list(ideas.values()), "IDEA")
    clock = Clock(at(6, 50))
    bridge = FakeBridge(clock, {"EURUSD": cols}, {"EURUSD": ent})
    feed = PriceFeed(factory=bridge.factory)
    feed.ping()
    eng = Practice(paths, feed, ledger=led, now_fn=clock, log=lambda m: None, prime_bars=3000)
    run_minutes(eng, clock, at(6, 50), at(20, 10), every_status=600)
    live = {key: [r["opened_utc"][11:16] for r in trades_of(led) if r["candidate_id"] == c.id]
            for key, c in ideas.items()}
    ser = synth.series_from_columns(ent, cols)
    win = synth.window_of([ser], [LIVE_DAY])
    past = {key: [tr.opened_utc[11:16] for tr in sim.run(c, win).trades] for key, c in ideas.items()}
    want = {("LONDON", 120): ["07:01", "09:31", "13:01"], ("LONDON", 240): ["13:01"],
            ("NEWYORK", 120): ["13:01", "17:01", "20:01"], ("NEWYORK", 240): ["13:01", "20:01"]}
    assert past == want
    assert live == want                                   # was: London 120 without its 07:01 entry
    assert len({tuple(x) for x in live.values()}) == 4    # lookback and session each change the record
    assert set(bridge.calls) <= ALLOWED
    led.close()


# ------------------------------------------ the flat comes on the CLOCK (14 Oct) ---
# Seen live on 14 Oct: ten AUS200 practice positions opened 12:38-13:49 UTC were still open at 22:33 UTC. AUS200's
# last bar was 19:59 UTC, and a position was only walked - so only flattened - when its market printed a new bar:
# the 21:55 UK flat never came, and the next bar (the next session) would have closed them FEED_GAP, held
# overnight. The flat now comes on the clock: at 21:55 UK (21:00 on a Friday) every position still open is closed
# at its market's last price before the flat, as the past test closes it at its last bar before the flat.
# MADE-UP minutes: an index flat at 8800.0 with a 10-point range every minute, so neither the stop nor the
# 2R target is ever touched and the position can only leave by the flat.
AUS_P0 = 88000                                            # points of 0.1


def aus200():
    import dataclasses
    base = synth.entry_for("US500")                       # made-up index specification, renamed
    return dataclasses.replace(base, name="AUS200", spec=dataclasses.replace(base.spec, name="AUS200"))


def stopping_columns(days, last_bar: dict, reopen: dict = None):
    """Flat minutes on each UTC day of ``days``; on a day in ``last_bar`` none after that UTC (hh, mm) (the
    market stops printing), resuming on a day in ``reopen`` from that UTC (hh, mm)."""
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    reopen = reopen or {}
    for day in days:
        base = day_min(day)
        stop = base + last_bar[day][0] * 60 + last_bar[day][1] if day in last_bar else None
        start = base + reopen[day][0] * 60 + reopen[day][1] if day in reopen else None
        for m in range(base, base + 1440):
            if stop is not None and m > stop and (start is None or m < start):
                continue                                  # the market prints nothing
            t.append(m), o.append(AUS_P0), h.append(AUS_P0 + 50), l.append(AUS_P0 - 50), c.append(AUS_P0)
            s.append(10), v.append(100)
    return t, o, h, l, c, s, v


def clock_buy_on_the_index(hour_uk: int = 13):
    """The clock trade (it trades at a known minute) bought at ``hour_uk`` UK on the index, with a 2R target
    and no time exit: the flat is its only way out on flat prices."""
    base = next(c for c in family("CONTROL_TIME_OF_DAY").grid() if c.p("hour_uk") == hour_uk
                and c.p("side") == "BUY" and c.markets == "FX_MAJOR" and c.exit == "time1h")
    return base.replace(exit="t2", markets="INDEX")


def index_lab(tmp_path, cols, live_day: str, start: dt.datetime):
    """A MADE-UP lab on the one index: the bars before ``live_day`` stored, the rest served by the fake bridge."""
    paths = LabPaths(tmp_path / "data", tmp_path / "logs")
    store = BarStore(paths)
    ent = aus200()
    k = cols[0].index(day_min(live_day))
    store.merge_columns("AUS200", *(x[:k] for x in cols), point=ent.spec.point, digits=ent.spec.digits,
                        source="SYNTHETIC", now=start)
    write_universe(paths, [ent])
    idea = clock_buy_on_the_index()
    led = LabLedger(paths.sqlite)
    _register(led, [idea], "IDEA")
    clock = Clock(start)
    bridge = FakeBridge(clock, {"AUS200": cols}, {"AUS200": ent})
    feed = PriceFeed(factory=bridge.factory)
    feed.ping()
    eng = Practice(paths, feed, ledger=led, now_fn=clock, log=lambda m: None, prime_bars=3000)
    return paths, led, clock, bridge, eng, idea, ent


def utc(day: str, hh: int, mm: int, ss: int = 3) -> dt.datetime:
    y, mo, d = (int(x) for x in day.split("-"))
    return dt.datetime(y, mo, d, hh, mm, ss, tzinfo=UTC)


def past_test_exit(idea, ent, cols, day: str):
    """The past test's own trade on the same bars (its exit is its last bar before the flat)."""
    ser = synth.series_from_columns(ent, cols)
    got = sim.run(idea, synth.window_of([ser], [day])).trades
    assert len(got) == 1
    return got[0]


def test_a_market_that_stops_printing_before_the_flat_is_closed_at_2155_uk_on_the_clock(tmp_path):
    day = "2026-10-14"                                        # Wednesday (BST): 21:55 UK is 20:55 UTC
    cols = stopping_columns(["2026-10-13", day], {day: (19, 59)}, {day: (23, 0)})
    paths, led, clock, bridge, eng, idea, ent = index_lab(tmp_path, cols, day, utc(day, 11, 50))
    run_minutes(eng, clock, utc(day, 11, 58), utc(day, 20, 54), every_status=600)
    assert [r["opened_utc"] for r in led.query("SELECT * FROM practice_open")] == ["2026-10-14T12:01:00+00:00"]
    run_minutes(eng, clock, utc(day, 20, 55), utc(day, 20, 55))
    assert led.query("SELECT * FROM practice_open") == [] and eng.open == {}      # was: still open at 22:33 UTC
    run_minutes(eng, clock, utc(day, 20, 56), utc(day, 23, 10), every_status=600)  # the market prints again at 23:00
    rows = trades_of(led)
    assert [(r["exit_reason"], r["closed_utc"]) for r in rows] == \
        [("SESSION_END", "2026-10-14T20:55:00+00:00")]                           # was FEED_GAP at 23:01 UTC
    # at the market's last price before the flat (its 19:59 UTC bar) with the usual cost against it: the past
    # test's own exit, to the point
    past = past_test_exit(idea, ent, cols, day)
    assert past.exit_reason == "SESSION_END" and past.closed_utc == "2026-10-14T20:00:00+00:00"
    assert rows[0]["exit_price"] == pytest.approx(past.exit_price)
    assert rows[0]["exit_price"] < AUS_P0 * ent.spec.point                        # a buy sells at the bid less slip
    assert rows[0]["stop"] is not None and rows[0]["mode"] == "PRACTICE"
    assert set(bridge.calls) <= ALLOWED                                           # still prices only
    led.close()


@pytest.mark.parametrize("restart_at", [(20, 30), (21, 30)])
def test_the_clock_flat_never_books_a_price_from_before_the_position(tmp_path, restart_at):
    """Review, 15 Oct: practice started again with the bridge answering NO bars (MT5's copy_rates_from_pos gave
    None, the adapter answers [] with no error) holds only the stored days before the position. The clock flat
    must not book the position at Tuesday's price (+17 R of invented performance): it waits and tries again.
    MADE-UP prices: Tuesday 13 Oct at 9000.0, Wednesday 14 Oct at 8800.0; the market stops at 19:59 UTC."""
    tue, wed = "2026-10-13", "2026-10-14"
    level = {tue: 90000, wed: 88000}
    t, o, h, low, c, sp, v = [], [], [], [], [], [], []
    for d in (tue, wed):
        base = day_min(d)
        for m in range(base, base + 1440):
            if d == wed and m > base + 19 * 60 + 59:
                continue
            p = level[d]
            t.append(m), o.append(p), h.append(p + 50), low.append(p - 50), c.append(p), sp.append(10), v.append(100)
    cols = (t, o, h, low, c, sp, v)
    paths, led, clock, bridge, eng, idea, ent = index_lab(tmp_path, cols, wed, utc(wed, 11, 50))
    run_minutes(eng, clock, utc(wed, 11, 58), utc(wed, 18, 0), every_status=600)
    assert len(led.query("SELECT * FROM practice_open")) == 1
    led.close()
    empty = FakeBridge(clock, {"AUS200": cols}, {"AUS200": ent}, hole=(day_min(tue) - 10 ** 6, 10 ** 9))
    feed = PriceFeed(factory=empty.factory)
    feed.ping()
    led = LabLedger(paths.sqlite)
    eng2 = Practice(paths, feed, ledger=led, now_fn=clock, log=lambda m: None, prime_bars=3000)
    hh, mm = restart_at
    start = utc(wed, hh, mm)
    run_minutes(eng2, clock, start, start + dt.timedelta(minutes=2), every_status=600)
    run_minutes(eng2, clock, max(utc(wed, 20, 50), start + dt.timedelta(minutes=3)), utc(wed, 21, 0),
                every_status=600)
    booked = trades_of(led)
    assert all(r["exit_price"] < 8900 for r in booked)            # never Tuesday's 9000-ish price
    assert booked == [] and len(eng2.open) == 1                    # it waits for a real price, nothing invented
    led.close()
    # the bridge answers again: the bars it missed are read back and it is closed at the flat (21:55 UK, 20:55 UTC)
    # at the market's last price before it - the past test's own exit - never held overnight, never FEED_GAP
    led = LabLedger(paths.sqlite)
    eng3 = Practice(paths, PriceFeed(factory=bridge.factory), ledger=led, now_fn=clock, log=lambda m: None,
                    prime_bars=3000)
    run_minutes(eng3, clock, utc(wed, 21, 5), utc(wed, 21, 7), every_status=600)
    rows = trades_of(led)
    assert [(r["exit_reason"], r["closed_utc"]) for r in rows] == [("SESSION_END", "2026-10-14T20:55:00+00:00")]
    assert rows[0]["exit_price"] < 8850 and eng3.open == {}        # Wednesday's 8800 less the cost, not Tuesday's
    led.close()


@pytest.mark.parametrize("restart", [False, True], ids=["running", "restarted-on-saturday"])
def test_the_friday_flat_comes_on_the_clock_and_nothing_is_held_over_the_weekend(tmp_path, restart):
    """Friday 16 Oct: the market's last bar is 19:29 UTC, before the Friday flat (21:00 UK, 20:00 UTC). Running,
    it is closed at 21:00 UK; started again on the Saturday (the process was down over Friday's flat), it is
    closed at once, booked at Friday's flat - never held to Monday's first bar (a FEED_GAP)."""
    fri, sat, sun = "2026-10-16", "2026-10-17", "2026-10-18"
    cols = stopping_columns(["2026-10-15", fri, "2026-10-19"], {fri: (19, 29)})
    paths, led, clock, bridge, eng, idea, ent = index_lab(tmp_path, cols, fri, utc(fri, 11, 50))
    run_minutes(eng, clock, utc(fri, 11, 58), utc(fri, 18, 0), every_status=600)
    assert len(led.query("SELECT * FROM practice_open")) == 1
    if restart:
        led.close()
        led = LabLedger(paths.sqlite)
        eng = Practice(paths, PriceFeed(factory=bridge.factory), ledger=led, now_fn=clock, log=lambda m: None,
                       prime_bars=3000)
        clock.t = utc(sat, 9, 0)
        assert eng.minute(clock.t)["weekend"] is True
    else:
        run_minutes(eng, clock, utc(fri, 18, 1), utc(fri, 20, 0), every_status=600)
    assert led.query("SELECT * FROM practice_open") == [] and eng.open == {}
    for when in (utc(sat, 10, 0), utc(sun, 12, 0), utc("2026-10-19", 0, 5)):     # Monday's bars change nothing
        clock.t = when
        eng.minute(when)
    rows = trades_of(led)
    assert [(r["exit_reason"], r["closed_utc"]) for r in rows] == \
        [("SESSION_END", "2026-10-16T20:00:00+00:00")]                           # was FEED_GAP on Monday
    past = past_test_exit(idea, ent, cols, fri)
    assert past.exit_reason == "SESSION_END" and rows[0]["exit_price"] == pytest.approx(past.exit_price)
    assert set(bridge.calls) <= ALLOWED
    led.close()
