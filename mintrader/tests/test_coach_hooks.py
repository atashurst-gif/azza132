"""The coach's hooks in the bots (spec section 11), on SimBroker + PaperBroker + Trader.

THE INVARIANT: with the coach off - COACH-OFF, no envelope, an envelope that
is OFF or invalid - or its policy missing, corrupt, forged, from another
envelope or stale (a lapsed lease drops every grant), every bot does exactly
what it does without the coach: the same orders, the same sizes, the same
daily-loss figure. The reference run is the same trader with every coach hook
detached (no gate, no limits, no order check, no ceiling), which is today's
code path.

Also: Formula 1's orders are identical with the coach OFF, WATCH, GUARD and
PILOT before its judgement; a demoted key goes to practice; a granted index
key goes real and its stop can be moved and it can be closed after a
PaperBroker restart (C3); the Momentum Runner's copy volume 0 means no ride;
Band Breaker and Crowd Fader on PAPER are never gated; Financial Ian's
``_entry_block`` honours the gate and its own £50 limit is unchanged;
``weather.json`` is written atomically.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import zlib
from pathlib import Path

import pytest

from mintel.broker import sim as SIM
from mintel.coach import (F1_KEY, Block, DemoLimits, Entry, Envelope, Paths, RiskCtx)
from mintel.coach import gate as G
from mintel.coach import policy as P
from tests.test_coach_policy import (grant, make_policy, put_envelope, put_policy, put_raw, sealed_obj)
from tests.test_tnb_paper_wiring import TNB, entry, move, paper_on, setup, trader_on

RUNNER = 990_511
MC = "MOMENTUM_CONTINUATION"
US500_KEY = "tnb|SESSION_EXPANSION|INDEX|TREND"
GER40_KEY = "tnb|MOMENTUM_CONTINUATION|INDEX|TREND"        # an index idea the Runner never rides
EURJPY_KEY = "tnb|TREND_PULLBACK|FX_MINOR|*"
SYMBOLS = ("EURGBP", "EURJPY", "US500", "GER40", "EURUSD")

# ================================================================ the same market in every run --
# SimBroker picks each symbol's market regime by ``hash(symbol)``, which Python salts differently in
# every process (PYTHONHASHSEED), so the simulated prices - and with them whether the live Runner
# rides - changed from run to run and these tests passed or failed by chance. Here the regime is
# SimBroker's own choice with a fixed number per symbol (zlib.crc32) in place of the salted hash:
# the same scenario in every process. Nothing the tests check is relaxed.
#
# SimBroker's own regime table, in its order: block = (i // 400 + hash(sym) % 4) % 4, and hash("") is
# 0 in every process, so these four calls read it as written in mintel/broker/sim.py
_SIM_REGIMES = tuple(SIM.SimBroker._regime_for(None, 400 * k, "") for k in range(4))


def _fixed_regime_for(self, i, sym):
    return _SIM_REGIMES[(i // 400 + zlib.crc32(sym.encode()) % 4) % 4]


@pytest.fixture(scope="module", autouse=True)
def same_market_in_every_process():
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(SIM.SimBroker, "_regime_for", _fixed_regime_for)
        yield


def test_the_simulated_market_is_the_same_whatever_the_hash_seed(tmp_path, monkeypatch):
    """Each salt stands in for another process's PYTHONHASHSEED: the prices must not move with it."""
    def closes(salt):
        monkeypatch.setattr(SIM, "hash", lambda o: hash(o) + salt, raising=False)
        sim, _cfg = setup(tmp_path / f"salt{salt}", symbols=SYMBOLS)
        return [round(b.close, 6) for s in SYMBOLS for b in sim._bars[s]]
    base = closes(0)
    assert base and all(closes(salt) == base for salt in (1, 2, 3))
    assert len(set(_SIM_REGIMES)) == 4                     # still SimBroker's four regimes, each in turn


# ================================================================ helpers --
def build(tmp_path, *, runner="PAPER", live_tactics=(), manage_leftovers=True, prepare=None, strip=False):
    sim, cfg = setup(tmp_path, runner=runner, symbols=SYMBOLS)
    sim.margin_factor = 0.01                     # the simulator's margin is not in pounds for a yen cross
    cfg.tnb.live_groups = ("FX_MINOR",)
    cfg.tnb.live_tactics = tuple(live_tactics)
    cfg.tnb.manage_leftovers = manage_leftovers
    cfg.scan.max_new_positions_per_hour = 20
    Path(cfg.ops.data_dir).mkdir(parents=True, exist_ok=True)
    if prepare is not None:
        prepare(cfg, sim)
    pb = paper_on(sim, cfg)
    tr = trader_on(pb, sim, cfg)
    if strip:
        strip_coach(tr)
    return sim, cfg, pb, tr


def strip_coach(tr):
    """Today's code path: every hook detached."""
    tr.coach_gate = None
    tr.risk.limits = None
    if hasattr(tr.broker, "coach_confirm"):
        tr.broker.coach_confirm = None
        tr.broker.ceiling_pct = None


def act_on(tr, sim, state, must=True):
    acct = tr.broker.account()
    tr.last_entry_utc = None
    snap = tr.risk.snapshot(acct, tr.broker.positions(TNB), sim.now)
    tr._last_snapshot = snap
    acted, msg, blocked = tr.act([state], acct, snap, tr.broker.positions(TNB), sim.now)
    if must:
        assert acted is not None, blocked
    return acted, blocked


def full_policy(cfg, sim, *, seq=1, sha=None, **over):
    """A policy that would change everything if it were used: a grant, restrictions on every bot, a cap,
    a block and lifted demo limits."""
    sha = sha or put_envelope(cfg)
    entries = {GER40_KEY: grant(),
               EURJPY_KEY: Entry("PRACTICE", "BANNED_REAL", rule="S1"),
               "tnb|BREAKOUT_ACCEPTANCE|FX_MAJOR|*": Entry("PRACTICE", "BANNED_REAL", rule="S1"),
               "runner|*|*|*": Entry("BASELINE", "PLACED", money_gbp=1.0),
               "bandbreaker|*|*|*": Entry("SKIP", "PAUSED", rule="B2"),
               "crowd|*|*|*": Entry("SKIP", "PAUSED", rule="B2"),
               "ian|*|*|*": Entry("SKIP", "PAUSED", rule="B2")}
    kw = dict(seq=seq, now=sim.now, entries=entries, equity=10_000.0, headroom_gbp=200.0,
              blocks=(Block(("tnb",), ("GBPJPY",), "PRACTICE", "S2"),),
              demo=DemoLimits(max_daily_loss_pct=6.0, max_drawdown_pct=25.0))
    kw.update(over)
    return make_policy(sha, **kw)


def play(sim, tr):
    """The same scenario every time: Formula 1 real, a real minor-pair trade on Aaron's settings, an
    index trade and a major on paper, prices moving, cycles managing them."""
    states = [entry(sim, "EURGBP", 0.0030, tactic=MC),
              entry(sim, "EURJPY", 0.300, tactic="TREND_PULLBACK"),
              entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"),
              entry(sim, "GER40", 25.0, tactic=MC),
              entry(sim, "EURUSD", 0.0025, tactic="BREAKOUT_ACCEPTANCE")]
    for st in states:
        act_on(tr, sim, st)
    tr.cycle()                                                 # the live Runner rides the index trade now
    move(sim, "EURGBP", -0.0012)
    move(sim, "US500", 4.0)
    move(sim, "EURUSD", -0.0011)
    for _ in range(3):
        sim.advance(1)
        tr.cycle()


def record(sim, pb, tr) -> dict:
    acct = tr.broker.account()
    positions = tr.broker.positions(TNB)
    snap = tr.risk.snapshot(acct, positions, sim.now)
    rows = sorted((r["ticket"], r["symbol"], r["volume"], r["risk_money"], r.get("runner_volume"))
                  for r in tr.journal.open_trades())
    return {"orders": [{**dataclasses.asdict(o["req"]), "retcode": o["retcode"], "ticket": o.get("ticket")}
                       for o in sim.order_log],
            "modifies": list(sim.modify_log),
            "paper": [(d.position, d.symbol, d.side.value, d.volume, d.price, d.is_entry, d.reason)
                      for d in pb._deals],
            "journal": rows,
            "positions": sorted((p.ticket, p.symbol, p.volume, p.sl) for p in positions),
            "daily": (snap.daily_pnl, snap.daily_pnl_pct, snap.daily_real_pnl, snap.daily_practice_pnl,
                      tuple((b.name, b.reason) for b in snap.breakers)),
            "bots": (tr._bots_may_enter(), tr.runner.entries_allowed() if tr.runner else None,
                     tr.bandbreaker.entries_allowed() if tr.bandbreaker else None,
                     tr.crowd.entries_allowed() if tr.crowd else None)}


def run(tmp_path, name, *, strip=False, prepare=None, runner="LIVE"):
    sim, cfg, pb, tr = build(tmp_path / name, runner=runner, prepare=prepare, strip=strip)
    try:
        play(sim, tr)
        return record(sim, pb, tr)
    finally:
        tr.journal.close()
        pb.close_db()


# ================================================================ the invariant --
def _no_envelope(cfg, sim):
    pass


def _coach_off(cfg, sim):
    put_policy(cfg, full_policy(cfg, sim), now=sim.now)
    (Path(cfg.ops.data_dir) / "COACH-OFF").write_text("YES")


def _envelope_off(cfg, sim):
    put_policy(cfg, full_policy(cfg, sim), now=sim.now)
    put_envelope(cfg, mode="OFF")


def _envelope_invalid(cfg, sim):
    sha = put_envelope(cfg)
    put_policy(cfg, full_policy(cfg, sim, sha=sha), now=sim.now)
    raw = json.loads((Path(cfg.ops.data_dir) / "coach.json").read_text())
    raw["max_step_pct"] = 2.0                                  # above config.json's 1.5% ceiling: invalid
    (Path(cfg.ops.data_dir) / "coach.json").write_text(json.dumps(raw))


def _policy_missing(cfg, sim):
    put_envelope(cfg)


def _policy_corrupt(cfg, sim):
    put_policy(cfg, full_policy(cfg, sim), now=sim.now)
    p = Paths.of(cfg.ops.data_dir).policy
    p.write_bytes(p.read_bytes()[:-37])


def _policy_forged(cfg, sim):
    obj = sealed_obj(full_policy(cfg, sim))
    obj["entries"][GER40_KEY]["money_gbp"] = 8.0               # edited after it was sealed
    put_raw(cfg, obj, seal=False)


def _policy_old_envelope(cfg, sim):
    sha = put_envelope(cfg)
    put_policy(cfg, full_policy(cfg, sim, sha=sha), now=sim.now)
    put_envelope(cfg, max_trials=1)                             # Aaron changed his envelope since


def _policy_stale_grants(cfg, sim):
    # a lease written ten minutes ago: its grants and lifted limits lapsed (it holds no restriction)
    pol = full_policy(cfg, sim, now=sim.now - dt.timedelta(minutes=10),
                      entries={GER40_KEY: grant()})
    put_raw(cfg, sealed_obj(pol))


def _policy_future(cfg, sim):
    put_raw(cfg, sealed_obj(full_policy(cfg, sim, now=sim.now + dt.timedelta(minutes=10),
                                        entries={GER40_KEY: grant()})))


def _watch(cfg, sim):
    sha = put_envelope(cfg, mode="WATCH")
    put_policy(cfg, full_policy(cfg, sim, sha=sha), now=sim.now)


def _watch_day(cfg, sim):
    sha = put_envelope(cfg, watch_until_utc=(sim.now + dt.timedelta(days=1)).isoformat())
    put_policy(cfg, full_policy(cfg, sim, sha=sha), now=sim.now)


FAILURES = {"no envelope": _no_envelope, "COACH-OFF": _coach_off, "envelope OFF": _envelope_off,
            "envelope invalid": _envelope_invalid, "policy missing": _policy_missing,
            "policy corrupt": _policy_corrupt, "policy forged": _policy_forged,
            "policy from an old envelope": _policy_old_envelope, "policy stale": _policy_stale_grants,
            "policy from the future": _policy_future, "WATCH": _watch,
            "PILOT before the WATCH day ends": _watch_day}


@pytest.fixture(scope="module")
def head(tmp_path_factory):
    return run(tmp_path_factory.mktemp("head"), "head", strip=True)


@pytest.mark.parametrize("name", list(FAILURES))
def test_with_the_coach_off_or_broken_every_bot_does_exactly_what_it_does_without_it(tmp_path, head, name):
    got = run(tmp_path, "coach", prepare=FAILURES[name])
    assert head["orders"], "the scenario places real orders"
    assert any(o["magic"] == RUNNER for o in head["orders"]), "and the live Runner rides"
    for part in ("orders", "modifies", "paper", "journal", "positions", "daily", "bots"):
        assert got[part] == head[part], (name, part)


def test_the_scenario_does_change_with_a_valid_policy(tmp_path, head):
    """The comparison above can see a difference: a valid PILOT policy does change things."""
    def valid(cfg, sim):
        cfg.runner.mode = "LIVE"
        put_policy(cfg, full_policy(cfg, sim, entries={
            EURJPY_KEY: Entry("PRACTICE", "BANNED_REAL", rule="S1"),
            "runner|*|*|*": Entry("SKIP", "PAUSED", rule="B2")}), now=sim.now)
    got = run(tmp_path, "valid", prepare=valid)
    assert got["orders"] != head["orders"]


def test_a_lapsed_lease_keeps_its_restrictions_only(tmp_path):
    """Spec 10.3 step 6 and 10.4: when the coach dies its grants lapse and its restrictions stay - never
    more real money than Aaron's own settings, sometimes less."""
    def stale(cfg, sim):
        pol = full_policy(cfg, sim, now=sim.now - dt.timedelta(minutes=10))
        put_raw(cfg, sealed_obj(pol))
    sim, cfg, pb, tr = build(tmp_path, prepare=stale)
    act_on(tr, sim, entry(sim, "EURJPY", 0.300, tactic="TREND_PULLBACK"))      # real on Aaron's settings
    act_on(tr, sim, entry(sim, "GER40", 25.0, tactic=MC))                      # the lapsed grant: paper
    assert not [o for o in sim.order_log if o["req"].magic == TNB]               # the restriction: practice
    assert all(pb.is_paper(p.ticket) for p in pb.positions(TNB))
    tr.journal.close()
    pb.close_db()


# ================================================================ Formula 1 --
@pytest.mark.parametrize("mode", ["OFF", "WATCH", "GUARD", "PILOT"])
def test_formula1_orders_are_identical_in_every_mode_before_its_judgement(tmp_path, mode):
    def f1_orders(where, prepare, strip=False):
        sim, cfg, pb, tr = build(where, live_tactics=(MC,), prepare=prepare, strip=strip)
        act_on(tr, sim, entry(sim, "EURGBP", 0.0030, tactic=MC))
        move(sim, "EURGBP", 0.0020)
        sim.advance(1)
        tr.cycle()
        out = ([dataclasses.asdict(o["req"]) for o in sim.order_log if o["req"].symbol == "EURGBP"],
               list(sim.modify_log), [r["risk_money"] for r in tr.journal.open_trades()])
        tr.journal.close()
        pb.close_db()
        return out

    def coach(cfg, sim):
        sha = put_envelope(cfg, mode=mode)
        restrict_all = {"tnb|*|*|*": Entry("PRACTICE", "PAUSED", rule="B2"),
                        "tnb|*|FX_MINOR|*": Entry("SKIP", "PAUSED", rule="D2")}
        put_raw(cfg, sealed_obj(full_policy(cfg, sim, sha=sha, entries=restrict_all,
                                            blocks=(Block(("tnb",), ("EURGBP",), "SKIP", "S2"),))))
    base = f1_orders(tmp_path / "base", None, strip=True)
    got = f1_orders(tmp_path / mode, coach)
    assert base[0] and got == base
    assert base[0][0]["coach_route"] == ""


# ================================================================ routes --
def test_a_demoted_key_goes_to_practice(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    act_on(tr, sim, entry(sim, "EURJPY", 0.300, tactic="TREND_PULLBACK"))
    assert not sim.order_log
    (p,) = pb.positions(TNB)
    assert pb.is_paper(p.ticket)
    ev = tr.journal._exec("SELECT detail_json FROM events WHERE kind='ENTRY'").fetchall()
    assert any("The coach: practice" in r[0] for r in ev)
    tr.journal.close()
    pb.close_db()


def test_a_granted_index_key_goes_real_and_is_managed_after_a_restart(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={US500_KEY: grant()}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, manage_leftovers=False, prepare=coach)
    state = entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION")
    act_on(tr, sim, state)
    (order,) = [o for o in sim.order_log if o["req"].magic == TNB]
    assert order["req"].coach_route == "REAL" and order["req"].symbol == "US500"
    spec = sim.spec("US500")
    risk = spec.money_per_lot(abs(state.entry - state.stop)) * order["req"].volume
    assert 0 < risk <= 6.53 + 1e-9
    ticket = sim.positions(TNB)[0].ticket
    assert pb.is_coach_real(ticket) and not pb.is_paper(ticket)
    row = json.loads((Path(cfg.ops.data_dir) / "coach" / "gate" / f"tnb-{sim.now.date()}.jsonl")
                     .read_text().splitlines()[-1])
    assert row["event"] == "fill" and row["ticket"] == ticket and row["key"] == US500_KEY
    tr.journal.close()
    pb.close_db()
    # a restart: a new wrapper on the same file still knows the ticket is the coach's (C3)
    pb2 = paper_on(sim, cfg)
    assert pb2.is_coach_real(ticket)
    pos = sim.positions(TNB)[0]
    new_stop = spec.normalise_price(pos.sl + 2.0)
    assert pb2.modify_stops(ticket, new_stop, pos.tp).ok
    assert sim.positions(TNB)[0].sl == pytest.approx(new_stop)
    assert pb2.close(ticket).ok and not sim.positions(TNB)
    # without the coach's record the same real position would be refused (manage_leftovers off)
    pb2.close_db()


def test_a_coach_real_order_the_envelope_does_not_confirm_stays_on_paper(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={US500_KEY: grant()}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    pb.coach_confirm = lambda req, kind=None: False
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    assert not sim.order_log and all(pb.is_paper(p.ticket) for p in pb.positions(TNB))
    tr.journal.close()
    pb.close_db()


# ================================================================ the Runner --
def test_runner_copy_volume_zero_means_no_ride(tmp_path):
    def skip(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={"runner|*|*|*": Entry("SKIP", "PAUSED", rule="B2")}),
                   now=sim.now)
    for prepare, rides in ((None, True), (skip, False)):
        sim, cfg, pb, tr = build(tmp_path / str(rides), runner="LIVE", prepare=prepare)
        act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
        (row,) = tr.journal.open_trades()
        sim.advance(1)
        tr.cycle()
        ridden = [o for o in sim.order_log if o["req"].magic == RUNNER]
        if rides:
            assert ridden and row["runner_volume"] != 0.0
        else:
            assert row["runner_volume"] == 0.0 and not ridden
        tr.journal.close()
        pb.close_db()


def test_a_lower_money_cap_shrinks_the_runners_copy(tmp_path):
    def cap(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={"runner|*|*|*": Entry("BASELINE", "PLACED",
                                                                             money_gbp=3.0)}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, runner="LIVE", prepare=cap)
    state = entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION")
    act_on(tr, sim, state)
    (row,) = tr.journal.open_trades()
    per_lot = sim.spec("US500").money_per_lot(10.0)
    assert row["runner_volume"] * per_lot <= 3.0 + 1e-9
    tr.journal.close()
    pb.close_db()


# ================================================================ the other bots --
def test_band_breaker_and_crowd_fader_on_paper_are_never_gated(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim), now=sim.now)
        (Path(cfg.ops.data_dir) / "HALT").write_text("YES")
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    acct = tr.broker.account()
    tr._last_snapshot = tr.risk.snapshot(acct, tr.broker.positions(TNB), sim.now)
    assert tr._bots_may_enter() is True
    assert tr.bandbreaker.entries_allowed() is True and tr.crowd.entries_allowed() is True
    # the same switches with them LIVE: no new real entries
    cfg.bandbreaker.mode = cfg.crowd.mode = "LIVE"
    assert tr.bandbreaker.entries_allowed() is False and tr.crowd.entries_allowed() is False
    tr.journal.close()
    pb.close_db()


# ================================================================ Financial Ian --
class _LiveExec:
    mode = "LIVE"


def ian(tmp_path, gate, mode="LIVE"):
    from mintel.ian.engine import IanConfig, IanEngine
    from mintel.ian.feeds.base import NotConfiguredFeed
    ex = _LiveExec()
    ex.mode = mode
    return IanEngine(tmp_path / "ian", IanConfig(mode=mode, record=False), broker=None, feed=NotConfiguredFeed(),
                     executor=ex, coach_gate=gate)


def test_ian_entry_block_honours_the_gate_and_keeps_its_own_limit(tmp_path):
    from mintel.config import Config
    now = dt.datetime(2026, 10, 13, 10, 0, tzinfo=dt.timezone.utc)
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    Path(cfg.ops.data_dir).mkdir(parents=True)
    sha = put_envelope(cfg)
    g = G.CoachGate("ian", cfg.ops.data_dir, cfg, clock=lambda: now)
    e = ian(tmp_path, g)
    assert e._entry_block(now) == ""                                     # no policy yet: its own settings
    put_policy(cfg, make_policy(sha, now=now, entries={"ian|*|*|*": Entry("SKIP", "PAUSED", rule="B2")}), now=now)
    why = e._entry_block(now)
    assert why.startswith("The coach: no new entry")
    put_policy(cfg, make_policy(sha, seq=2, now=now,
                                entries={"ian|*|*|*": Entry("BASELINE", "PLACED", money_gbp=4.0)}), now=now)
    assert e._entry_block(now) == "" and e._coach_cap == 4.0
    seen = {}

    def fake_submit(sig, tick, when, hist, spec):
        seen["risk"], seen["max"] = e.bridge.cfg.risk_money, e.bridge.cfg.max_risk_money
        return "report"
    e.bridge.submit = fake_submit
    assert e._submit(None, None, now, [], None) == "report"
    assert seen == {"risk": 4.0, "max": 4.0} and e.bridge.cfg.risk_money == 10.0     # for that one order
    # B5: no headroom left for its £10
    put_policy(cfg, make_policy(sha, seq=3, now=now, entries={}, headroom_gbp=6.0), now=now)
    assert "headroom" in e._entry_block(now)
    # its own £50 limit is checked first and is unchanged
    e.journal.closed_since = lambda day0, mode: [{"net_pnl": -50.0, "data_source": e.data_source}]
    assert "its own limit 50" in e._entry_block(now)
    # PAPER is never gated
    (Path(cfg.ops.data_dir) / "HALT").write_text("YES")
    paper = ian(tmp_path / "p", g, mode="PAPER")
    assert paper._entry_block(now) == ""
    assert ian(tmp_path / "l", g)._entry_block(now).startswith("HALT")
    # no gate at all: exactly as before
    assert ian(tmp_path / "n", None)._entry_block(now) == ""
    e.close()


def test_ian_with_the_coach_broken_is_exactly_as_before(tmp_path):
    from mintel.config import Config
    now = dt.datetime(2026, 10, 13, 10, 0, tzinfo=dt.timezone.utc)
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    Path(cfg.ops.data_dir).mkdir(parents=True)
    sha = put_envelope(cfg)
    pol = sealed_obj(make_policy(sha, now=now, entries={"ian|*|*|*": Entry("SKIP", "PAUSED", rule="B2")}))
    pol["headroom_gbp"] = 0.0                                          # forged after sealing
    put_raw(cfg, pol, seal=False)
    e = ian(tmp_path, G.CoachGate("ian", cfg.ops.data_dir, cfg, clock=lambda: now))
    assert e._entry_block(now) == "" and e._coach_cap is None
    e.close()


# ================================================================ weather.json --
def test_weather_json_is_written_atomically(tmp_path, monkeypatch):
    sim, cfg, pb, tr = build(tmp_path)
    replaced = []
    real_replace = os.replace

    def spy(src, dst):
        replaced.append((str(src), str(dst)))
        return real_replace(src, dst)
    monkeypatch.setattr(os, "replace", spy)
    tr.cycle()
    path = Path(cfg.ops.data_dir) / "coach" / "weather.json"
    assert (str(path.with_name("weather.json.tmp")), str(path)) in replaced
    doc = json.loads(path.read_text())
    assert doc["account_demo"] is True and doc["account_is_demo"] is True and "Demo" in doc["account_server"]
    assert doc["config_mode"] == "DEMO" and doc["account_login"] == sim.login
    assert {r["symbol"] for r in doc["symbols"]} == {"EURGBP", "EURJPY", "US500", "GER40", "EURUSD"}
    assert all(set(r) == {"symbol", "kind", "regime", "confidence", "vol_state", "spread_points", "news_state",
                          "tradable", "as_of_utc"} for r in doc["symbols"])
    assert not path.with_name("weather.json.tmp").exists()
    first = path.read_text()
    tr.cycle()                                                         # within 60 s: not rewritten
    assert path.read_text() == first
    tr.journal.close()
    pb.close_db()


def test_a_dry_run_has_no_gate_and_writes_nothing_for_the_coach(tmp_path):
    from mintel.engine.trader import Trader
    sim, cfg = setup(tmp_path)
    tr = Trader(sim, cfg, clock=lambda: sim.now, enable_model=False, dry_run=True)
    assert tr.coach_gate is None and tr.risk.limits is None
    tr.journal.close()
