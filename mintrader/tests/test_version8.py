"""Version 8 (17 Oct, Aaron: "Keep the approaches off and continue with how we
should be if they weren't there"): the market rule counts only the trades
today's rules still allow, as the approach rules do. On that count none of
version 7's five markets meets it, so they are back on. Every version 6 rule
is kept, and the ten markets of 4 and 8 Oct stay off. The coach publishes the
combined count the nightly review judges the rule on. Made-up trades only."""
import dataclasses
import datetime as dt
from types import SimpleNamespace as NS

from mintel.coach import decide as D
from mintel.coach import records as R
from mintel.coach import rules as RU
from mintel.config import Config, STRATEGY_NAME, STRATEGY_VERSION
from mintel.engine.tactics import allowed_by_rules

UTC = dt.timezone.utc
V6_EXCLUDED = ("USDJPY", "DE40", "XAUGBP", "CHFJPY", "EURNZD", "UK100", "GBPJPY", "XAUAUD", "USDCHF", "XAUJPY")
BACK_ON = ("CADJPY", "EURCAD", "EURUSD", "GBPAUD", "GBPCAD")
V6_GROUPS = (("MOMENTUM_CONTINUATION", "GOLD"), ("MOMENTUM_CONTINUATION", "SILVER"),
             ("MOMENTUM_CONTINUATION", "METAL"), ("MOMENTUM_CONTINUATION", "FX_MAJOR"),
             ("MOMENTUM_CONTINUATION", "INDEX"))


def test_version_8_name_and_the_clock_is_unchanged():
    assert STRATEGY_NAME == "Commission First (version 8)"
    assert STRATEGY_VERSION.startswith("2026-10-05 complete reset to zero")


def test_the_five_are_back_on_the_earlier_markets_stay_off_and_every_approach_rule_is_version_6s():
    c = Config()
    assert tuple(c.universe.excluded_symbols) == V6_EXCLUDED
    assert not set(BACK_ON) & set(c.universe.excluded_symbols)
    assert tuple(c.scan.disabled_tactic_groups) == V6_GROUPS


def test_the_scanner_takes_the_five_again():
    from mintel.broker.sim import SimBroker
    from mintel.engine.scanner import discover
    cfg = Config()
    cfg.universe.max_symbols = 60
    broker = SimBroker(BACK_ON + ("USDJPY", "USDCHF"), start=dt.datetime(2026, 10, 7, 9, tzinfo=UTC))
    broker.connect()
    uni = discover(broker, cfg)
    for sym in BACK_ON:
        assert sym in uni.symbols, sym
    for sym in ("USDJPY", "USDCHF"):
        assert sym not in uni.symbols and "evidence" in uni.rejected.get(sym, ""), sym


def test_allowed_by_rules_is_todays_rules_without_the_market_list():
    c = Config()
    ok = lambda *a: allowed_by_rules(c, *a)
    assert ok("MOMENTUM_CONTINUATION", "HIGH_VOL", "FX_MINOR", 70.0)
    assert not ok("MOMENTUM_CONTINUATION", "HIGH_VOL", "FX_MINOR", 69.9)       # the STRONG floor (and its own 70)
    assert not ok("MOMENTUM_CONTINUATION", "HIGH_VOL", "INDEX", 80.0)          # off on the indices (version 6)
    assert not ok("MOMENTUM_CONTINUATION", "HIGH_VOL", "FX_MAJOR", 80.0)       # off on the majors (version 5)
    assert not ok("MOMENTUM_CONTINUATION", "TREND", "FX_MINOR", 80.0)          # off in a trend
    assert ok("SESSION_EXPANSION", "TREND", "FX_MINOR", 80.0)                  # still on on the minors
    assert not ok("TREND_PULLBACK", "TREND", "INDEX", 80.0)                    # off everywhere
    assert not ok("BREAKOUT_RETEST", "TREND", "INDEX", 80.0)
    assert not ok("SESSION_EXPANSION_2X", "TREND", "FX_MAJOR", 80.0)           # the twin is off
    assert ok("SESSION_EXPANSION", "TREND", "FX_MAJOR", 72.0)
    assert not ok("SESSION_EXPANSION", "TREND", "FX_MAJOR", 65.0)
    assert ok("SESSION_EXPANSION", "TREND", "FX_MAJOR", None)                  # no score on record: not excluded


def test_allowed_by_rules_mirrors_the_scanner_on_signals_and_twins():
    c = Config()
    # BREAKOUT_RETEST is a signal of BREAKOUT_ACCEPTANCE: best_signal refuses it wherever its tactic is off
    c.scan.disabled_tactic_regimes = (("BREAKOUT_ACCEPTANCE", "UNCLEAR"),)
    assert not allowed_by_rules(c, "BREAKOUT_RETEST", "UNCLEAR", "FX_MINOR", 80.0)
    assert allowed_by_rules(c, "BREAKOUT_RETEST", "SQUEEZE", "FX_MINOR", 80.0)
    # a twin only while twins are on, and only for a twin approach
    c.scan.twin_enabled, c.scan.twin_tactics = True, ("SESSION_EXPANSION",)
    assert allowed_by_rules(c, "SESSION_EXPANSION_2X", "TREND", "FX_MAJOR", 80.0)
    assert not allowed_by_rules(c, "MOMENTUM_CONTINUATION_2X", "HIGH_VOL", "FX_MINOR", 80.0)


def _t(ticket, day, net, approach, condition, kind="FX_MAJOR", symbol="EURUSD", score=75.0, flags=()):
    t = dt.datetime(2026, 9, day, 10, tzinfo=UTC)
    return R.Trade("journal", "tnb", ticket, approach, kind, condition, symbol, "BUY", t, t + dt.timedelta(minutes=30),
                   True, net, 0.6, 10.0, score, flags=tuple(flags))


def _eurusd():
    """Made up: 6 allowed days (session expansion in a trend, score 75+): 3 up, 3 down - not met on its own.
    Three more losing days only from approaches or scores today's rules refuse."""
    allowed = [_t(1, 14, 6.0, "SESSION_EXPANSION", "TREND"), _t(2, 15, -4.0, "SESSION_EXPANSION", "TREND"),
               _t(3, 16, 5.0, "SESSION_EXPANSION", "TREND"), _t(4, 17, -4.0, "SESSION_EXPANSION", "TREND"),
               _t(5, 18, 7.0, "SESSION_EXPANSION", "TREND"), _t(6, 21, -3.0, "SESSION_EXPANSION", "TREND")]
    refused = [_t(10, 22, -5.0, "MOMENTUM_CONTINUATION", "HIGH_VOL"),          # off on the majors
               _t(11, 23, -5.0, "SESSION_EXPANSION", "TREND", score=64.0),      # refused only by its score
               _t(12, 24, -5.0, "SESSION_EXPANSION", "TREND", flags=(R.F_TWIN,))]   # a twin: off
    return allowed, refused


END = dt.datetime(2026, 9, 25, 23, tzinfo=UTC)


def test_the_market_rule_no_longer_counts_approaches_already_off():
    allowed, refused = _eurusd()
    every = {r.target: r for r in RU.s2(allowed + refused, END, bots=["tnb"])}
    assert every["EURUSD"].fires and (every["EURUSD"].days, every["EURUSD"].negative_days) == (9, 6)
    keep = D.todays_rules(NS(cfg=Config()))
    today = {r.target: r for r in RU.s2(allowed + refused, END, bots=["tnb"], allowed=keep)}
    assert today["EURUSD"].source == "real" and (today["EURUSD"].days, today["EURUSD"].negative_days) == (6, 3)
    assert not today["EURUSD"].fires
    # each refused trade is refused for its own reason (approach off, score, twin); the allowed six are kept
    assert [keep(t) for t in refused] == [False, False, False]
    assert all(keep(t) for t in allowed)
    assert keep(dataclasses.replace(refused[2], flags=())) is True             # the same trade, not a twin


def test_the_coach_s2_list_uses_todays_rules():
    allowed, refused = _eurusd()
    w = NS(trades=allowed + refused, now=END, haircuts=None, cfg=Config())
    assert D.s2_blocks(w) == []
    assert [r.target for r in D.s2_blocks(NS(trades=allowed + refused, now=END, haircuts=None, cfg=None))] == ["EURUSD"]


def test_a_twin_from_the_journal_is_flagged_and_refused(tmp_path):
    from tests.test_coach_records import NOW, REAL_TICKET, journal_row, write_journal, write_paper_book
    data = tmp_path / "data"
    data.mkdir()
    write_journal(data, [journal_row(REAL_TICKET, symbol="EURUSD", tactic="SESSION_EXPANSION_2X", regime="TREND",
                                     opportunity=80.0),
                         journal_row(REAL_TICKET + 1, symbol="EURUSD", tactic="SESSION_EXPANSION", regime="TREND",
                                     opportunity=80.0)])
    write_paper_book(data, [])
    ev = R.read_evidence(data, now=NOW, sources=(R.JOURNAL,))
    by = {t.ticket: t for t in ev.trades}
    twin, plain = by[REAL_TICKET], by[REAL_TICKET + 1]
    assert twin.approach == "SESSION_EXPANSION" and R.F_TWIN in twin.flags and R.F_TWIN not in plain.flags
    keep = D.todays_rules(NS(cfg=Config()))
    assert keep(twin) is False and keep(plain) is True
    assert D.todays_rules(NS(cfg=None)) is None                               # no config: every trade counts


def test_the_coach_publishes_the_combined_count_the_review_judges():
    # made up: EURCAD on 2 DEMO days (both down) and 4 PAPER days (2 down) of allowed trades: neither side has
    # 6 days, so S2 cannot fire; added together it is 6 days, 4 down - the rule is met on the combined count
    def eurcad(ticket, day, net, real, approach="SESSION_EXPANSION", score=75.0):
        t = dt.datetime(2026, 10, day, 10, tzinfo=UTC)
        return R.Trade("journal", "tnb", ticket, approach, "FX_MINOR", "TREND", "EURCAD", "BUY", t,
                       t + dt.timedelta(minutes=30), real, net, 0.6, 10.0, score)
    trades = [eurcad(1, 5, -4.0, True), eurcad(2, 7, -6.0, True),
              eurcad(3, 12, -3.0, False), eurcad(4, 13, 5.0, False), eurcad(5, 14, -2.0, False),
              eurcad(6, 15, 4.0, False),
              eurcad(7, 16, -9.0, False, approach="BREAKOUT_RETEST")]          # refused: not counted
    end = dt.datetime(2026, 10, 16, 23, tzinfo=UTC)
    w = NS(trades=trades, now=end, haircuts=None, cfg=Config())
    assert D.s2_blocks(w) == []                                               # the coach's own rule: never mixed
    rows = {r["symbol"]: r for r in D.market_watch(w)}
    r = rows["EURCAD"]
    assert (r["days"], r["negative_days"], r["real_days"], r["practice_days"], r["trades"]) == (6, 4, 2, 4, 6)
    assert r["meets"] is True and r["combined_net_gbp"] == -6.0
    assert r["real_net_gbp"] == -10.0 and r["practice_net_gbp"] == 4.0              # DEMO and PAPER apart
    assert D.market_watch(NS(trades=None, now=end, haircuts=None, cfg=None)) == []    # never raises


def test_the_combined_count_lists_the_rows_that_meet_the_rule_first_then_the_most_negative():
    def tr(ticket, sym, day, net, real=False):
        t = dt.datetime(2026, 10, day, 10, tzinfo=UTC)
        return R.Trade("journal", "tnb", ticket, "SESSION_EXPANSION", "FX_MINOR", "TREND", sym, "BUY", t,
                       t + dt.timedelta(minutes=30), real, net, 0.6, 10.0, 75.0)
    trades = ([tr(i, "AUDJPY", 5 + i, -1.0 if i < 4 else 2.0) for i in range(6)]          # 6 days, 4 down: meets
              + [tr(20 + i, "GBPNZD", 5 + i, -1.0 if i < 2 else 1.0) for i in range(3)]   # 3 days, 2 down
              + [tr(40 + i, "EURAUD", 5 + i, 1.0) for i in range(4)]                       # 4 days, none down
              + [tr(60 + i, "EURCAD", 5 + i, -1.0 if i < 3 else 1.0) for i in range(4)])  # 4 days, 3 down
    rows = RU.market_counts(trades, dt.datetime(2026, 10, 20, tzinfo=UTC), bots=["tnb"])
    assert [r["symbol"] for r in rows] == ["AUDJPY", "EURCAD", "GBPNZD", "EURAUD"]
    assert [r["meets"] for r in rows] == [True, False, False, False]
    assert len(RU.market_counts(trades, dt.datetime(2026, 10, 20, tzinfo=UTC), bots=["tnb"], limit=2)) == 2


def test_the_replay_loader_marks_twins_as_the_journal_does():
    from mintel.coach.run import load_reports_csv
    from tests.test_coach_replay import FIXTURE
    import csv
    with open(FIXTURE) as fh:
        raw = {int(r["ticket"]): r["tactic"] for r in csv.DictReader(l for l in fh if not l.startswith("#"))}
    trades = load_reports_csv(FIXTURE)
    twins = [t for t in trades if raw[t.ticket].strip().upper().endswith("_2X")]
    assert twins and all(R.F_TWIN in t.flags for t in twins)
    assert all(R.F_TWIN not in t.flags for t in trades if t not in twins)


def test_allowed_by_rules_agrees_with_best_signal_on_every_approach_condition_and_kind_of_market(monkeypatch):
    # the filter must take exactly what the scanner's best_signal takes, today's lists unchanged
    from mintel.data.regime import Regime
    from mintel.engine import tactics as T
    c = Config()
    kw = dict(disabled=c.scan.disabled_tactics, disabled_in=c.scan.disabled_tactic_regimes,
              disabled_in_group=c.scan.disabled_tactic_groups)
    pairs = [(tac.name, tac.name) for tac in T.TACTICS] + [(p, s) for s, p in T.SIGNAL_PARENT.items()]
    kinds = ("FX_MAJOR", "FX_MINOR", "INDEX", "GOLD", "SILVER", "METAL", "ENERGY", "")
    seen = 0
    for tactic, signal in pairs:
        sig = T.TacticSignal(name=signal, direction=1, quality=80.0, invalidation=1.0, entry_hint=1.1)
        for regime in Regime:
            tac = T.Tactic(tactic, (regime,), lambda ctx, side, s=sig: s)
            monkeypatch.setattr(T, "candidates_for", lambda r, t=tac: (t,))
            for kind in kinds:
                scanner = T.best_signal(None, 1, regime, group=kind, **kw) is not None
                assert allowed_by_rules(c, signal, regime.value, kind, None) is scanner, (tactic, signal, regime, kind)
                seen += 1
    assert seen >= 8 * len(Regime) * len(kinds)


def test_an_approachs_own_floor_above_the_tier_and_names_in_any_case():
    c = Config()
    c.scan.tactic_min_score = {"SESSION_EXPANSION": 80.0}                    # above the STRONG floor (70)
    assert not allowed_by_rules(c, "SESSION_EXPANSION", "TREND", "FX_MINOR", 75.0)
    assert allowed_by_rules(c, "SESSION_EXPANSION", "TREND", "FX_MINOR", 80.0)
    assert allowed_by_rules(c, "MOMENTUM_CONTINUATION", "HIGH_VOL", "FX_MINOR", 75.0)   # only its own approach
    c = Config()
    assert not allowed_by_rules(c, "trend_pullback", "trend", "fx_minor", 80.0)     # off everywhere, any case
    assert not allowed_by_rules(c, "session_expansion_2x", "TREND", "FX_MINOR", 80.0)   # a twin, twins off
    assert allowed_by_rules(c, "session_expansion", "trend", "fx_minor", 80.0)


# --------------------------------------------------- today's news window (17 Oct) --
def _calendar(data, events):
    from mintel.broker.base import CalendarEvent
    from mintel.news.calendar_store import CalendarStore
    store = CalendarStore(data / "calendar.sqlite")
    store.upsert([CalendarEvent(f"e{k}", when, ccy, "", name, imp, source="public")
                  for k, (when, ccy, name, imp) in enumerate(events)])
    store.close()


def _mac_config(window_s=900.0):
    c = Config()
    c.news.blackout_before_seconds = window_s                                   # the Mac's 15 minutes (10 Oct)
    return c


def _news_trades():
    def tr(ticket, when, symbol, net, real=False):
        return R.Trade("journal", "tnb", ticket, "SESSION_EXPANSION", "FX_MINOR", "TREND", symbol, "SELL", when,
                       when + dt.timedelta(minutes=40), real, net, 0.6, 13.0, 80.0)
    return [tr(1, dt.datetime(2026, 10, 9, 12, 21, tzinfo=UTC), "GBPCAD", -32.47),      # 9 min before CAD data
            tr(2, dt.datetime(2026, 10, 9, 12, 10, tzinfo=UTC), "GBPCAD", 5.0),         # 20 min before: outside
            tr(3, dt.datetime(2026, 10, 9, 7, 52, tzinfo=UTC), "EURAUD", 4.0),          # before a medium release
            tr(4, dt.datetime(2026, 10, 9, 12, 25, tzinfo=UTC), "EURAUD", 3.0),         # CAD is not its currency
            tr(5, dt.datetime(2026, 9, 20, 22, 5, tzinfo=UTC), "GBPCAD", -2.0)]         # a day not in the archive


NEWS_EVENTS = [(dt.datetime(2026, 10, 9, 12, 30, tzinfo=UTC), "CAD", "Employment Change", 3),
               (dt.datetime(2026, 10, 9, 8, 0, tzinfo=UTC), "EUR", "Final Manufacturing PMI", 2),
               (dt.datetime(2026, 10, 10, 9, 0, tzinfo=UTC), "GBP", "Bank Holiday", 0)]


def test_a_trade_entered_inside_todays_news_window_does_not_count(tmp_path):
    _calendar(tmp_path, NEWS_EVENTS)
    trades = _news_trades()
    end = dt.datetime(2026, 10, 10, 23, tzinfo=UTC)
    w = NS(trades=trades, now=end, haircuts=None, cfg=_mac_config(), data_dir=str(tmp_path))
    keep = D.todays_rules(w)
    assert [keep(t) for t in trades] == [False, True, True, True, True]
    assert D.todays_rules(w) is keep                                            # built once for each World
    nw = D.news_watch(w)
    assert nw["window_minutes"] == 15.0 and nw["calendar"] is True and nw["left_out_count"] == 1
    assert nw["left_out"][0]["ticket"] == 1 and nw["left_out"][0]["before"] == "CAD Employment Change at 12:30 UTC"
    assert nw["unchecked"] == 1                                                 # 20 Sep: counted, not checked
    rows = {r["symbol"]: r for r in D.market_watch(w)}
    assert rows["GBPCAD"]["trades"] == 2 and rows["GBPCAD"]["combined_net_gbp"] == 3.0   # the news trade left out
    # the same record on the code's own 90 seconds: nothing is inside the window
    w90 = NS(trades=trades, now=end, haircuts=None, cfg=_mac_config(90.0), data_dir=str(tmp_path))
    assert all(D.todays_rules(w90)(t) for t in trades) and D.news_watch(w90)["left_out"] == []


def test_without_a_calendar_or_with_news_off_every_trade_counts_and_nothing_is_written(tmp_path):
    trades = _news_trades()
    end = dt.datetime(2026, 10, 10, 23, tzinfo=UTC)
    w = NS(trades=trades, now=end, haircuts=None, cfg=_mac_config(), data_dir=str(tmp_path))
    assert all(D.todays_rules(w)(t) for t in trades)
    assert not (tmp_path / "calendar.sqlite").exists()                          # read only: never created
    assert D.news_watch(w)["calendar"] is False
    _calendar(tmp_path, NEWS_EVENTS)
    off = _mac_config()
    off.news.enabled = False
    assert all(D.todays_rules(NS(trades=trades, now=end, cfg=off, data_dir=str(tmp_path)))(t) for t in trades)
    (tmp_path / "broken").mkdir()
    (tmp_path / "broken" / "calendar.sqlite").write_text("not a database")
    assert all(D.todays_rules(NS(trades=trades, now=end, cfg=_mac_config(),
                                 data_dir=str(tmp_path / "broken")))(t) for t in trades)
    assert D.news_watch(NS(trades=None, now=end, cfg=_mac_config(), data_dir=str(tmp_path)))["left_out"] == []


def test_the_news_window_reads_the_archive_without_changing_it_or_making_files(tmp_path):
    import hashlib
    from mintel.broker.base import CalendarEvent
    from mintel.coach import newswin as NW
    from mintel.news.calendar_store import CalendarStore
    _calendar(tmp_path, NEWS_EVENTS)
    span = (dt.datetime(2026, 10, 1, tzinfo=UTC), dt.datetime(2026, 10, 10, tzinfo=UTC))
    at = dt.datetime(2026, 10, 9, 12, 21, tzinfo=UTC)
    def files():                     # every file by name; contents too, but SQLite's own -shm index is its business
        return {p.name: None if p.name.endswith("-shm") else hashlib.sha256(p.read_bytes()).hexdigest()
                for p in tmp_path.iterdir()}
    before = files()
    win = NW.load(tmp_path, _mac_config(), *span)                              # the trader not running
    assert win is not None and win.inside("GBPCAD", at) is True
    assert files() == before                                                    # nothing changed, nothing made
    # the trader running: its newest release is still in its -wal file, and is read with it
    store = CalendarStore(tmp_path / "calendar.sqlite")
    store.upsert([CalendarEvent("late", dt.datetime(2026, 10, 9, 7, 55, tzinfo=UTC), "AUD", "", "Made-up Rate", 3,
                                source="public")])
    try:
        win = NW.load(tmp_path, _mac_config(), *span)
        assert win.inside("EURAUD", dt.datetime(2026, 10, 9, 7, 52, tzinfo=UTC)) is True
    finally:
        store.close()


def test_which_releases_close_the_window_for_a_market():
    from mintel.coach.newswin import currencies_of
    assert currencies_of("GBPCAD") == ("GBP", "CAD") and currencies_of("EURAUD.pro") == ("EUR", "AUD")
    assert currencies_of("XAUUSD") == ("XAU", "USD") and currencies_of("XAGUSD") == ("XAG", "USD")
    assert currencies_of("JP225") == ("JPY",) and currencies_of("DE40") == ("EUR",)
    assert currencies_of("US500") == ("USD",) and currencies_of("SOMETHING") == ("USD",)


def test_the_window_edges_after_a_release_and_trades_from_10_oct(tmp_path):
    from mintel.coach import newswin as NW
    release = dt.datetime(2026, 10, 9, 12, 30, tzinfo=UTC)
    _calendar(tmp_path, NEWS_EVENTS)
    win = NW.load(tmp_path, _mac_config(), dt.datetime(2026, 10, 1, tzinfo=UTC), dt.datetime(2026, 10, 10, tzinfo=UTC))
    at = lambda s: win.inside("GBPCAD", release + dt.timedelta(seconds=s))
    assert at(-900) is True and at(-901) is False                               # 0 <= release - entry <= 900, ...
    assert at(0) is True and at(1) is False and at(60) is False                 # ... never after the release
    # a trade from 10 Oct was taken with the window in force: the scanner let it through, so it is not judged again
    from mintel.coach.epochs import EPOCHS
    assert NW.WINDOW_SINCE == next(e.when_utc for e in EPOCHS if e.name.startswith("news block"))
    later = dt.datetime(2026, 10, 12, 12, 21, tzinfo=UTC)
    (tmp_path / "x").mkdir()
    _calendar(tmp_path / "x", [(dt.datetime(2026, 10, 12, 12, 30, tzinfo=UTC), "CAD", "Made-up Release", 3)])
    t = R.Trade("journal", "tnb", 9, "SESSION_EXPANSION", "FX_MINOR", "TREND", "GBPCAD", "SELL", later,
                later + dt.timedelta(minutes=30), False, -5.0, 0.6, 13.0, 80.0)
    w = NS(trades=[t], now=later + dt.timedelta(days=1), cfg=_mac_config(), data_dir=str(tmp_path / "x"))
    assert D.todays_rules(w).news.inside("GBPCAD", later) is True and D.todays_rules(w)(t) is True


def test_a_window_across_midnight_and_an_archive_in_an_odd_folder(tmp_path):
    from mintel.coach import newswin as NW
    # Sunday 23:55 entry; no event on Sunday (not covered), a high release at 00:05 on Monday: known, so inside
    _calendar(tmp_path, [(dt.datetime(2026, 10, 5, 0, 5, tzinfo=UTC), "AUD", "Made-up Rate", 3)])
    win = NW.load(tmp_path, _mac_config(), dt.datetime(2026, 10, 1, tzinfo=UTC), dt.datetime(2026, 10, 9, tzinfo=UTC))
    assert win.inside("EURAUD", dt.datetime(2026, 10, 4, 23, 55, tzinfo=UTC)) is True
    assert win.inside("EURUSD", dt.datetime(2026, 10, 4, 23, 55, tzinfo=UTC)) is None    # nothing known: counts
    for name in ("a#b", "a?b", "a%20b"):
        (tmp_path / name).mkdir()
        _calendar(tmp_path / name, [(dt.datetime(2026, 10, 9, 12, 30, tzinfo=UTC), "CAD", f"In {name}", 3)])
    (tmp_path / "a b").mkdir()
    _calendar(tmp_path / "a b", [(dt.datetime(2026, 10, 9, 12, 30, tzinfo=UTC), "CAD", "In the sibling", 3)])
    for name in ("a#b", "a?b", "a%20b"):
        w = NW.load(tmp_path / name, _mac_config(), dt.datetime(2026, 10, 1, tzinfo=UTC),
                    dt.datetime(2026, 10, 10, tzinfo=UTC))
        assert w.release("GBPCAD", dt.datetime(2026, 10, 9, 12, 21, tzinfo=UTC)) == f"CAD In {name} at 12:30 UTC"


def test_the_pinned_rules_use_todays_rules_too():
    allowed, refused = _eurusd()
    key = "tnb|SESSION_EXPANSION|FX_MAJOR|TREND"
    w = NS(trades=allowed + refused, now=END, haircuts=None, cfg=Config(),
           key_trades=lambda k: [t for t in allowed + refused if t.approach == "SESSION_EXPANSION"])
    assert not [x for x in D.pinned_rules(w, key, s1_results=[]) if "market rule" in x]
    w.cfg = None                                                                # every trade: the rule fires
    w.__dict__.pop("_todays_rules", None)
    assert [x for x in D.pinned_rules(w, key, s1_results=[]) if "market rule" in x]


def test_random_switch_off_lists_agree_with_best_signal(monkeypatch):
    import random
    from mintel.data.regime import Regime
    from mintel.engine import tactics as T
    names = sorted({tac.name for tac in T.TACTICS} | set(T.SIGNAL_PARENT))
    kinds = ("FX_MAJOR", "FX_MINOR", "INDEX", "GOLD", "SILVER", "METAL")
    pairs = [(tac.name, tac.name) for tac in T.TACTICS] + [(p, s) for s, p in T.SIGNAL_PARENT.items()]
    rnd = random.Random(17)
    for _ in range(60):
        c = Config()
        c.scan.disabled_tactics = tuple(rnd.sample(names, rnd.randint(0, 2)))
        c.scan.disabled_tactic_regimes = tuple((rnd.choice(names), rnd.choice(list(Regime)).value)
                                               for _ in range(rnd.randint(0, 4)))
        c.scan.disabled_tactic_groups = tuple((rnd.choice(names), rnd.choice(kinds)) for _ in range(rnd.randint(0, 4)))
        kw = dict(disabled=c.scan.disabled_tactics, disabled_in=c.scan.disabled_tactic_regimes,
                  disabled_in_group=c.scan.disabled_tactic_groups)
        for tactic, signal in pairs:
            sig = T.TacticSignal(name=signal, direction=1, quality=80.0, invalidation=1.0, entry_hint=1.1)
            for regime in Regime:
                tac = T.Tactic(tactic, (regime,), lambda ctx, side, s=sig: s)
                monkeypatch.setattr(T, "candidates_for", lambda r, t=tac: (t,))
                for kind in kinds:
                    scanner = T.best_signal(None, 1, regime, group=kind, **kw) is not None
                    assert allowed_by_rules(c, signal, regime.value, kind, None) is scanner, (c.scan, tactic, signal)


def test_the_floors_conditions_in_any_case_and_a_twins_own_floor():
    c = Config()
    assert not allowed_by_rules(c, "MOMENTUM_CONTINUATION", "trend", "FX_MINOR", 80.0)   # off in a trend, any case
    c.scan.tactic_min_score = {"SESSION_EXPANSION": 60.0}                       # below the STRONG floor: 70 holds
    assert not allowed_by_rules(c, "SESSION_EXPANSION", "TREND", "FX_MINOR", 65.0)
    c.scan.tactic_min_score = {"SESSION_EXPANSION": 80.0}
    c.scan.twin_enabled, c.scan.twin_tactics = True, ("SESSION_EXPANSION",)
    assert not allowed_by_rules(c, "SESSION_EXPANSION_2X", "TREND", "FX_MINOR", 75.0)    # the base's own floor
    assert allowed_by_rules(c, "SESSION_EXPANSION_2X", "TREND", "FX_MINOR", 80.0)
