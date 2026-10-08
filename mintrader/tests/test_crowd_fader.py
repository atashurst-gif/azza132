"""Crowd Fader: the positioning read, the client, the paper engine, and the
same engine LIVE on the simulated broker."""
import datetime as dt
import io
import json
import os
import urllib.error

import pytest

from mintel.botexec import MAGIC_CROWD
from mintel.broker.base import Bar, OrderRequest, Side, TF, Tick
from mintel.broker.sim import SimBroker
from mintel.clock import to_utc
from mintel.config import Config
from mintel.crowd import STRATEGY_ID, STRATEGY_LABEL
from mintel.crowd.client import CoinversaClient, CoinversaError, normalize_coin, read_api_key, save_api_key
from mintel.crowd.engine import CrowdConfig, CrowdFader, context_from_db
from mintel.crowd.signal import summarise

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 7, 9, 0, tzinfo=UTC)          # a Wednesday, inside entry hours


def gold_cohorts():
    """Coinversa's xyz:GOLD read on 8 Oct 2026, 07:08 UTC (the real figures)."""
    rows = [("money_printer", 20, 10, 38042632.8, 81305542.4), ("giga_rekt", 13, 11, 23833397.5, 24709591.6),
            ("full_rekt", 52, 20, 13674102.1, 21661729.0), ("smart_money", 89, 20, 18707617.3, 8106134.1),
            ("semi_rekt", 130, 29, 16941052.6, 6915152.5), ("grinder", 224, 35, 19120952.0, 3717932.8),
            ("humble_earner", 2242, 350, 16384546.2, 4837749.8), ("exit_liquidity", 1702, 367, 8701344.1, 4210107.6)]
    return [{"coin": "xyz:GOLD", "pnlTier": t, "longCount": lc, "shortCount": sc, "longNotional": ln, "shortNotional": sn}
            for t, lc, sc, ln, sn in rows]


def gold_heatmap(price=4124.4):
    w = price * 0.005
    buckets = []
    for k in range(-6, 6):
        lo, hi = price + k * w, price + (k + 1) * w
        b = {"priceLow": lo, "priceHigh": hi, "longNotionalAtRisk": 0.0, "shortNotionalAtRisk": 0.0}
        if k == -5:
            b["longNotionalAtRisk"] = 1_971_141.7        # the heaviest long cluster, about 2.25% below
        elif k == -6:
            b["longNotionalAtRisk"] = 435_902.9
        elif k == -4:
            b["longNotionalAtRisk"] = 1_259_235.8
        elif k == -3:
            b["longNotionalAtRisk"] = 601_157.8
        elif k == 2:
            b["shortNotionalAtRisk"] = 865_844.2
        elif k in (3, 4, 5):
            b["shortNotionalAtRisk"] = 100_000.0
        buckets.append(b)
    return {"coin": "xyz:GOLD", "currentPrice": price, "buckets": buckets}


class TestTheRead:
    def test_gold_on_8_october_reads_as_a_stretched_crowd_the_winners_do_not_share(self):
        r = summarise("XAUUSD", "xyz:GOLD", gold_cohorts(), gold_heatmap(), NOW)
        assert r.crowd_long == 1702 + 130 + 52 + 13 and r.crowd_short == 367 + 29 + 20 + 11
        assert r.crowd_bias == pytest.approx(0.63, abs=0.01)                      # 82% of the crowd long
        assert r.winners_bias == pytest.approx(-0.22, abs=0.01)                   # Apex + Sharps net short by money
        assert r.fuel_below > r.fuel_above and r.cluster_below == pytest.approx(4124.4 * (1 - 0.0225), abs=1.0)
        assert r.direction == -1 and 65 <= r.score <= 80
        assert "lean SHORT" in r.reason and "crowd 82% long" in r.reason

    def test_a_crowd_that_is_not_stretched_is_nothing(self):
        rows = gold_cohorts()
        for row in rows:
            row["shortCount"] = row["longCount"]
        r = summarise("XAUUSD", "xyz:GOLD", rows, gold_heatmap(), NOW)
        assert r.direction == 0 and "not stretched" in r.reason

    def test_winners_with_the_crowd_means_stand_aside(self):
        rows = gold_cohorts()
        for row in rows:
            if row["pnlTier"] in ("money_printer", "smart_money"):
                row["longNotional"], row["shortNotional"] = 90e6, 10e6
        r = summarise("XAUUSD", "xyz:GOLD", rows, gold_heatmap(), NOW)
        assert r.direction == 0 and "winners are with the crowd" in r.reason

    def test_the_mirror_image_leans_long(self):
        rows = gold_cohorts()
        for row in rows:
            row["longCount"], row["shortCount"] = row["shortCount"], row["longCount"]
            row["longNotional"], row["shortNotional"] = row["shortNotional"], row["longNotional"]
        r = summarise("XAUUSD", "xyz:GOLD", rows, None, NOW)
        assert r.direction == 1 and r.crowd_bias < -0.5 and "lean LONG" in r.reason

    def test_too_few_wallets_is_no_read(self):
        rows = [dict(r, longCount=r["longCount"] // 50, shortCount=r["shortCount"] // 50) for r in gold_cohorts()]
        r = summarise("XAUUSD", "xyz:GOLD", rows, gold_heatmap(), NOW)
        assert r.direction == 0 and "too few" in r.reason


class FakeResponse:
    def __init__(self, body, status=200, headers=None):
        self.body = json.dumps(body).encode(); self.status = status
        self.headers = headers or {"X-RateLimit-Remaining": "599"}
    def read(self): return self.body
    def __enter__(self): return self
    def __exit__(self, *a): return False


class TestTheClient:
    def test_coins_are_spelt_as_the_connector_spells_them(self):
        assert normalize_coin("XYZ:gold") == "xyz:GOLD" and normalize_coin("btc") == "BTC"

    def test_the_key_goes_in_the_header_and_the_coin_in_the_path(self):
        seen = []
        def opener(req, timeout=0):
            seen.append(req)
            return FakeResponse(gold_cohorts())
        c = CoinversaClient("cvsa_test", opener=opener, sleep=lambda s: None)
        rows = c.cohort_bias("xyz:GOLD")
        assert len(rows) == 8 and seen[0].full_url.endswith("/api/public/v1/live/cohort-bias/xyz:GOLD")
        assert seen[0].get_header("X-api-key") == "cvsa_test" and c.remaining == 599
        c.heatmap("xyz:GOLD", 3.0, 20)
        assert "/live/liquidation-heatmap/xyz:GOLD?" in seen[1].full_url and "range=3" in seen[1].full_url

    def test_numbers_go_on_the_wire_as_javascript_sends_them(self):
        seen = []
        def opener(req, timeout=0):
            seen.append(req); return FakeResponse({"buckets": []})
        c = CoinversaClient("cvsa_test", opener=opener, sleep=lambda s: None)
        c.heatmap("xyz:GOLD", 3.0, 20)
        assert seen[0].full_url.endswith("/live/liquidation-heatmap/xyz:GOLD?buckets=20&range=3")
        c.heatmap("xyz:GOLD", 2.5, 12)
        assert seen[1].full_url.endswith("?buckets=12&range=2.5")

    def test_a_validation_error_names_the_request_and_the_detail(self):
        def opener(req, timeout=0):
            raise urllib.error.HTTPError(req.full_url, 422, "no", {}, io.BytesIO(b'{"error":"validation failed","errors":[{"path":"range","message":"must be integer"}]}'))
        c = CoinversaClient("cvsa_test", opener=opener, sleep=lambda s: None)
        with pytest.raises(CoinversaError) as ei:
            c.heatmap("xyz:GOLD", 3.0, 20)
        assert ei.value.status == 422 and "/live/liquidation-heatmap/xyz:GOLD" in str(ei.value) and "must be integer" in str(ei.value)

    def test_a_rate_limit_is_retried_once_and_a_bad_key_is_not(self):
        calls = []
        def opener(req, timeout=0):
            calls.append(1)
            if len(calls) == 1:
                raise urllib.error.HTTPError(req.full_url, 429, "slow down", {"Retry-After": "1"}, io.BytesIO(b'{"error":"too fast"}'))
            return FakeResponse(gold_cohorts())
        waits = []
        c = CoinversaClient("cvsa_test", opener=opener, sleep=waits.append)
        assert len(c.cohort_bias("xyz:GOLD")) == 8 and waits == [1.0] and len(calls) == 2
        def bad(req, timeout=0):
            raise urllib.error.HTTPError(req.full_url, 401, "no", {}, io.BytesIO(b'{"detail":"invalid key"}'))
        c2 = CoinversaClient("cvsa_bad", opener=bad, sleep=lambda s: None)
        with pytest.raises(CoinversaError) as ei:
            c2.cohort_bias("xyz:GOLD")
        assert ei.value.status == 401 and "invalid key" in str(ei.value)

    def test_the_key_is_kept_in_secrets_json_with_everything_else(self, tmp_path):
        (tmp_path / "secrets.json").write_text(json.dumps({"github_token": "ghp_keep", "account_password": "pw"}))
        p = save_api_key(tmp_path, "cvsa_abc ")
        d = json.loads(p.read_text())
        assert d == {"github_token": "ghp_keep", "account_password": "pw", "coinversa_api_key": "cvsa_abc"}
        assert read_api_key(tmp_path) == "cvsa_abc"
        assert oct(os.stat(p).st_mode & 0o777) == "0o600"
        assert read_api_key(tmp_path / "nowhere") == ""


class Spec:
    """Index-like: point 0.01, one price point = 1.00 per lot."""
    point = 0.01
    volume_min = 0.01
    volume_step = 0.01
    def money(self, dist, volume): return dist * volume
    def money_per_lot(self, dist): return abs(dist)
    def normalise_price(self, p): return round(p, 2)
    def normalise_volume(self, v): return max(0.0, round(int(v / 0.01 + 1e-9) * 0.01, 2))


class FakeClient:
    def __init__(self, cohorts=None, heat=None):
        self.cohorts = cohorts if cohorts is not None else gold_cohorts()
        self.heat = heat if heat is not None else gold_heatmap()
        self.calls = 0; self.remaining = 500
    def cohort_bias(self, coin): self.calls += 1; return self.cohorts
    def heatmap(self, coin, r, b): self.calls += 1; return self.heat


def m15(now, closes, highs=None, lows=None):
    """Closed M15 bars ending 15 minutes before now, plus one still forming."""
    n = len(closes)
    bars = []
    for i, c in enumerate(closes):
        t = now - dt.timedelta(minutes=15 * (n - i))
        h = (highs[i] if highs else c + 2.0); lo = (lows[i] if lows else c - 2.0)
        bars.append(Bar(t, c, h, lo, c, 100.0))
    bars.append(Bar(now, closes[-1], closes[-1] + 50, closes[-1] - 50, closes[-1] - 40, 10.0))   # forming: must be ignored
    return bars


def engine(tmp_path, client=None, **kw):
    kw.setdefault("markets", ("XAUUSD=xyz:GOLD",))
    return CrowdFader(tmp_path, CrowdConfig(**kw), client=client if client is not None else FakeClient(),
                      spec_fn=lambda s: Spec(), clock=lambda: NOW, threaded=False)


def run(e, now, bid, ask, bars):
    return e.step(now, lambda s: Tick(s, now, bid, ask), lambda s, tf, n: bars[-n:] if tf is TF.M15 else [])


class TestTheEngine:
    def test_a_read_is_recorded_and_no_trade_until_the_price_turns(self, tmp_path):
        e = engine(tmp_path)
        flat = [4120.0] * 12
        notes = run(e, NOW, 4120.0, 4120.3, m15(NOW, flat))
        assert not e.open and e.polls == 1 and e.client.calls == 2
        assert "waiting for the price to turn" in e.notes_by_market["XAUUSD"]
        rows = e.checks_since(NOW - dt.timedelta(hours=1))
        assert len(rows) == 1 and rows[0]["direction"] == -1 and rows[0]["symbol"] == "XAUUSD"
        assert any("lean SHORT" in n for n in notes)
        st = e.status(NOW)
        assert st["status"] == "WATCHING" and st["reads"]["XAUUSD"]["crowd_long_pct"] == pytest.approx(81.6, abs=0.1)

    def test_a_break_down_opens_a_short_with_the_stop_over_the_last_hour_and_the_cluster_as_target(self, tmp_path):
        e = engine(tmp_path)
        closes = [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0]      # the last close under the previous eight
        highs = [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]       # the last hour's high is 4150
        notes = run(e, NOW, 4120.0, 4120.3, m15(NOW, closes, highs))
        assert any("XAUUSD SELL" in n for n in notes), notes
        p = e.open["XAUUSD"]
        assert p.side is Side.SELL and p.entry == pytest.approx(4120.0 - 0.01)
        assert p.stop == pytest.approx(4150.0 + 0.3)
        dist = p.stop - p.entry
        assert p.volume == pytest.approx(10.0 / dist, abs=0.01)
        cluster_move = (4124.4 * (1 - 0.0225) - 4124.4) / 4124.4
        assert p.target == pytest.approx(4120.0 * (1 + cluster_move), abs=0.6)         # the heaviest long cluster, in R range
        assert 1.0 <= (p.entry - p.target) / dist <= 4.0
        row = e.db.execute("SELECT * FROM trades WHERE ticket=?", (p.ticket,)).fetchone()
        assert row["score"] >= 60 and row["risk_money"] == pytest.approx(10.0, abs=0.4) and row["coin"] == "xyz:GOLD"
        assert e.entries_today[("XAUUSD", NOW.date().isoformat())] == 1

    def _short(self, tmp_path, **kw):
        e = engine(tmp_path, **kw)
        closes = [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0]
        highs = [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]
        run(e, NOW, 4120.0, 4120.3, m15(NOW, closes, highs))
        assert e.open
        return e, m15(NOW, closes, highs)

    def test_the_stop_closes_at_the_stop_for_the_planned_loss(self, tmp_path):
        e, bars = self._short(tmp_path)
        p = e.open["XAUUSD"]
        later = NOW + dt.timedelta(minutes=30)
        notes = run(e, later, 4151.0, 4151.3, bars)                   # the ask is through the stop
        assert not e.open and any("closed STOP" in n for n in notes)
        row = e.closed_since(NOW)[-1]
        assert row["exit_reason"] == "STOP" and row["exit_price"] == pytest.approx(p.stop)
        assert row["net_pnl"] == pytest.approx(-10.0, abs=0.4) and row["realised_r"] == pytest.approx(-1.0, abs=0.01)

    def test_the_target_closes_at_the_target(self, tmp_path):
        e, bars = self._short(tmp_path)
        p = e.open["XAUUSD"]
        notes = run(e, NOW + dt.timedelta(hours=2), p.target - 1.0, p.target - 0.7, bars)
        assert not e.open and any("closed TARGET" in n for n in notes)
        row = e.closed_since(NOW)[-1]
        assert row["exit_reason"] == "TARGET" and row["net_pnl"] > 10.0 and row["realised_r"] >= 1.0

    def test_a_trade_that_goes_nowhere_is_closed_after_the_hold(self, tmp_path):
        e, bars = self._short(tmp_path)
        notes = run(e, NOW + dt.timedelta(hours=25), 4121.0, 4121.3, bars)
        assert not e.open and any("closed TIME" in n for n in notes)

    def test_friday_evening_closes_everything_and_takes_nothing_new(self, tmp_path):
        e, bars = self._short(tmp_path, hold_hours=100.0)
        fri = dt.datetime(2026, 10, 9, 20, 35, tzinfo=UTC)
        notes = run(e, fri, 4121.0, 4121.3, bars)
        assert not e.open and any("closed WEEKEND" in n for n in notes)
        assert not e.in_entry_hours(fri) and not e.in_entry_hours(dt.datetime(2026, 10, 10, 10, 0, tzinfo=UTC))
        assert e.in_entry_hours(NOW) and not e.in_entry_hours(NOW.replace(hour=21))

    def test_one_entry_per_read_and_at_most_two_a_day(self, tmp_path):
        e, bars = self._short(tmp_path)
        p = e.open["XAUUSD"]
        run(e, NOW + dt.timedelta(minutes=5), 4151.0, 4151.3, bars)            # stopped
        run(e, NOW + dt.timedelta(minutes=6), 4120.0, 4120.3, bars)            # same read: no re-entry
        assert not e.open and "already acted" in e.notes_by_market["XAUUSD"]
        t2 = NOW + dt.timedelta(minutes=11)                                    # a fresh read is due
        run(e, t2, 4120.0, 4120.3, m15(t2, [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0], [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]))
        assert e.open and e.polls == 2
        run(e, t2 + dt.timedelta(minutes=5), 4151.0, 4151.3, bars)             # stopped again
        t3 = t2 + dt.timedelta(minutes=11)
        run(e, t3, 4120.0, 4120.3, m15(t3, [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0], [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]))
        assert not e.open and "no more entries today" in e.notes_by_market["XAUUSD"]

    def test_a_wide_stop_takes_the_smallest_size_up_to_the_cap_or_is_skipped(self, tmp_path):
        class Gold(Spec):
            def money_per_lot(self, dist): return abs(dist) * 100.0       # 100 oz a lot
        e = CrowdFader(tmp_path, CrowdConfig(markets=("XAUUSD=xyz:GOLD",)), client=FakeClient(),
                       spec_fn=lambda s: Gold(), clock=lambda: NOW, threaded=False)
        closes = [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0]
        highs = [4132.0] * 8 + [4140.0, 4131.0, 4129.0, 4125.0]            # 20 points: 0.01 lots risks 20
        run(e, NOW, 4120.0, 4120.3, m15(NOW, closes, highs))
        p = e.open["XAUUSD"]
        assert p.volume == 0.01 and e.db.execute("SELECT risk_money FROM trades").fetchone()[0] == pytest.approx(20.3, abs=0.1)
        e2 = CrowdFader(tmp_path / "b", CrowdConfig(markets=("XAUUSD=xyz:GOLD",)), client=FakeClient(),
                        spec_fn=lambda s: Gold(), clock=lambda: NOW, threaded=False)
        highs = [4132.0] * 8 + [4160.0, 4131.0, 4129.0, 4125.0]            # 40 points: 0.01 lots risks 40, over 30
        run(e2, NOW, 4120.0, 4120.3, m15(NOW, closes, highs))
        assert not e2.open and "over 30" in e2.notes_by_market["XAUUSD"]

    def test_a_restart_restores_the_position_and_the_last_read(self, tmp_path):
        e, bars = self._short(tmp_path)
        p = e.open["XAUUSD"]
        e.close()
        e2 = engine(tmp_path, client=FakeClient(cohorts=[]))
        assert e2.open["XAUUSD"].ticket == p.ticket and e2.open["XAUUSD"].target == p.target
        assert e2.latest["XAUUSD"].direction == -1 and e2.entries_today[("XAUUSD", NOW.date().isoformat())] == 1

    def test_without_a_key_it_says_so_and_never_polls(self, tmp_path):
        e = CrowdFader(tmp_path, CrowdConfig(markets=("XAUUSD=xyz:GOLD",)), spec_fn=lambda s: Spec(),
                       clock=lambda: NOW, threaded=False, api_key="")
        assert e.client is None and not e.key_present
        run(e, NOW, 4120.0, 4120.3, m15(NOW, [4120.0] * 12))
        assert e.notes_by_market["XAUUSD"] == "no Coinversa key saved" and e.polls == 0
        assert e.status(NOW)["status"].startswith("NO KEY")

    def test_coinversa_down_is_reported_not_fatal(self, tmp_path):
        class Down:
            calls = 0; remaining = None
            def cohort_bias(self, coin): raise CoinversaError("cannot reach Coinversa: timed out")
            def heatmap(self, coin, r, b): raise CoinversaError("x")
        e = engine(tmp_path, client=Down())
        run(e, NOW, 4120.0, 4120.3, m15(NOW, [4120.0] * 12))
        assert "cannot reach" in e.poll_error and e.status(NOW)["status"] == "COINVERSA UNAVAILABLE"

    def test_the_context_for_another_bots_trade(self, tmp_path):
        e = engine(tmp_path)
        run(e, NOW, 4120.0, 4120.3, m15(NOW, [4120.0] * 12))
        c = e.context_for("XAUUSD", NOW + dt.timedelta(minutes=40))
        assert c and c["direction"] == -1 and c["age_minutes"] == 40.0
        assert e.context_for("XAUUSD", NOW + dt.timedelta(hours=3)) is None
        assert e.context_for("XAUUSD", NOW - dt.timedelta(minutes=1)) is None
        e.close()
        c2 = context_from_db(tmp_path / "crowd.sqlite", "XAUUSD", NOW + dt.timedelta(minutes=5))
        assert c2 and "lean SHORT" in c2["reason"]

    def test_the_nightly_file_carries_the_reads(self, tmp_path):
        from mintel.config import Config
        from mintel.ops.report_upload import paper_bot_json
        e, bars = self._short(tmp_path)
        run(e, NOW + dt.timedelta(minutes=30), 4151.0, 4151.3, bars)
        e.write_status(NOW + dt.timedelta(minutes=30), every_seconds=0)
        cfg = Config(); cfg.ops.data_dir = str(tmp_path)
        out = json.loads(paper_bot_json(cfg, NOW, "crowd-status.json", "crowd.sqlite"))
        assert len(out["trades"]) == 1 and out["checks"] and out["status"]["strategy_id"] == STRATEGY_ID

    def test_it_has_its_own_tab(self, tmp_path):
        from mintel.ops.attribution import paper_tab
        e, bars = self._short(tmp_path)
        run(e, NOW + dt.timedelta(minutes=30), 4151.0, 4151.3, bars)
        e.write_status(NOW + dt.timedelta(minutes=30), every_seconds=0)
        stats, status = paper_tab(tmp_path, "crowd.sqlite", "crowd-status.json", STRATEGY_ID, STRATEGY_LABEL,
                                  NOW.replace(hour=0), None, "GBP", now=NOW + dt.timedelta(minutes=31))
        assert stats["trades"] == 1 and stats["net_today"] == pytest.approx(-10.0, abs=0.4) and status["present"]


    def test_the_account_gate_holds_in_paper_too(self, tmp_path):
        e = CrowdFader(tmp_path, CrowdConfig(markets=("XAUUSD=xyz:GOLD",)), client=FakeClient(), spec_fn=lambda s: Spec(),
                       clock=lambda: NOW, threaded=False, entries_allowed=lambda: False)
        closes = [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0]
        highs = [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]
        run(e, NOW, 4120.0, 4120.3, m15(NOW, closes, highs))
        assert not e.open and e.notes_by_market["XAUUSD"] == "entries paused by the account gate"
        assert e.acted_on == {}                                                 # the read is still there for when the gate opens

    def test_paper_tickets_carry_on_from_the_records_after_a_restart(self, tmp_path):
        e, bars = self._short(tmp_path)
        first = e.open["XAUUSD"].ticket
        run(e, NOW + dt.timedelta(minutes=30), 4151.0, 4151.3, bars)           # stopped
        e.close()
        e2 = engine(tmp_path)
        t2 = NOW + dt.timedelta(minutes=11)
        run(e2, t2, 4120.0, 4120.3, m15(t2, [4130.0] * 8 + [4128.0, 4126.0, 4124.0, 4119.0], [4132.0] * 8 + [4150.0, 4131.0, 4129.0, 4125.0]))
        assert e2.open and e2.open["XAUUSD"].ticket == first + 1 and e2.open["XAUUSD"].mode == "PAPER"
        assert e2.db.execute("SELECT mode FROM trades WHERE ticket=?", (first + 1,)).fetchone()[0] == "PAPER"


class LiveRig:
    """The engine in LIVE on a SimBroker. The M15 fixtures sit around the sim's
    own gold price so the broker accepts every stop and target, and each pass
    moves the sim's quote to the price under test before the engine looks."""

    def __init__(self, tmp_path, wrap=None, **kw):
        self.tmp_path = tmp_path
        self.sim = SimBroker(["XAUUSD", "EURUSD"], start=NOW - dt.timedelta(minutes=1), history_bars=50)
        self.sim.connect()
        self.L = float(round(self.sim.tick("XAUUSD").mid))
        self.broker = wrap(self.sim) if wrap else self.sim
        self.kw = kw
        self.e = self._build()

    def _build(self):
        kw = dict(self.kw)
        kw.setdefault("markets", ("XAUUSD=xyz:GOLD",))
        kw.setdefault("mode", "LIVE")
        gate = kw.pop("entries_allowed", None)
        return CrowdFader(self.tmp_path, CrowdConfig(**kw), client=FakeClient(), spec_fn=self.sim.spec, clock=lambda: NOW,
                          threaded=False, broker=self.broker, entries_allowed=gate)

    def restart(self):
        self.e.close()
        self.e = self._build()
        return self.e

    def quote(self, now, price, high=None, low=None):
        self.sim.now = now - dt.timedelta(seconds=60)
        self.sim.push_bar("XAUUSD", price, high=price + 0.5 if high is None else high, low=price - 0.5 if low is None else low)

    def bars(self, now, turned=True):
        """Eight closes at L+10, then a slide that closes under them (the turn);
        the last hour's high is L+20, so the stop sits at L+20 plus the spread."""
        L = self.L
        closes = [L + 10.0] * 8 + ([L + 8.0, L + 6.0, L + 4.0, L - 1.0] if turned else [L + 8.0] * 4)
        highs = [L + 12.0] * 8 + [L + 20.0, L + 11.0, L + 9.0, L + 5.0]
        return m15(now, closes, highs)

    def run(self, now, price, turned=True, high=None, low=None):
        self.quote(now, price, high, low)
        bars = self.bars(now, turned)
        return self.e.step(now, self.sim.tick, lambda s, tf, n: bars[-n:] if tf is TF.M15 else [])

    def mine(self):
        return self.sim.positions(MAGIC_CROWD)

    def row(self, ticket):
        return dict(self.e.db.execute("SELECT * FROM trades WHERE ticket=?", (ticket,)).fetchone())


class Blank:
    """A broker whose positions() answers [] while ``blank`` is set, as the MT5
    adapter does on a transient terminal error (no exception, an empty list)."""

    def __init__(self, sim):
        self.sim = sim; self.blank = False

    def __getattr__(self, name):
        return getattr(self.sim, name)

    def positions(self, magic=None):
        return [] if self.blank else self.sim.positions(magic)


def n_rows(e):
    return e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]


class TestLive:
    def test_a_live_entry_carries_the_magic_with_the_stop_and_the_target_at_the_broker(self, tmp_path):
        rig = LiveRig(tmp_path)
        notes = rig.run(NOW, rig.L)
        assert any("XAUUSD SELL" in n and "live ticket" in n for n in notes), notes
        p = rig.e.open["XAUUSD"]
        mine = rig.mine()
        assert len(mine) == 1
        q = mine[0]
        assert q.ticket == p.ticket and q.magic == MAGIC_CROWD and q.comment == "CROWD" and q.side is Side.SELL
        assert q.sl == pytest.approx(p.stop) and q.tp == pytest.approx(p.target)      # the broker holds both levels
        assert p.stop == pytest.approx(rig.L + 20.0 + 0.18) and p.target < p.entry < p.stop
        assert p.entry == pytest.approx(rig.sim.tick("XAUUSD").bid)                  # the broker's fill is the entry
        assert p.volume == 0.01 and p.mode == "LIVE"
        assert not rig.sim.positions(Config().magic)                                 # Trend & Breakout never sees it
        row = rig.row(p.ticket)
        assert row["mode"] == "LIVE" and row["coin"] == "xyz:GOLD" and row["target"] == pytest.approx(p.target)
        st = rig.e.status(NOW)
        assert st["mode"] == "LIVE" and st["live"] is True and st["magic"] == MAGIC_CROWD and st["status"] == "IN TRADE"
        assert st["executor_error"] == "" and st["refused"] == 0 and st["open"][0]["mode"] == "LIVE"

    def test_a_time_exit_closes_at_the_broker_and_the_brokers_figure_is_the_result(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        notes = rig.run(NOW + dt.timedelta(hours=25), rig.L + 1.0)
        assert any("closed TIME" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()
        row = rig.e.closed_since(NOW)[-1]
        deal = rig.sim.closed_deal(ticket)
        assert row["ticket"] == ticket and row["exit_reason"] == "TIME" and row["mode"] == "LIVE"
        assert row["exit_price"] == pytest.approx(deal["exit_price"]) and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["net_pnl"] < 0                                                     # sold at L, bought back at L+1 plus the spread

    def test_the_brokers_stop_closes_it_and_the_row_says_stop_from_the_deal(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        p = rig.e.open["XAUUSD"]
        notes = rig.run(NOW + dt.timedelta(minutes=30), rig.L + 15.0, high=rig.L + 21.0)   # the bar's high goes through the stop
        assert any("closed by the broker (STOP)" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()
        row = rig.e.closed_since(NOW)[-1]
        deal = rig.sim.closed_deal(p.ticket)
        assert row["exit_reason"] == "STOP" and row["exit_price"] == pytest.approx(p.stop)
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["net_pnl"] == pytest.approx(-20.27, abs=0.05)
        assert row["realised_r"] == pytest.approx(-1.0, abs=0.01)

    def test_the_brokers_target_closes_it_and_the_row_says_target(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        p = rig.e.open["XAUUSD"]
        notes = rig.run(NOW + dt.timedelta(hours=2), rig.L - 40.0, low=p.target - 5.0)     # the bar's low goes through the target
        assert any("closed by the broker (TARGET)" in n for n in notes), notes
        row = rig.e.closed_since(NOW)[-1]
        deal = rig.sim.closed_deal(p.ticket)
        assert row["exit_reason"] == "TARGET" and row["exit_price"] == pytest.approx(p.target)
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["net_pnl"] > 0 and row["realised_r"] > 1.0

    def test_a_target_the_broker_did_not_keep_is_closed_at_the_market_as_a_backstop(self, tmp_path):
        class DropsTheTarget:
            def __init__(self, sim):
                self.sim = sim
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def send(self, req):
                req.tp = 0.0                                                 # a broker quirk: the stop is kept, the target is not
                return self.sim.send(req)
        rig = LiveRig(tmp_path, wrap=DropsTheTarget)
        rig.run(NOW, rig.L)
        p = rig.e.open["XAUUSD"]
        assert rig.mine()[0].tp == 0.0 and rig.mine()[0].sl == pytest.approx(p.stop)
        notes = rig.run(NOW + dt.timedelta(hours=2), p.target - 1.0, low=p.target - 1.5)   # the price is through the target
        assert any("closed TARGET" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()
        row = rig.e.closed_since(NOW)[-1]
        assert row["exit_reason"] == "TARGET" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(p.ticket)["pnl"], 2))
        assert row["net_pnl"] > 0

    def test_a_position_the_broker_closed_is_finalised_once_from_its_deal(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        p = rig.e.open["XAUUSD"]
        ticket = p.ticket
        rig.quote(NOW + dt.timedelta(minutes=30), rig.L - 45.0)              # near the target, not through it
        rig.sim.close(ticket)                                                # the broker takes it between two passes
        notes = rig.run(NOW + dt.timedelta(minutes=31), rig.L - 45.0)
        assert any("closed by the broker (MANUAL)" in n for n in notes), notes     # the broker's own word, not the nearer level
        rows = rig.e.closed_since(NOW)
        deal = rig.sim.closed_deal(ticket)
        assert len(rows) == 1 and rows[0]["ticket"] == ticket and rows[0]["exit_reason"] == "MANUAL"
        assert rows[0]["exit_price"] == pytest.approx(deal["exit_price"]) and rows[0]["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        before = rig.row(ticket)
        notes2 = rig.run(NOW + dt.timedelta(minutes=32), rig.L - 45.0)       # nothing more happens to it
        assert not any("closed" in n for n in notes2) and rig.row(ticket) == before and not rig.e.open
        assert len(rig.e.closed_since(NOW)) == 1

    def test_a_refused_order_records_nothing_and_the_bot_keeps_running(self, tmp_path):
        class Refuses:
            def __init__(self, sim):
                self.sim = sim; self.refuse = True; self.sends = 0
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def send(self, req):
                self.sends += 1
                if self.refuse:
                    raise RuntimeError("market closed")
                return self.sim.send(req)
        rig = LiveRig(tmp_path, wrap=Refuses)
        notes = rig.run(NOW, rig.L)
        assert any("order refused" in n and "market closed" in n for n in notes), notes
        assert not rig.e.open and not rig.mine() and rig.broker.sends == 1
        assert rig.e.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        assert rig.e.notes_by_market["XAUUSD"].endswith("order refused: send failed: market closed")
        st = rig.e.status(NOW)
        assert st["refused"] == 1 and "market closed" in st["executor_error"] and st["status"] == "WATCHING"
        rig.run(NOW + dt.timedelta(minutes=1), rig.L)                        # the same read is spent: not hammered
        assert rig.broker.sends == 1 and "already acted" in rig.e.notes_by_market["XAUUSD"]
        rig.broker.refuse = False
        t2 = NOW + dt.timedelta(minutes=11)                                  # a fresh read is due
        notes = rig.run(t2, rig.L)
        assert rig.e.open and len(rig.mine()) == 1 and rig.broker.sends == 2 and rig.e.polls == 2

    def test_a_refused_close_keeps_the_position_and_tries_again(self, tmp_path):
        class CloseRefuses:
            def __init__(self, sim):
                self.sim = sim; self.refuse = 0
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def close(self, ticket, volume=0.0, comment=""):
                if self.refuse > 0:
                    self.refuse -= 1
                    raise RuntimeError("busy")
                return self.sim.close(ticket, volume, comment)
        rig = LiveRig(tmp_path, wrap=CloseRefuses, hold_hours=100.0)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.broker.refuse = 1
        fri = dt.datetime(2026, 10, 9, 20, 35, tzinfo=UTC)
        notes = rig.run(fri, rig.L + 1.0)
        assert any("close (WEEKEND) refused" in n and "will retry" in n for n in notes), notes
        assert rig.e.open["XAUUSD"].ticket == ticket and len(rig.mine()) == 1     # still there, with its stop
        assert "will retry" in rig.e.notes_by_market["XAUUSD"]
        notes = rig.run(fri + dt.timedelta(minutes=1), rig.L + 1.0)
        assert any("closed WEEKEND" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()
        row = rig.e.closed_since(NOW)[-1]
        assert row["exit_reason"] == "WEEKEND" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))

    def test_a_restart_keeps_a_live_position_still_at_the_broker(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.restart()
        assert rig.e.open["XAUUSD"].ticket == ticket and rig.e.open["XAUUSD"].mode == "LIVE" and len(rig.mine()) == 1
        assert rig.e.entries_today[("XAUUSD", NOW.date().isoformat())] == 1
        notes = rig.run(NOW + dt.timedelta(minutes=5), rig.L)
        assert not any("orphan" in n or "closed" in n for n in notes), notes
        assert len(rig.mine()) == 1 and len(rig.sim.closed) == 0

    def test_a_restart_finalises_a_position_the_broker_closed_while_the_bot_was_down(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.quote(NOW + dt.timedelta(minutes=30), rig.L + 15.0, high=rig.L + 21.0)   # the stop, while the bot is down
        assert not rig.mine()
        rig.restart()
        assert not rig.e.open
        notes = rig.run(NOW + dt.timedelta(minutes=31), rig.L + 15.0)
        assert any("closed by the broker (STOP)" in n for n in notes), notes
        row = rig.e.closed_since(NOW)[-1]
        assert row["ticket"] == ticket and row["exit_reason"] == "STOP"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))

    def test_a_restart_closes_an_orphan_with_our_magic_and_leaves_other_bots_alone(self, tmp_path):
        rig = LiveRig(tmp_path)
        t = rig.sim.tick("XAUUSD")
        ours = rig.sim.send(OrderRequest("XAUUSD", Side.BUY, 0.1, sl=t.bid - 10.0, comment="CROWD", magic=MAGIC_CROWD))
        theirs = rig.sim.send(OrderRequest("XAUUSD", Side.BUY, 0.1, sl=t.bid - 10.0, comment="MI", magic=Config().magic))
        assert ours.ok and theirs.ok
        rig.restart()
        notes = rig.run(NOW, rig.L, turned=False)
        assert any("orphan closed" in n and str(ours.ticket) in n for n in notes), notes
        assert not rig.mine() and not rig.e.open
        assert [p.ticket for p in rig.sim.positions(Config().magic)] == [theirs.ticket]     # not ours: never touched
        rows = rig.e.closed_since(NOW - dt.timedelta(days=1))
        assert len(rows) == 1 and rows[0]["ticket"] == ours.ticket and rows[0]["exit_reason"] == "ORPHAN_CLOSED"
        assert rows[0]["mode"] == "LIVE" and rows[0]["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ours.ticket)["pnl"], 2))

    def test_the_account_gate_blocks_a_new_entry_but_never_the_exit(self, tmp_path):
        gate = {"open": False}
        rig = LiveRig(tmp_path, entries_allowed=lambda: gate["open"])
        rig.run(NOW, rig.L)
        assert not rig.e.open and not rig.mine()
        assert rig.e.notes_by_market["XAUUSD"] == "entries paused by the account gate"
        gate["open"] = True
        rig.run(NOW + dt.timedelta(minutes=1), rig.L)
        assert rig.e.open and len(rig.mine()) == 1
        gate["open"] = False                                                 # managing what is open is never gated
        notes = rig.run(NOW + dt.timedelta(hours=25), rig.L + 1.0)
        assert any("closed TIME" in n for n in notes), notes
        assert not rig.e.open and not rig.mine()

    def test_a_live_row_is_left_alone_by_a_paper_run(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.kw["mode"] = "PAPER"
        rig.restart()
        notes = rig.run(NOW + dt.timedelta(minutes=1), rig.L, turned=False)
        assert not rig.e.open and len(rig.mine()) == 1 and rig.mine()[0].ticket == ticket   # still at the broker, untouched
        assert any("opened in LIVE" in n for n in notes), notes
        st = rig.e.status(NOW)
        assert st["mode"] == "PAPER" and st["live"] is False and st["unmanaged"][0]["ticket"] == ticket

    def test_mode_off_does_nothing_and_never_starts_the_poll_thread(self, tmp_path):
        sim = SimBroker(["XAUUSD"], start=NOW - dt.timedelta(minutes=1), history_bars=50)
        sim.connect()
        client = FakeClient()
        e = CrowdFader(tmp_path, CrowdConfig(markets=("XAUUSD=xyz:GOLD",), mode="OFF"), client=client, spec_fn=sim.spec,
                       clock=lambda: NOW, threaded=True, broker=sim)
        assert e.executor is None and e.mode == "OFF"
        L = float(round(sim.tick("XAUUSD").mid))
        closes = [L + 10.0] * 8 + [L + 8.0, L + 6.0, L + 4.0, L - 1.0]
        bars = m15(NOW, closes, [L + 12.0] * 8 + [L + 20.0, L + 11.0, L + 9.0, L + 5.0])
        notes = e.step(NOW, sim.tick, lambda s, tf, n: bars[-n:] if tf is TF.M15 else [])
        assert notes == [] and not e.open and not sim.positions() and client.calls == 0 and e.polls == 0
        assert e._poll_thread is None
        st = e.status(NOW)
        assert st["mode"] == "OFF" and st["status"] == "OFF" and st["live"] is False and st["magic"] == MAGIC_CROWD
        e.close()

    def test_one_empty_positions_read_never_books_a_position_still_at_the_broker(self, tmp_path):
        rig = LiveRig(tmp_path, wrap=Blank)
        rig.run(NOW, rig.L)
        p = rig.e.open["XAUUSD"]
        ticket = p.ticket
        rig.broker.blank = True                                              # one bad read: the position is still there
        notes = rig.run(NOW + dt.timedelta(minutes=1), rig.L)
        assert any("not in the broker's list and no closing deal yet" in n and "kept" in n for n in notes), notes
        assert rig.e.open["XAUUSD"] is p and p.missing_passes == 1
        assert rig.row(ticket)["closed_utc"] is None and rig.row(ticket)["exit_reason"] is None
        assert "kept, checked again" in rig.e.notes_by_market["XAUUSD"]
        assert len(rig.mine()) == 1 and len(rig.sim.closed) == 0             # untouched at the broker, with its stop
        rig.broker.blank = False
        notes = rig.run(NOW + dt.timedelta(minutes=2), rig.L)
        assert any("back in the broker's list" in n for n in notes), notes
        assert p.missing_passes == 0 and p.missing_since is None and p.broker_pnl is not None
        assert n_rows(rig.e) == 1 and len(rig.mine()) == 1                   # no second position on the market
        notes = rig.run(NOW + dt.timedelta(minutes=30), rig.L + 15.0, high=rig.L + 21.0)   # the broker's stop, later
        assert any("closed by the broker (STOP)" in n for n in notes), notes
        row = rig.row(ticket)
        assert row["exit_reason"] == "STOP" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))
        assert n_rows(rig.e) == 1 and not rig.e.open

    def test_a_position_unseen_for_three_reads_over_a_minute_is_estimated_and_then_takes_the_brokers_figure(self, tmp_path):
        rig = LiveRig(tmp_path, wrap=Blank)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.broker.blank = True
        notes = []
        for m in (1, 2, 3):
            notes += rig.run(NOW + dt.timedelta(minutes=m), rig.L)
        assert any("gone from the broker for 120 s with no deal record" in n and "(estimated" in n for n in notes), notes
        row = rig.row(ticket)
        assert row["exit_reason"] == "STOP?" and not rig.e.open               # nearer the stop than the target; marked estimated
        assert rig.e.status(NOW + dt.timedelta(minutes=3))["estimated_rows"] == 1
        rig.broker.blank = False                                             # the reads work again: it was there all along
        notes = rig.run(NOW + dt.timedelta(minutes=4), rig.L)
        assert any("orphan closed" in n and "booked STOP?, now holds the broker's figure" in n for n in notes), notes
        assert not rig.mine() and not rig.e.open and n_rows(rig.e) == 1
        row = rig.row(ticket)
        deal = rig.sim.closed_deal(ticket)
        assert row["exit_reason"] == "ORPHAN_CLOSED" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["exit_price"] == pytest.approx(deal["exit_price"])
        assert rig.e.status(NOW + dt.timedelta(minutes=4))["estimated_rows"] == 0

    def test_an_estimated_row_takes_the_brokers_figure_once_its_record_comes_in(self, tmp_path):
        class HistoryLags:
            def __init__(self, sim):
                self.sim = sim; self.lagging = False
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def closed_deal(self, ticket):
                return None if self.lagging else self.sim.closed_deal(ticket)
        rig = LiveRig(tmp_path, wrap=HistoryLags)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.broker.lagging = True
        t = NOW + dt.timedelta(hours=25)
        notes = rig.run(t, rig.L + 1.0)
        assert any("closed TIME?" in n for n in notes), notes
        row = rig.row(ticket)
        deal = rig.sim.closed_deal(ticket)
        assert row["exit_reason"] == "TIME?" and row["exit_price"] == pytest.approx(deal["exit_price"])
        assert not rig.e.open and not rig.mine() and rig.e.status(t)["estimated_rows"] == 1
        rig.broker.lagging = False
        notes = rig.run(t + dt.timedelta(minutes=1), rig.L + 1.0, turned=False)
        assert any(f"ticket {ticket}" in n and "now holds the broker's record: TIME at" in n for n in notes), notes
        row = rig.row(ticket)
        assert row["exit_reason"] == "TIME" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["exit_price"] == pytest.approx(deal["exit_price"])
        assert dt.datetime.fromisoformat(row["closed_utc"]) == to_utc(deal["time"])
        assert rig.e.status(t)["estimated_rows"] == 0 and n_rows(rig.e) == 1

    def test_a_close_by_hand_between_the_levels_is_not_a_stop_or_a_target(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.quote(NOW + dt.timedelta(minutes=30), rig.L - 5.0)
        rig.sim.close(ticket, 0.0, "closed by hand")
        notes = rig.run(NOW + dt.timedelta(minutes=31), rig.L - 5.0)
        assert any("closed by the broker (BROKER_CLOSED)" in n for n in notes), notes
        row = rig.row(ticket)
        deal = rig.sim.closed_deal(ticket)
        assert row["exit_reason"] == "BROKER_CLOSED" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["net_pnl"] > 0

    def test_an_engine_close_finalised_after_a_restart_keeps_its_own_reason(self, tmp_path):
        rig = LiveRig(tmp_path)
        rig.run(NOW, rig.L)
        ticket = rig.e.open["XAUUSD"].ticket
        rig.quote(NOW + dt.timedelta(hours=24), rig.L + 1.0)
        rig.sim.close(ticket, 0.0, "CROWD TIME")        # the engine's own close went through; the process died before the row was written
        assert rig.row(ticket)["closed_utc"] is None
        rig.restart()
        notes = rig.run(NOW + dt.timedelta(hours=24, minutes=1), rig.L + 1.0, turned=False)
        assert any("closed by the broker (TIME)" in n for n in notes), notes
        row = rig.row(ticket)
        assert row["exit_reason"] == "TIME" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ticket)["pnl"], 2))
        assert not rig.e.open and n_rows(rig.e) == 1

    def test_a_failed_positions_read_at_start_is_followed_by_the_sweep_once_reads_work(self, tmp_path):
        class FailsOnce:
            def __init__(self, sim):
                self.sim = sim; self.fail = 0
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def positions(self, magic=None):
                if self.fail > 0:
                    self.fail -= 1
                    raise RuntimeError("bridge timeout")
                return self.sim.positions(magic)
        rig = LiveRig(tmp_path, wrap=FailsOnce)
        t = rig.sim.tick("XAUUSD")
        ours = rig.sim.send(OrderRequest("XAUUSD", Side.BUY, 0.1, sl=t.bid - 10.0, comment="CROWD", magic=MAGIC_CROWD))
        assert ours.ok
        rig.broker.fail = 1
        rig.restart()                                                        # the start read fails: nothing is swept yet
        assert len(rig.mine()) == 1 and not rig.e.open and n_rows(rig.e) == 0
        notes = rig.run(NOW, rig.L, turned=False)
        assert any("could not read the broker's positions at start" in n for n in notes), notes
        assert any("orphan closed" in n and str(ours.ticket) in n for n in notes), notes
        assert not rig.mine() and not rig.e.open
        row = rig.row(ours.ticket)
        assert row["exit_reason"] == "ORPHAN_CLOSED" and row["mode"] == "LIVE"
        assert row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(ours.ticket)["pnl"], 2))

    def test_a_fill_whose_ticket_is_already_on_record_never_overwrites_that_row(self, tmp_path):
        rig = LiveRig(tmp_path)
        clash = rig.sim._next_ticket + 1                                     # the number the broker's next ticket will carry
        rig.e.db.execute("""INSERT INTO trades (ticket, symbol, coin, side, volume, entry, initial_stop, target, opened_utc,
                            closed_utc, stop, exit_price, realised_r, net_pnl, exit_reason, mode, session_date)
                            VALUES (?, 'XAUUSD', 'xyz:GOLD', 'BUY', 0.01, 2000.0, 1990.0, 2020.0, ?, ?, 1990.0, 2020.0, 2.0, 0.2,
                                    'TARGET', 'PAPER', ?)""",
                         (clash, (NOW - dt.timedelta(days=2)).isoformat(), (NOW - dt.timedelta(days=1)).isoformat(),
                          (NOW - dt.timedelta(days=2)).date().isoformat()))
        rig.e.db.commit()
        before = rig.row(clash)
        notes = rig.run(NOW, rig.L)
        assert any("order refused" in n and f"ticket {clash} is already on record, closed again at once" in n for n in notes), notes
        assert rig.row(clash) == before                                      # the old PAPER row is untouched
        assert not rig.e.open and not rig.mine() and n_rows(rig.e) == 1
        assert len(rig.sim.closed) == 1 and rig.sim.closed[0]["ticket"] == clash and rig.sim.closed[0]["reason"] == "CROWD dup ticket"
        st = rig.e.status(NOW)
        assert st["refused"] == 1 and st["status"] == "WATCHING"
        rig.run(NOW + dt.timedelta(minutes=1), rig.L)                        # the read is spent: not sent again
        assert len(rig.sim.closed) == 1 and "already acted" in rig.e.notes_by_market["XAUUSD"]
        rig.run(NOW + dt.timedelta(minutes=11), rig.L)                       # the next read: a fresh ticket, kept
        assert rig.e.open and rig.e.open["XAUUSD"].ticket != clash and len(rig.mine()) == 1 and n_rows(rig.e) == 2
        assert rig.row(clash) == before

    def test_an_order_whose_outcome_is_unknown_is_not_sent_again_and_the_stray_position_is_swept(self, tmp_path):
        class ReadBackFails:
            """The order fills, then the read-back of the broker's list fails (a bridge hiccup)."""
            def __init__(self, sim):
                self.sim = sim; self.sends = 0; self.failures = 1; self.fail_next = False
            def __getattr__(self, name):
                return getattr(self.sim, name)
            def send(self, req):
                self.sends += 1
                res = self.sim.send(req)
                self.fail_next = res.ok and self.failures > 0
                return res
            def positions(self, magic=None):
                if self.fail_next:
                    self.fail_next = False; self.failures -= 1
                    raise RuntimeError("bridge timeout")
                return self.sim.positions(magic)
        rig = LiveRig(tmp_path, wrap=ReadBackFails)
        notes = rig.run(NOW, rig.L)
        assert any("order sent, state unknown: bridge timeout" in n and "stray position" in n for n in notes), notes
        assert not rig.e.open and len(rig.mine()) == 1 and n_rows(rig.e) == 0    # filled at the broker, nothing on record
        assert rig.e.status(NOW)["refused"] == 1
        stray = rig.mine()[0].ticket
        notes = rig.run(NOW + dt.timedelta(seconds=5), rig.L)                # swept at once, not on the next minute
        assert any("orphan closed" in n and str(stray) in n for n in notes), notes
        assert not rig.mine() and not rig.e.open and rig.broker.sends == 1    # not sent again on the same read
        row = rig.row(stray)
        assert row["exit_reason"] == "ORPHAN_CLOSED" and row["net_pnl"] == pytest.approx(round(rig.sim.closed_deal(stray)["pnl"], 2))
        rig.run(NOW + dt.timedelta(minutes=11), rig.L)                       # the next read: sent once, read back, kept
        assert rig.broker.sends == 2 and rig.e.open and len(rig.mine()) == 1 and rig.e.open["XAUUSD"].ticket != stray
        assert n_rows(rig.e) == 2
