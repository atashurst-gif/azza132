"""Crowd Fader: the positioning read, the client, the paper engine."""
import datetime as dt
import io
import json
import os
import urllib.error

import pytest

from mintel.broker.base import Bar, Side, TF, Tick
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
        assert "/live/liquidation-heatmap/xyz:GOLD?" in seen[1].full_url and "range=3.0" in seen[1].full_url

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
