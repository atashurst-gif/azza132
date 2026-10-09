"""Financial Ian's test on past CME data (mintel/ian/research/historical.py and backtest.py).

Cost first (nothing bought without a typed yes, or --yes with a --max-cost at or above the quote; over the
limit it stops; a bad key, no billing and an unavailable dataset are said plainly); day-chunk downloads
that are never bought twice; the live adapter's own record mapping; the REAL engine replayed
deterministically with no look-ahead; P&L in spot pips with the inversions (6J, 6C, 6S); stops never
filled better than the stop; the report's fields and the verdict's thresholds; the launchers.

Everything here is offline: a mocked Databento client and small fixtures built from the synthetic
scenario generator - SYNTHETIC - NOT PERFORMANCE, and every output built from them says so.
"""
import dataclasses
import datetime as dt
import heapq
import json
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from mintel.broker.base import Side
from mintel.ian import SYNTHETIC_LABEL
from mintel.ian.book import ADD, ASK, BID, BUY, CANCEL, CLEAR, MODIFY, SELL, BookEvent, OrderBook, TradeEvent, received_at
from mintel.ian.engine import IanConfig
from mintel.ian.feeds import databento as live_adapter
from mintel.ian.feeds.synthetic import SyntheticFeed
from mintel.ian.research import backtest as bt
from mintel.ian.research import historical as hist

UTC = dt.timezone.utc
ROOT = Path(__file__).resolve().parents[1]
DAY = dt.date(2026, 10, 7)                       # a Wednesday: the synthetic scenarios start at 12:00 UTC
NOW = dt.datetime(2026, 10, 8, 6, 0, tzinfo=UTC)  # the next morning: 7 Oct is the last complete weekday
KEY = "db-TESTKEY-never-to-be-printed-123"


# ------------------------------------------------------------------ fixtures --

def write_secrets(data_dir: Path, **extra) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "secrets.json").write_text(json.dumps({"databento_api_key": KEY, **extra}))


class HttpError(Exception):
    """Shaped like databento.common.error.BentoClientError (http_status, json_body with detail.case)."""

    def __init__(self, status, case="", message=""):
        super().__init__(f"{status} {message}")
        self.http_status = status
        self.json_body = {"detail": {"case": case, "message": message, "docs": None}}
        self.message = message


class FakeClient:
    """The parts of databento.Historical this tool uses, recording every call."""

    fixture = True                               # its files are fixtures: never recorded as real data

    def __init__(self, cost=1.25, size=150_000_000, files=None, fail=None, rng=None, cut_off=(), serve=None,
                 fail_on_call=None):
        self.calls = []
        self.cost, self.size = cost, size
        self.files = files or {}                 # (day, schema) -> bytes
        self.serve = serve                       # kw -> bytes: what Databento would send for exactly this request
        self.fail_on_call = fail_on_call or {}   # (method, n) -> exception raised on that method's n-th call
        self.fail = fail or {}                   # method name -> exception
        self.cut_off = set(cut_off)              # (day, schema): the stream breaks half way
        self.rng = rng or {"start": "2010-06-06T00:00:00.000000000Z", "end": "2026-10-08T00:00:00.000000000Z",
                           "schema": {"mbp-10": {"start": "2017-05-21T00:00:00.000000000Z",
                                                 "end": "2026-10-08T00:00:00.000000000Z"}}}
        outer = self

        class Meta:
            def get_dataset_range(self, dataset):
                outer.calls.append(("range", dataset))
                outer._maybe_fail("get_dataset_range")
                return outer.rng

            def get_cost(self, **kw):
                outer.calls.append(("cost", kw))
                outer._maybe_fail("get_cost")
                return outer.cost(kw) if callable(outer.cost) else outer.cost

            def get_billable_size(self, **kw):
                outer.calls.append(("size", kw))
                outer._maybe_fail("get_billable_size")
                return outer.size

        class Series:
            def get_range(self, **kw):
                outer.calls.append(("get_range", kw))
                outer._maybe_fail("get_range")
                day = dt.date.fromisoformat(kw["start"][:10])
                with open(kw["path"], "xb") as fh:          # the real client opens with "x+b" too
                    data = outer.serve(kw) if outer.serve is not None else \
                        outer.files.get((day, kw["schema"]), b"\x28\xb5\x2f\xfd fixture bytes")
                    fh.write(data)
                    if (day, kw["schema"]) in outer.cut_off:
                        raise ConnectionError("connection reset half way")
                return None

        self.metadata, self.timeseries = Meta(), Series()

    def _maybe_fail(self, name):
        if name in self.fail:
            raise self.fail[name]
        kind = {"get_dataset_range": "range", "get_cost": "cost", "get_billable_size": "size"}.get(name, name)
        exc = self.fail_on_call.get((name, self.n(kind)))
        if exc is not None:
            raise exc

    def n(self, kind):
        return sum(1 for c in self.calls if c[0] == kind)


def run(tmp_path, *args, client=None, answer="n", tty=True, now=NOW, uploader=None, factory=None):
    """The CLI with a mocked client; returns (exit code, printed text, client)."""
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    out = []
    client = client if client is not None else FakeClient()
    asked = []

    def ask(prompt):
        asked.append(prompt)
        out.append(prompt)
        return answer

    code = hist.main(["--config", str(data / "config.json"), *args],
                     client_factory=factory or (lambda key: client), ask=ask, isatty=lambda: tty, now=now,
                     say=lambda *a: out.append(" ".join(str(x) for x in a)), uploader=uploader,
                     getpass_fn=lambda prompt: KEY)
    text = "\n".join(out)
    assert KEY not in text, "the API key must never be printed"
    return code, text, client, asked


SCENARIOS = {"6EZ6": "sweep_up", "6JZ6": "breakout", "6BZ6": "trend_up", "6CZ6": "trend_down"}


def fixture_events(scenarios=None, seed=7, start=None):
    scenarios = scenarios or SCENARIOS
    feed = SyntheticFeed("balanced", instruments=list(scenarios), scenarios=scenarios, seed=seed, start=start)
    return feed.all_events


def shifted(ev, by):
    """The same event ``by`` later (every timestamp moved)."""
    return dataclasses.replace(ev, **{f.name: getattr(ev, f.name) + by for f in dataclasses.fields(ev)
                                      if isinstance(getattr(ev, f.name), dt.datetime)})


def window_for(events, pad_min=1):
    first, last = received_at(events[0]), received_at(events[-1])
    return [bt.Window(first.replace(minute=0, second=0, microsecond=0), last + dt.timedelta(minutes=pad_min),
                      first.date().isoformat(), {})]


def backtest(tmp_path, events=None, name="run", cfg=None, costs=None):
    events = events if events is not None else fixture_events()
    res = bt.run_backtest(bt.event_batches(events), window_for(events), cfg or IanConfig(), costs or bt.CostModel(),
                          tmp_path / name, synthetic=True, sequence_scope="instrument")
    return res


# real DBN files (the record classes Databento's own package decodes), built from the synthetic events

def _px(p):
    return int(round(float(p) * 1e9))


def dbn_bytes(events, schema, day, ids, keep=None):
    """``keep(instrument, ts_recv)``: only those records - a piece of the day, as Databento sends one for a
    request of fewer contracts or hours (the records themselves are the same)."""
    d = pytest.importorskip("databento_dbn")
    zstd = pytest.importorskip("zstandard")
    start = bt._ns(dt.datetime(day.year, day.month, day.day, tzinfo=UTC))
    mappings = [NS(raw_symbol=raw, intervals=[NS(start_date=day, end_date=day + dt.timedelta(days=1), symbol=str(i))])
                for raw, i in ids.items()]
    md = d.Metadata("GLBX.MDP3", start, d.SType.RAW_SYMBOL, d.SType.INSTRUMENT_ID,
                    d.Schema.MBP_10 if schema == "mbp-10" else d.Schema.TRADES, symbols=list(ids), partial=[],
                    not_found=[], mappings=mappings, end=start + 86_400 * 10 ** 9)
    books, recs = {}, []
    acts = {ADD: d.Action.ADD, MODIFY: d.Action.MODIFY, CANCEL: d.Action.CANCEL, CLEAR: d.Action.CLEAR}

    def levels(b):
        bids, asks = b.levels(BID, 10), b.levels(ASK, 10)
        out = []
        for k in range(10):
            bp, bs = bids[k] if k < len(bids) else (None, 0)
            ap, az = asks[k] if k < len(asks) else (None, 0)
            out.append(d.BidAskPair(bid_px=_px(bp) if bp else d.UNDEF_PRICE, ask_px=_px(ap) if ap else d.UNDEF_PRICE,
                                    bid_sz=int(bs), ask_sz=int(az)))
        return out

    for ev in sorted(events, key=received_at):
        inst = ev.instrument
        b = books.setdefault(inst, OrderBook(inst))
        wanted = keep is None or keep(inst, received_at(ev))
        if isinstance(ev, BookEvent):
            b.apply(ev)
            if schema == "mbp-10" and wanted:
                side = d.Side.BID if ev.side == BID else d.Side.ASK if ev.side == ASK else d.Side.NONE
                recs.append(d.MBP10Msg(1, ids[inst], bt._ns(ev.ts_exchange), _px(ev.price) if ev.price else d.UNDEF_PRICE,
                                       int(ev.size), acts[ev.action], side, 0, bt._ns(ev.ts_received), flags=0,
                                       sequence=int(ev.sequence), levels=levels(b)))
        elif isinstance(ev, TradeEvent) and wanted:
            side = d.Side.BID if ev.aggressor == BUY else d.Side.ASK if ev.aggressor == SELL else d.Side.NONE
            args = (1, ids[inst], bt._ns(ev.ts), _px(ev.price), max(1, int(round(ev.size))), d.Action.TRADE, side, 0,
                    bt._ns(ev.received))
            if schema == "trades":
                recs.append(d.TradeMsg(*args, flags=0, sequence=int(ev.sequence)))
            else:                                # mbp-10 carries the trades as well
                recs.append(d.MBP10Msg(*args, flags=0, sequence=int(ev.sequence), levels=levels(b)))
    raw = md.encode() + b"".join(bytes(r) for r in recs)
    return zstd.ZstdCompressor().compress(raw), len(recs)


def fixture_files(day=DAY, scenarios=None):
    evs = fixture_events(scenarios)
    insts = sorted({e.instrument for e in evs})
    ids = {raw: 4000 + i for i, raw in enumerate(insts)}
    mbp, n_mbp = dbn_bytes(evs, "mbp-10", day, ids)
    trd, n_trd = dbn_bytes(evs, "trades", day, ids)
    return {(day, "mbp-10"): mbp, (day, "trades"): trd}, evs, ids


class FixtureServer:
    """Serves exactly what a request asks for - its contracts and its hours - from fixture events, as DBN."""

    def __init__(self, events, day=DAY):
        self.events, self.day = events, day
        self.ids = {raw: 4000 + i for i, raw in enumerate(sorted({e.instrument for e in events}))}

    def __call__(self, kw):
        t0, t1 = (dt.datetime.fromisoformat(kw[k]).replace(tzinfo=UTC) for k in ("start", "end"))
        want = set(kw["symbols"])
        raw, _ = dbn_bytes(self.events, kw["schema"], self.day, self.ids,
                           keep=lambda inst, ts: inst in want and t0 <= ts < t1)
        return raw


# ---------------------------------------------------------------- cost first --

class TestCostFirst:
    def test_the_range_is_shown_first_then_the_cost_in_plain_english_and_no_is_no(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, asked = run(tmp_path, answer="n")
        assert code == hist.DECLINED
        assert c.n("get_range") == 0, "nothing may be downloaded without a yes"
        assert text.index("Databento has CME Globex") < text.index("Asking Databento what this costs")
        days, schemas = 5, 2
        assert c.n("cost") == days * schemas and c.n("size") == days * schemas
        assert (f"This test will download about {hist.fmt_bytes(150_000_000 * days * schemas)} of CME order-book data "
                f"for {days} days and Databento will charge about ${1.25 * days * schemas:,.2f}.") in text
        assert asked == ["Go ahead? (y/N) "]
        assert "does not report account credit, so none is assumed" in text
        assert "A cheaper first look: --days 2" in text and "$5.00" in text
        assert "nothing was downloaded and nothing was charged" in text
        assert not list((tmp_path / "data").rglob("*.dbn.zst"))

    @pytest.mark.parametrize("answer", ["", "no", "N", "yes please", "maybe"])
    def test_anything_but_y_or_yes_buys_nothing(self, tmp_path, answer):
        write_secrets(tmp_path / "data")
        code, _, c, _ = run(tmp_path, "--days", "1", answer=answer)
        assert code == hist.DECLINED and c.n("get_range") == 0

    @pytest.mark.parametrize("answer", ["y", "Y", "yes", " YES "])
    def test_a_typed_yes_buys_each_day_and_schema_once(self, tmp_path, answer):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--days", "2", "--download-only", answer=answer)
        assert code == hist.OK, text
        assert c.n("get_range") == 4
        for call in [k for k in c.calls if k[0] == "get_range"]:
            kw = call[1]
            assert kw["dataset"] == "GLBX.MDP3" and kw["stype_in"] == "raw_symbol"
            assert kw["start"].endswith("T07:00:00") and kw["end"].endswith("T21:00:00")
            assert sorted(kw["symbols"]) == sorted(["6EZ6", "6BZ6", "6AZ6", "6NZ6", "6CZ6", "6JZ6", "6SZ6"])
        assert "[1/4]" in text and "[4/4]" in text

    def test_yes_needs_max_cost_and_nothing_is_asked_of_databento(self, tmp_path):
        write_secrets(tmp_path / "data")
        made = []
        code, text, _, _ = run(tmp_path, "--yes", factory=lambda key: made.append(key) or FakeClient())
        assert code == hist.DECLINED and made == []
        assert "--yes needs --max-cost" in text

    def test_yes_with_a_max_cost_at_the_quote_buys_without_asking(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, asked = run(tmp_path, "--days", "2", "--yes", "--max-cost", "5.00", "--download-only")
        assert code == hist.OK, text
        assert asked == [] and c.n("get_range") == 4
        assert "within your limit" in text

    def test_over_max_cost_stops_and_says_so(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, asked = run(tmp_path, "--days", "2", "--yes", "--max-cost", "4.99")
        assert code == hist.DECLINED and c.n("get_range") == 0 and asked == []
        assert "STOPPED" in text and "over your limit" in text and "$5.00" in text and "$4.99" in text

    def test_a_max_cost_also_stops_an_interactive_run_before_asking(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, asked = run(tmp_path, "--days", "2", "--max-cost", "1", answer="y")
        assert code == hist.DECLINED and c.n("get_range") == 0 and asked == []

    def test_no_terminal_and_no_yes_buys_nothing(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, asked = run(tmp_path, "--days", "1", tty=False, answer="y")
        assert code == hist.DECLINED and c.n("get_range") == 0 and asked == []
        assert "no terminal" in text

    def test_no_key_says_how_to_set_it_and_never_calls_databento(self, tmp_path):
        made = []
        code, text, _, _ = run(tmp_path, factory=lambda key: made.append(key) or FakeClient())
        assert code == hist.SETUP_ERROR and made == [] and "--set-key" in text

    def test_set_key_is_hidden_saved_kept_private_and_does_not_switch_the_live_feed(self, tmp_path):
        data = tmp_path / "data"
        data.mkdir(parents=True)
        (data / "secrets.json").write_text(json.dumps({"github_token": "gh-keep"}))
        (data / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "none"}}))
        code, text, _, _ = run(tmp_path, "--set-key")
        assert code == hist.OK
        s = json.loads((data / "secrets.json").read_text())
        assert s == {"github_token": "gh-keep", "databento_api_key": KEY}
        assert oct((data / "secrets.json").stat().st_mode)[-3:] == "600"
        assert json.loads((data / "ian.json").read_text())["feed"] == {"vendor": "none"}
        assert "does NOT switch Financial Ian's live feed on" in text

    @pytest.mark.parametrize("status,case,words", [
        (401, "auth_invalid_api_key", "did not accept the API key"),
        (402, "payment_required", "will not bill this request"),
        (403, "license_not_entitled", "not allowed this data"),
        (422, "dataset_unavailable", "dataset is not available"),
    ])
    def test_plain_errors(self, tmp_path, status, case, words):
        for where in ("get_dataset_range", "get_cost", "get_range"):
            write_secrets(tmp_path / where / "data")
            c = FakeClient(fail={where: HttpError(status, case, "refused")})
            code, text, _, _ = run(tmp_path / where, "--days", "1", client=c, answer="y")
            assert code == hist.SETUP_ERROR, (where, text)
            assert words in text, (where, text)
            assert c.n("get_range") <= 1
            assert not list((tmp_path / where).rglob("*.dbn.zst"))

    def test_the_key_is_never_printed_even_when_the_server_repeats_it(self, tmp_path):
        write_secrets(tmp_path / "data")
        c = FakeClient(fail={"get_dataset_range": HttpError(401, "", f"API key {KEY} is not valid")})
        code, text, _, _ = run(tmp_path, client=c)            # run() fails if the key appears anywhere
        assert code == hist.SETUP_ERROR and "<API key hidden>" in text

    def test_offline_with_download_only_is_refused(self, tmp_path):
        code, text, _, _ = run(tmp_path, "--offline", "--download-only")
        assert code == hist.DECLINED and "do nothing" in text

    def test_an_unreachable_databento_is_said_plainly(self, tmp_path):
        write_secrets(tmp_path / "data")
        c = FakeClient(fail={"get_dataset_range": ConnectionError("no route to host")})
        code, text, _, _ = run(tmp_path, client=c)
        assert code == hist.SETUP_ERROR and "could not be reached" in text

    def test_dates_outside_the_range_are_refused_before_any_quote(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--start", "2026-10-07", "--end", "2026-10-09")
        assert code == hist.DECLINED and c.n("cost") == 0 and c.n("get_range") == 0
        assert "2026-10-08" in text and "outside what Databento has" in text
        code, text, c, _ = run(tmp_path, "--start", "2009-01-05", "--end", "2009-01-06")
        assert code == hist.DECLINED and c.n("cost") == 0

    def test_the_default_days_are_the_last_complete_weekdays_inside_the_range(self):
        fri = dt.datetime(2026, 10, 9, 22, 0, tzinfo=UTC)
        assert hist.last_complete_weekdays(5, "london_ny", fri) == [dt.date(2026, 10, d) for d in (5, 6, 7, 8, 9)]
        assert hist.last_complete_weekdays(5, "london_ny", fri.replace(hour=20))[-1] == dt.date(2026, 10, 8)
        assert hist.last_complete_weekdays(2, "london_ny", fri, latest=dt.datetime(2026, 10, 8, tzinfo=UTC)) == \
            [dt.date(2026, 10, 6), dt.date(2026, 10, 7)]
        assert hist.window(dt.date(2026, 10, 7), "all")[1] == dt.datetime(2026, 10, 8, tzinfo=UTC)

    def test_the_size_budget_stops_a_big_request(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--max-gb", "1", answer="y")
        assert code == hist.DECLINED and c.n("get_range") == 0 and "size budget" in text

    def test_the_contracts_are_ians_own_front_months_and_roll_with_it(self, tmp_path):
        write_secrets(tmp_path / "data")
        # 6E December 2026: last trade Mon 14 Dec, roll 8 days before -> Sun 6 Dec; Fri 4 Dec is still 6EZ6
        c = FakeClient(rng={"start": "2010-06-06", "end": "2026-12-12T00:00:00Z"})
        code, text, c, _ = run(tmp_path, "--start", "2026-12-04", "--end", "2026-12-07", client=c)
        syms = {k[1]["start"][:10]: k[1]["symbols"] for k in c.calls if k[0] == "cost" and k[1]["schema"] == "mbp-10"}
        assert "6EZ6" in syms["2026-12-04"] and "6EH7" in syms["2026-12-07"]


# -------------------------------------------------------------------- resume --

class TestResume:
    def test_a_day_on_disk_is_never_quoted_downloaded_or_paid_for_again(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--days", "2", "--yes", "--max-cost", "10", "--download-only")
        assert code == hist.OK and c.n("get_range") == 4
        files = sorted((tmp_path / "data" / "ian-research" / "history").rglob("*.dbn.zst"))
        assert [f.parent.name + "/" + f.name for f in files] == [
            "2026-10-06/mbp-10.dbn.zst", "2026-10-06/trades.dbn.zst", "2026-10-07/mbp-10.dbn.zst",
            "2026-10-07/trades.dbn.zst"]
        m = json.loads((files[0].parent / "manifest.json").read_text())
        e = m["files"]["mbp-10.dbn.zst"]
        assert e["schema"] == "mbp-10" and e["quoted_cost_usd"] == 1.25 and m["synthetic"] is True  # a fixture client
        assert sorted(e["symbols"]) == sorted(["6EZ6", "6BZ6", "6AZ6", "6NZ6", "6CZ6", "6JZ6", "6SZ6"])
        assert KEY not in json.dumps(m)
        code, text, c2, asked = run(tmp_path, "--days", "2", "--download-only", answer="y")
        assert code == hist.OK and c2.n("get_range") == 0 and c2.n("cost") == 0 and asked == []
        assert "Every day is already on disk: nothing to download, nothing to pay" in text
        # one more day: only that day is quoted and bought
        code, text, c3, _ = run(tmp_path, "--days", "3", "--download-only", answer="y")
        assert c3.n("get_range") == 2 and {k[1]["start"][:10] for k in c3.calls if k[0] == "cost"} == {"2026-10-05"}
        assert "already on disk and cost nothing" in text

    def test_a_download_cut_off_half_way_leaves_nothing_that_looks_complete(self, tmp_path):
        write_secrets(tmp_path / "data")
        c = FakeClient(cut_off={(DAY, "trades")})
        code, text, c, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only", client=c)
        assert code == hist.SETUP_ERROR and "could not be reached" in text and "stays on disk" in text
        assert "was paid for" in text
        d = tmp_path / "data" / "ian-research" / "history" / DAY.isoformat()
        assert (d / "mbp-10.dbn.zst").exists()
        assert not (d / "trades.dbn.zst").exists() and not (d / "trades.dbn.zst.part").exists()
        code, text, c2, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only")
        assert code == hist.OK and [k[1]["schema"] for k in c2.calls if k[0] == "get_range"] == ["trades"]

    def test_more_hours_for_a_day_on_disk_buy_only_the_missing_hours(self, tmp_path):
        write_secrets(tmp_path / "data")
        run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only")
        code, text, c, asked = run(tmp_path, "--days", "1", "--hours", "all", "--download-only", answer="y",
                                   now=dt.datetime(2026, 10, 8, 6, 0, tzinfo=UTC))
        # 07:00-21:00 is on disk: only 00:00-07:00 and 21:00-24:00 are quoted and bought, for both schemas
        assert code == hist.OK, text
        spans = sorted((k[1]["schema"], k[1]["start"][11:16], k[1]["end"]) for k in c.calls if k[0] == "get_range")
        assert spans == [("mbp-10", "00:00", "2026-10-07T07:00:00"), ("mbp-10", "21:00", "2026-10-08T00:00:00"),
                         ("trades", "00:00", "2026-10-07T07:00:00"), ("trades", "21:00", "2026-10-08T00:00:00")]
        assert sorted((k[1]["schema"], k[1]["start"][11:16]) for k in c.calls if k[0] == "cost") == \
            [(s, h) for s in ("mbp-10", "trades") for h in ("00:00", "21:00")]
        assert ("Part of 2026-10-07 is already on disk (not bought again); only what is missing is bought: "
                "mbp-10 00:00-07:00 UTC; mbp-10 21:00-24:00 UTC; trades 00:00-07:00 UTC; trades 21:00-24:00 UTC") in text
        assert "Databento will charge about $5.00" in text and asked == ["Go ahead? (y/N) "]
        d = tmp_path / "data" / "ian-research" / "history" / DAY.isoformat()
        assert sorted(f.name for f in d.glob("*.dbn.zst")) == [
            "mbp-10.2.dbn.zst", "mbp-10.3.dbn.zst", "mbp-10.dbn.zst", "trades.2.dbn.zst", "trades.3.dbn.zst",
            "trades.dbn.zst"]
        # and now the whole day is on disk, for either window
        for hours in ("all", "london_ny"):
            code, text, c, _ = run(tmp_path, "--days", "1", "--hours", hours, "--download-only", answer="y")
            assert code == hist.OK and c.n("get_range") == 0 and c.n("cost") == 0, (hours, text)


# ---------------------------------------------------------- record mapping --

class TestRecordMapping:
    def test_the_past_data_goes_through_the_live_adapters_own_mapper(self):
        assert bt.RecordMapper is live_adapter.RecordMapper

    def test_a_past_record_becomes_exactly_the_event_a_live_one_does(self, tmp_path):
        db = pytest.importorskip("databento")
        files, evs, ids = fixture_files()
        d = tmp_path / "day"
        d.mkdir()
        (d / "mbp-10.dbn.zst").write_bytes(files[(DAY, "mbp-10")])
        st = bt.DayStats(DAY.isoformat())
        s, e = hist.window(DAY, "all")
        past = [ev for batch in bt.dbn_day_batches(d, s, e, st, use_trades=False) for ev in batch]
        # the same subscription live: mbp-10 alone (the trades inside it are the trades)
        live = live_adapter.DatabentoFeed(tmp_path, api_key=KEY, client_factory=lambda **k: None, trades=False)
        live.mapper.symbols.update({i: raw for raw, i in ids.items()})
        for rec in db.DBNStore.from_file(d / "mbp-10.dbn.zst"):
            live._on_record(rec)
        assert past and past == live.events(10 ** 7)

    def test_trades_are_taken_once_from_the_trades_file(self, tmp_path):
        pytest.importorskip("databento")
        files, evs, ids = fixture_files()
        d = tmp_path / "day"
        d.mkdir()
        for (_, schema), raw in files.items():
            (d / f"{schema}.dbn.zst").write_bytes(raw)
        s, e = hist.window(DAY, "all")
        st = bt.DayStats(DAY.isoformat())
        out = [ev for b in bt.dbn_day_batches(d, s, e, st) for ev in b]
        n_trades = sum(1 for ev in evs if isinstance(ev, TradeEvent))
        assert sum(1 for ev in out if isinstance(ev, TradeEvent)) == n_trades == st.trade_records
        assert st.mbp_trade_records == n_trades          # the same trades inside mbp-10, not used again
        # merged in capture-time order
        times = [received_at(ev) for ev in out]
        assert times == sorted(times)
        # the London/New York window drops what is outside it
        st2 = bt.DayStats(DAY.isoformat())
        early = [ev for b in bt.dbn_day_batches(d, s, dt.datetime(2026, 10, 7, 12, 1, tzinfo=UTC), st2) for ev in b]
        assert early and all(received_at(ev) < dt.datetime(2026, 10, 7, 12, 1, tzinfo=UTC) for ev in early)
        assert st2.outside_window > 0


# ----------------------------------------------------------------- backtest --

def signals_of(res_dir):
    import sqlite3
    con = sqlite3.connect(str(res_dir / "ian.sqlite"))
    try:
        return [tuple(r) for r in con.execute("SELECT signal_id, ts_utc, trigger_ts, direction, score, status FROM signals "
                                              "ORDER BY ts_utc, signal_id")]
    finally:
        con.close()


class TestBacktest:
    def test_the_real_engine_trades_the_fixture_and_it_is_labelled_synthetic(self, tmp_path):
        res = backtest(tmp_path)
        assert res.label == SYNTHETIC_LABEL and res.synthetic
        assert res.trades, "the fixture should produce trades"
        assert all(t["data_label"] == SYNTHETIC_LABEL for t in res.trades)
        assert res.events_in == len(fixture_events())
        assert {t["pair"] for t in res.trades} <= {"EURUSD", "USDJPY", "GBPUSD", "USDCAD"}
        s = bt.summarise(res, {"period": {"first": "2026-10-07", "last": "2026-10-07"}})
        assert s["verdict"] == SYNTHETIC_LABEL and "means nothing about the real market" in s["verdict_reasons"][0]
        md = bt.report_md(s)
        assert md.startswith(f"# Financial Ian - test on past CME data ({SYNTHETIC_LABEL})")
        assert f"**{SYNTHETIC_LABEL}.**" in md and bt.REAL_LABEL not in md

    def test_the_same_data_gives_the_same_trades(self, tmp_path):
        a, b = backtest(tmp_path, name="a"), backtest(tmp_path, name="b")
        assert a.trades == b.trades and a.signals == b.signals and a.quality == b.quality
        assert signals_of(tmp_path / "a") == signals_of(tmp_path / "b")

    def test_no_decision_sees_its_future(self, tmp_path):
        evs = fixture_events()
        full = backtest(tmp_path, evs, "full")
        sig_times = sorted(dt.datetime.fromisoformat(r[1]) for r in signals_of(tmp_path / "full"))
        assert len(sig_times) >= 2, "the fixture should signal"
        cut = sig_times[1] + dt.timedelta(seconds=10)
        assert cut < received_at(evs[-1]) - dt.timedelta(seconds=30)    # a real future is removed
        # the future after the cut replaced by something else entirely (here: nothing)
        part = backtest(tmp_path, [e for e in evs if received_at(e) < cut], "part")
        margin = cut - dt.timedelta(seconds=2)
        # what was decided before the cut: each signal as it was first produced (its final status can come
        # later - the same signal id is offered again within its minute) and each trade decided before it
        before = lambda rows: [r[:5] for r in rows if r[1] < margin.isoformat()]
        assert before(signals_of(tmp_path / "full")), "the fixture should signal before the cut"
        assert before(signals_of(tmp_path / "full")) == before(signals_of(tmp_path / "part"))
        key = lambda t: (t["ticket"], t["signal_id"], t["decided_utc"], t["filled_utc"], t["entry"], t["initial_stop"])
        f = [key(t) for t in full.trades if t["decided_utc"] < margin.isoformat()]
        p = [key(t) for t in part.trades if t["decided_utc"] < margin.isoformat()]
        assert f and f == p

    def test_quotes_given_to_decisions_are_always_from_before_them(self):
        evs = fixture_events({"6EZ6": "sweep_up", "6JZ6": "breakout"})
        m = bt.Market(bt.event_batches(evs), ["6E", "6J"], bt.CostModel())
        seen = 0
        t = received_at(evs[0])
        while True:
            ev = m.next_event()
            if ev is None:
                break
            if received_at(ev) > t + dt.timedelta(milliseconds=500):
                t = received_at(ev)
                m.advance(t)
                for q in m.cur.values():
                    assert q.t < t
                    seen += 1
                m.quote_at("6E", t + dt.timedelta(seconds=1))     # reading ahead for a fill changes nothing released
                assert all(q.t < t for q in m.cur.values())
        assert seen > 50

    def test_every_trade_is_booked_after_costs_in_ians_own_records(self, tmp_path):
        import sqlite3
        res = backtest(tmp_path)
        con = sqlite3.connect(str(tmp_path / "run" / "ian.sqlite"))
        rows = {r[0]: r[1] for r in con.execute("SELECT ticket, net_pnl FROM trades")}
        con.close()
        for t in res.trades:
            assert rows[t["ticket"]] == pytest.approx(round(t["net_gbp"], 2))
            assert t["net_gbp"] < t["gross_pips"] * t["gbp_per_pip"] + 1e-9      # commission is always paid
            assert t["cost_pips"] > 0

    def test_a_trade_still_open_when_the_data_ends_is_closed_at_the_last_price(self, tmp_path):
        evs = fixture_events({"6EZ6": "trend_up", "6BZ6": "trend_up", "6AZ6": "balanced"})
        res = backtest(tmp_path, evs)
        last = received_at(evs[-1])
        assert all(dt.datetime.fromisoformat(t["closed_utc"]) <= last + dt.timedelta(minutes=2) for t in res.trades)
        if res.end_of_data_closes:
            assert any(t["exit_reason"] == "END_OF_DATA" for t in res.trades)


# -------------------------------------------------------------------- P&L --

def book_events(inst, t, bid, ask, tick, seq, size=50.0):
    """A fresh 5-level book with the given touch."""
    out = [BookEvent(inst, t, t, seq, "", 0.0, 0.0, CLEAR, cause="snapshot")]
    for k in range(5):
        out.append(BookEvent(inst, t, t, seq, BID, round(bid - k * tick, 9), size, ADD, cause="snapshot"))
        out.append(BookEvent(inst, t, t, seq, ASK, round(ask + k * tick, 9), size, ADD, cause="snapshot"))
    return out


def trade_once(root, inst, side, f0, f1, tick, costs=None):
    """Open at a futures touch of f0 and close (at market) once the futures touch is f1."""
    t0 = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
    t1 = t0 + dt.timedelta(seconds=30)
    batches = [book_events(inst, t0, f0, round(f0 + tick, 9), tick, 1),
               book_events(inst, t1, f1, round(f1 + tick, 9), tick, 2)]
    m = bt.Market(iter(batches), [root, "6B"], costs or bt.CostModel(spread_follows_futures=False))
    ex = bt.BacktestExecutor(m, m.costs)
    m.next_event()
    m.advance(t0 + dt.timedelta(seconds=1))
    sym = bt.CONTRACTS[root].spot
    tick0 = bt.SpotBroker(m).tick(sym)
    stop = tick0.mid - side.sign * 1000 * bt.pip_size(sym)              # far away: never hit
    fill = ex.open(sym, side, 1.0, stop, 0.0, tick0, m.spec(sym), m.now)
    assert fill.ok
    while m.next_event() is not None:
        pass
    m.advance(t1 + dt.timedelta(seconds=1))
    out = ex.close(fill.ticket, bt.SpotBroker(m).tick(sym), m.spec(sym))
    assert out.ok
    return ex.closed[-1]


class TestPnLAndInversions:
    @pytest.mark.parametrize("root,inst,f0,f1,tick,pair_move_pips", [
        ("6E", "6EZ6", 1.08500, 1.08600, 0.00005, +10.0),             # the euro up: EURUSD up 10 pips
        ("6J", "6JZ6", 1 / 150.0, 1 / 149.0, 0.0000005, -100.0),      # the yen up (6J up): USDJPY 150 -> 149
        ("6C", "6CZ6", 1 / 1.3700, 1 / 1.3650, 0.00005, -50.0),       # the Canadian dollar up: USDCAD down
        ("6S", "6SZ6", 1 / 0.9000, 1 / 0.8950, 0.00005, -50.0),       # the franc up: USDCHF down
    ])
    def test_a_futures_move_is_the_right_spot_move_in_pips(self, root, inst, f0, f1, tick, pair_move_pips):
        for side in (Side.BUY, Side.SELL):
            p = trade_once(root, inst, side, f0, f1, tick)
            expect = side.sign * pair_move_pips
            assert p.result["mid_pips"] == pytest.approx(expect, abs=0.6)           # half a futures tick of rounding
            assert p.result["net_pips"] < p.result["mid_pips"]                      # costs always go against
            assert (p.result["net_pips"] > 0) == (expect > 0)

    def test_pips_are_turned_into_pounds_at_the_replays_own_rates(self):
        p = trade_once("6J", "6JZ6", Side.SELL, 1 / 150.0, 1 / 149.0, 0.0000005)
        # no 6B in this market: the reference GBPUSD is used, and counted
        assert p.gbp_per_pip == pytest.approx(0.01 * 100_000 / (bt.REFERENCE_RATES["GBPUSD"] * 149.0), rel=0.01)
        assert p.result["net_gbp"] == pytest.approx(p.result["net_pips"] * p.gbp_per_pip, rel=1e-9)

    def test_the_costs_are_spread_slippage_and_commission(self):
        costs = bt.CostModel(spread_follows_futures=False, slippage_pips=0.1, commission_usd_per_lot_side=3.5,
                             latency_ms=0)
        p = trade_once("6E", "6EZ6", Side.BUY, 1.08500, 1.08500, 0.00005, costs)
        assert p.result["price_cost_pips"] == pytest.approx(0.12 + 2 * 0.1, abs=1e-6)     # one spread, two slips
        comm_gbp = 2 * 3.5 / bt.REFERENCE_RATES["GBPUSD"]
        assert p.result["commission_gbp"] == pytest.approx(comm_gbp, rel=1e-9)
        assert p.result["net_pips"] == pytest.approx(-(0.32 + comm_gbp / p.gbp_per_pip), abs=1e-6)


class TestStops:
    def _market_with_path(self, prices, root="6E", inst="6EZ6", tick=0.00005):
        t0 = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        batches = [book_events(inst, t0 + dt.timedelta(seconds=i), p, round(p + tick, 9), tick, i + 1)
                   for i, p in enumerate(prices)]
        m = bt.Market(iter(batches), [root], bt.CostModel(spread_follows_futures=False, latency_ms=0))
        return m, bt.BacktestExecutor(m, m.costs), t0

    @pytest.mark.parametrize("seed", range(25))
    def test_a_stop_is_never_filled_better_than_the_stop(self, seed):
        import random
        rnd = random.Random(seed)
        path = [1.0850]
        for _ in range(200):
            jump = rnd.choice([0, 0, 1, -1, 2, -2, 8, -8]) * 0.00005           # with gaps through the stop
            path.append(round(path[-1] + jump, 5))
        for side in (Side.BUY, Side.SELL):
            m, ex, t0 = self._market_with_path(path)
            m.next_event()
            m.advance(t0 + dt.timedelta(milliseconds=500))
            tick = bt.SpotBroker(m).tick("EURUSD")
            stop = round(tick.mid - side.sign * 0.0006, 5)
            f = ex.open("EURUSD", side, 0.1, stop, 0.0, tick, m.spec("EURUSD"), m.now)
            while m.next_event() is not None:
                pass
            m.advance(t0 + dt.timedelta(seconds=len(path) + 1))
            p = ex.held.get(f.ticket)
            if p is None or p.trigger is None:
                continue
            ex.close(f.ticket, bt.SpotBroker(m).tick("EURUSD"), m.spec("EURUSD"))
            done = ex.closed[-1]
            assert done.exit_kind == "STOP"
            assert (done.exit - stop) * side.sign <= 1e-12, (side, done.exit, stop)

    def test_a_gap_through_the_stop_fills_at_the_first_price_beyond_it(self):
        m, ex, t0 = self._market_with_path([1.08500, 1.08500, 1.08300])
        m.next_event()
        m.advance(t0 + dt.timedelta(milliseconds=500))
        tick = bt.SpotBroker(m).tick("EURUSD")
        f = ex.open("EURUSD", Side.BUY, 0.1, 1.08450, 0.0, tick, m.spec("EURUSD"), m.now)
        while m.next_event() is not None:
            pass
        m.advance(t0 + dt.timedelta(seconds=5))
        assert ex.stop_hit(f.ticket, bt.SpotBroker(m).tick("EURUSD"))
        ex.close(f.ticket, bt.SpotBroker(m).tick("EURUSD"), m.spec("EURUSD"))
        p = ex.closed[-1]
        assert p.exit < 1.08450 - 0.0010 and p.exit_t == t0 + dt.timedelta(seconds=2)

    def test_a_stop_is_checked_on_every_quote_not_only_at_the_engines_passes(self):
        m, ex, t0 = self._market_with_path([1.08500, 1.08400, 1.08500])          # dips through and comes back
        m.next_event()
        m.advance(t0 + dt.timedelta(milliseconds=500))
        tick = bt.SpotBroker(m).tick("EURUSD")
        f = ex.open("EURUSD", Side.BUY, 0.1, 1.08450, 0.0, tick, m.spec("EURUSD"), m.now)
        while m.next_event() is not None:
            pass
        m.advance(t0 + dt.timedelta(seconds=5))                                    # the engine looks only now
        assert ex.stop_hit(f.ticket, bt.SpotBroker(m).tick("EURUSD"))

    def test_in_the_full_replay_every_stop_exit_is_at_the_stop_or_worse(self, tmp_path):
        res = backtest(tmp_path)
        for t in res.trades:
            if t["exit_reason"] == "STOP":
                assert (t["exit"] - t["final_stop"]) * (1 if t["side"] == "BUY" else -1) <= 1e-9


# ------------------------------------------------------------------ report --

def fake_trades(spec):
    """spec: [(future, mid_pips, cost_pips)] -> trade dicts as the backtest writes them (1 GBP a pip)."""
    out = []
    for i, (fut, mid, cost) in enumerate(spec):
        net = mid - cost
        out.append({"data_label": bt.REAL_LABEL, "ticket": i, "future": fut, "regime": "TRENDING", "hour_utc": 9,
                    "exit_reason": "STOP", "mid_pips": mid, "cost_pips": cost, "gross_pips": mid - cost + 0.1,
                    "net_pips": net, "net_gbp": net, "gbp_per_pip": 1.0, "realised_r": net / 10, "mfe_r": 1.0,
                    "closed_utc": f"2026-10-07T09:{i % 60:02d}:00+00:00"})
    return out


class TestVerdict:
    def test_fewer_than_30_trades_is_insufficient_data(self):
        assert bt.verdict(29, 500.0, 400.0, 3)[0] == bt.INSUFFICIENT
        assert bt.verdict(0, 0.0, 0.0, 0)[0] == bt.INSUFFICIENT

    def test_not_positive_after_costs_is_no_edge(self):
        assert bt.verdict(30, 0.0, -10.0, 2)[0] == bt.NO_EDGE
        assert bt.verdict(80, -12.5, -40.0, 0)[0] == bt.NO_EDGE

    def test_a_profit_that_15x_costs_turn_into_a_loss_is_fragile(self):
        v, why = bt.verdict(40, 20.0, -5.0, 3)
        assert v == bt.FRAGILE and "1.5 times" in why[0]

    def test_a_profit_from_one_instrument_only_is_fragile(self):
        v, why = bt.verdict(40, 200.0, 150.0, 1)
        assert v == bt.FRAGILE and "one instrument" in why[0]

    def test_promising_needs_all_of_it(self):
        assert bt.verdict(30, 200.0, 150.0, 2)[0] == bt.PROMISING == "PROMISING - WORTH A LIVE FEED TRIAL"

    def test_the_stress_is_the_same_trades_with_every_cost_multiplied(self):
        tr = fake_trades([("6E", 3.0, 1.0)] * 10 + [("6J", 1.5, 1.0)] * 10)
        assert bt.stressed(tr, 1.0)["net_gbp"] == pytest.approx(sum(t["net_gbp"] for t in tr))
        assert bt.stressed(tr, 1.5)["net_pips"] == pytest.approx(10 * (3 - 1.5) + 10 * (1.5 - 1.5))
        assert bt.stressed(tr, 2.0)["net_pips"] == pytest.approx(10 * 1.0 + 10 * -0.5)

    def test_the_summary_and_report_carry_every_field(self, tmp_path):
        res = backtest(tmp_path)
        res.synthetic = False                             # only to check the REAL layout; data stays a fixture
        res.label = bt.REAL_LABEL
        s = bt.summarise(res, {"period": {"first": "2026-10-07", "last": "2026-10-07"}, "hours": "london_ny",
                               "hours_text": "London and New York hours only (07:00-21:00 UTC)",
                               "calendar": {"available": False}})
        for k in ("label", "source", "period", "windows", "days", "out_of_sample", "verdict", "verdict_reasons",
                  "stats", "cost_stress", "by_instrument", "by_regime", "by_hour_utc", "by_exit", "signals",
                  "events_replayed", "data_quality", "costs", "ian_settings", "caveats"):
            assert k in s, k
        for k in ("trades", "trades_per_day", "win_rate", "net_pips", "net_gbp", "profit_factor", "avg_win_gbp",
                  "avg_loss_gbp", "max_drawdown_gbp", "mfe_capture"):
            assert k in s["stats"], k
        assert [x["cost_multiple"] for x in s["cost_stress"]] == [1.0, 1.5, 2.0]
        assert s["cost_stress"][0]["net_gbp"] == pytest.approx(s["stats"]["net_gbp"], abs=0.011)
        assert set(s["data_quality"]["feed_seconds"]) <= {"LIVE", "DEGRADED", "DOWN"}
        assert sum(s["data_quality"]["feed_seconds"].values()) > 0
        assert s["verdict"] == bt.INSUFFICIENT                        # a few minutes of data: far fewer than 30
        assert "out-of-sample" in s["out_of_sample"]
        paths = bt.write_report(tmp_path / "out", s, res.trades)
        md = paths["report.md"].read_text()
        for words in ("## Verdict: INSUFFICIENT DATA", "out-of-sample", "## Cost stress", "| 1.5x |", "| 2x |",
                      "## Data quality", "## By instrument", "## By regime", "## By hour", "## What this test cannot show",
                      "No economic calendar"):
            assert words in md, words
        rows = paths["trades.csv"].read_text().splitlines()
        assert rows[0].split(",")[:4] == ["data_label", "ticket", "signal_id", "future"] and len(rows) == len(res.trades) + 1
        assert json.loads(paths["summary.json"].read_text())["verdict"] == bt.INSUFFICIENT


# ------------------------------------------------------- the whole pipeline --

class TestPipeline:
    def test_download_replay_report_and_upload_end_to_end_on_a_fixture(self, tmp_path):
        pytest.importorskip("databento")
        files, evs, ids = fixture_files()
        write_secrets(tmp_path / "data", github_token="gh-test")
        c = FakeClient(files=files)
        sent = {}

        def uploader(files_, repo, branch, token, day=""):
            sent.update(files=files_, repo=repo, branch=branch, token=token, day=day)
            return len(files_)

        code, text, c, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "2.50", "--upload", client=c,
                               uploader=uploader)
        assert code == hist.OK, text
        out = tmp_path / "data" / "ian-research" / NOW.date().isoformat()
        s = json.loads((out / "summary.json").read_text())
        # files served by a fixture client are never labelled real
        assert s["label"] == SYNTHETIC_LABEL and s["verdict"] == SYNTHETIC_LABEL
        assert f"VERDICT: {SYNTHETIC_LABEL}" in text and str(out / "report.md") in text
        assert s["events_replayed"] > 1000 and s["records"] > 1000
        assert any("not counted a second time" in x for x in s["caveats"])
        wins = [(dt.datetime.fromisoformat(w["start"]), dt.datetime.fromisoformat(w["end"])) for w in s["windows"]]
        import csv
        for t in csv.DictReader((out / "trades.csv").read_text().splitlines()):
            assert any(a <= dt.datetime.fromisoformat(t["decided_utc"]) < b for a, b in wins), t
        assert sorted(sent["files"]) == [f"reports/ian-research/{NOW.date()}/{n}" for n in
                                         ("report.md", "summary.json", "trades.csv")]
        assert sent["token"] == "gh-test" and sent["branch"] == "reports"
        assert "gh-test" not in text
        # offline: the same data again, no Databento at all, the same result
        made = []
        code, text2, _, _ = run(tmp_path, "--days", "1", "--offline", factory=lambda key: made.append(1))
        assert code == hist.OK and made == []
        s2 = json.loads((out / "summary.json").read_text())
        assert s2["stats"] == s["stats"]


# ------------------------------------------------- live and past: one stream --

def merged_records(*paths):
    """The records of these DBN files in capture order, the trades first at the same instant - the order the
    two live subscriptions deliver them in."""
    db = pytest.importorskip("databento")

    def recs(path, rank):
        for i, r in enumerate(db.DBNStore.from_file(path)):
            yield int(r.ts_recv), rank, i, r
    streams = [recs(pth, 0 if "trades" in Path(pth).name else 1) for pth in paths]
    return [x[3] for x in heapq.merge(*streams, key=lambda x: x[:3])]


class TestLiveAndPastStreams:
    def test_live_ian_with_both_subscriptions_gets_the_stream_the_backtest_replays(self, tmp_path):
        pytest.importorskip("databento")
        files, evs, ids = fixture_files()
        d = tmp_path / "day"
        d.mkdir()
        for (_, schema), raw in files.items():
            (d / f"{schema}.dbn.zst").write_bytes(raw)
        s, e = hist.window(DAY, "all")
        past = [ev for b in bt.dbn_day_batches(d, s, e, bt.DayStats("d")) for ev in b]
        # live Ian with its defaults (factory.py, the docs): mbp-10 AND trades
        live = live_adapter.DatabentoFeed(tmp_path, api_key=KEY, client_factory=lambda **k: None)
        assert live.schema == "mbp-10" and live.trades
        live.mapper.symbols.update({i: raw for raw, i in ids.items()})
        for rec in merged_records(d / "mbp-10.dbn.zst", d / "trades.dbn.zst"):
            live._on_record(rec)
        got = live.events(10 ** 8)
        n_trades = sum(1 for ev in evs if isinstance(ev, TradeEvent))
        assert sum(1 for ev in got if isinstance(ev, TradeEvent)) == n_trades, "every trade counted ONCE live"
        assert got == past, "live and the backtest must feed the engine the same stream"

    @pytest.mark.parametrize("schema,trades,counted", [("mbp-10", True, 1), ("mbp-10", False, 1), ("mbo", True, 1),
                                                       ("mbo", False, 1)])
    def test_a_trade_arriving_on_both_subscriptions_is_one_trade(self, tmp_path, schema, trades, counted):
        from mintel.ian.feeds import factory
        (tmp_path / "secrets.json").write_text(json.dumps({"databento_api_key": KEY}))
        feed = factory.build_feed({"vendor": "databento", "schema": schema, "trades": trades}, tmp_path)
        feed.mapper.symbols[42] = "6EZ6"
        ns = 1_791_374_400_000_000_000
        book = {"_type": "mbp10" if schema == "mbp-10" else "mbo", "instrument_id": 42, "ts_event": ns,
                "ts_recv": ns + 1000, "sequence": 7, "flags": 0, "action": "T", "side": "B", "price": 1_085_050_000,
                "size": 2, "order_id": 0, "levels": []}
        feed._on_record(book)
        if trades:                                  # the trades subscription delivers the same trade
            feed._on_record({"_type": "trade", "instrument_id": 42, "ts_event": ns, "ts_recv": ns + 900, "sequence": 7,
                             "price": 1_085_050_000, "size": 2, "side": "B", "action": "T"})
        assert sum(1 for ev in feed.events() if isinstance(ev, TradeEvent)) == counted
        assert live_adapter.book_trades_wanted(schema, trades) is (not trades)


# ------------------------------------------------------- only inside a window --

class TestTestWindows:
    def test_no_trade_is_opened_after_a_test_window_has_ended(self, tmp_path):
        """Day one stops a second before Ian's first trade; day two is the next day. Nothing may be decided
        in between, on the frozen last quote of day one."""
        evs = fixture_events()
        full = backtest(tmp_path, evs, "full")
        assert full.trades, "the fixture should trade"
        first = min(dt.datetime.fromisoformat(t["decided_utc"]) for t in full.trades)
        cut = first - dt.timedelta(seconds=1)
        day2 = [shifted(e, dt.timedelta(days=1)) for e in evs]
        w1 = bt.Window(received_at(evs[0]).replace(minute=0, second=0, microsecond=0), cut, "d1", {})
        w2 = bt.Window(w1.start + dt.timedelta(days=1), received_at(day2[-1]) + dt.timedelta(minutes=1), "d2", {})
        res = bt.run_backtest(bt.event_batches([e for e in evs if received_at(e) < cut] + day2), [w1, w2],
                              IanConfig(), bt.CostModel(), tmp_path / "cut", synthetic=True, sequence_scope="instrument")
        assert res.trades, "day two should still trade"
        for t in res.trades:
            decided = dt.datetime.fromisoformat(t["decided_utc"])
            assert any(w.start <= decided < w.end for w in (w1, w2)), t
            assert t["exit_reason"] != "TIME" or t["minutes"] < 60, t      # nothing held through the night


# ------------------------------------------------------- one contract at a time --

class TestContractRolls:
    T0 = dt.datetime(2026, 10, 7, 23, 59, tzinfo=UTC)
    END = dt.datetime(2026, 10, 8, 0, 0, tzinfo=UTC)          # a test window (day) ends; the next uses 6EH7

    def _market(self):
        tick = 0.00005
        batches = [book_events("6EZ6", self.T0, 1.08500, 1.08505, tick, 1),
                   book_events("6EH7", self.END + dt.timedelta(milliseconds=100), 1.08950, 1.08955, tick, 2),
                   book_events("6EH7", self.END + dt.timedelta(seconds=2), 1.08950, 1.08955, tick, 3)]
        m = bt.Market(iter(batches), ["6E"], bt.CostModel(spread_follows_futures=False))
        return m, bt.BacktestExecutor(m, m.costs)

    def test_a_position_is_priced_on_the_contract_it_was_filled_on_not_the_next_one(self):
        m, ex = self._market()
        m.next_event()
        m.advance(self.T0 + dt.timedelta(seconds=1))
        tick = bt.SpotBroker(m).tick("EURUSD")
        spec = m.spec("EURUSD")
        buy = ex.open("EURUSD", Side.BUY, 1.0, 1.0700, 0.0, tick, spec, m.now)
        sell = ex.open("EURUSD", Side.SELL, 1.0, 1.0870, 0.0, tick, spec, m.now)   # 6EH7 is above this stop
        assert ex.held[buy.ticket].instrument == "6EZ6"
        m.advance(self.END)                       # the window is over; 6EH7 starts 100 ms later
        ex.close(buy.ticket, bt.SpotBroker(m).tick("EURUSD"), spec)
        p = ex.closed[-1]
        assert abs(p.result["mid_pips"]) < 1.0, "6EZ6 did not move: the 45-pip roll basis is not a profit"
        assert p.exit < 1.0855
        m.advance(self.END + dt.timedelta(seconds=3))
        assert ex.held[sell.ticket].trigger is None, "the next contract's price never touches this position's stop"

    def test_a_trade_closed_at_a_window_end_is_priced_on_its_own_contract(self, tmp_path):
        """Contiguous windows whose contracts differ (a roll): the trade still open when day one ends is closed
        at day one's last price, not at the next contract's first one."""
        evs = fixture_events()
        full = backtest(tmp_path, evs, "full")
        open_at = [(dt.datetime.fromisoformat(t["filled_utc"]), dt.datetime.fromisoformat(t["closed_utc"]))
                   for t in full.trades]
        cut = next(f + dt.timedelta(seconds=2) for f, c in sorted(open_at) if c - f > dt.timedelta(seconds=4))
        day1 = [e for e in evs if received_at(e) < cut]
        books = {}
        for e in day1:
            if isinstance(e, BookEvent):
                books.setdefault(e.instrument, OrderBook(e.instrument)).apply(e)
        nxt = {inst: inst[:2] + "H7" for inst in books}
        later = []
        for k, (inst, b) in enumerate(sorted(books.items())):  # the next contract, 1% higher, 100 ms after the cut
            bid, ask = b.best_bid(), b.best_ask()
            tick = round(ask - bid, 10) or 0.00005
            for j, at in enumerate((cut + dt.timedelta(milliseconds=100), cut + dt.timedelta(seconds=60))):
                later += book_events(nxt[inst], at, round(bid * 1.01, 9), round(bid * 1.01 + tick, 9), tick, 10 + j)
        w1 = bt.Window(received_at(evs[0]).replace(minute=0, second=0, microsecond=0), cut, "d1",
                       {bt.root_of(i): i for i in books})
        w2 = bt.Window(cut, cut + dt.timedelta(minutes=5), "d2", {bt.root_of(i): n for i, n in nxt.items()})
        res = bt.run_backtest(bt.event_batches(day1 + later), [w1, w2], IanConfig(), bt.CostModel(), tmp_path / "roll",
                              synthetic=True, sequence_scope="instrument")
        eod = [t for t in res.trades if t["exit_reason"] == "END_OF_DATA"]
        assert eod, "a trade should still be open when day one ends"
        for t in eod:
            assert t["contract"] in books
            assert abs(t["mid_pips"]) < 20, t     # a 1% roll basis is 70-150 pips: none of it may be booked


# ---------------------------------------------------------- R after costs --

class TestRAfterCosts:
    def test_expectancy_and_mfe_capture_in_r_are_after_every_cost(self):
        # +0.3 pips before commission on a 10-pip stop, -0.2 pips after it: a losing edge
        trades = [{"data_label": bt.REAL_LABEL, "ticket": i, "future": "6E", "net_pips": -0.2, "net_gbp": -0.02,
                   "gross_pips": 0.3, "cost_pips": 0.62, "mid_pips": 0.42, "gbp_per_pip": 0.1, "realised_r": 0.03,
                   "net_r": -0.02, "mfe_r": 0.5, "closed_utc": f"2026-10-07T09:{i:02d}:00+00:00"} for i in range(40)]
        st = bt.trade_stats(trades, 1)
        assert st["expectancy_gbp"] < 0 and st["expectancy_r"] == pytest.approx(-0.02)
        assert st["mfe_capture"] < 0
        assert "after every cost" in st["r_basis"]

    def test_each_trades_r_after_costs_is_its_net_over_the_initial_risk(self, tmp_path):
        res = backtest(tmp_path)
        assert res.trades
        for t in res.trades:
            risk_pips = abs(t["entry"] - t["initial_stop"]) / bt.pip_size(t["pair"])
            assert t["net_r"] == pytest.approx(t["net_pips"] / risk_pips, abs=1e-3)
            # Ian's own R leaves the commission out; this one has it
            assert t["net_r"] == pytest.approx(t["realised_r"] - t["commission_pips"] / risk_pips, abs=2e-3)
            assert (t["net_r"] > 0) == (t["net_gbp"] > 0)
        s = bt.summarise(res, {"period": {"first": "2026-10-07", "last": "2026-10-07"}})
        md = bt.report_md(s)
        assert "| Expectancy (after costs) |" in md and "R after every cost)" in md
        assert "| MFE capture (after costs) |" in md
        assert "net_r" in bt.trades_csv(res.trades).splitlines()[0]


# --------------------------------------------- what the exit code promises --

class TestExitCodes:
    def test_nothing_bought_has_a_code_of_its_own(self):
        assert hist.DECLINED not in (0, 1, 2, hist.OK, hist.SETUP_ERROR, hist.DATA_ERROR, hist.STOPPED)
        r = subprocess.run([sys.executable, "-m", "mintel.ian.research.historical", "--config", "x", "--days", "x"],
                           capture_output=True, text=True, cwd=str(ROOT))
        assert r.returncode not in (0, hist.DECLINED)        # argparse's own code for a bad option

    def test_ctrl_c_during_a_paid_download_is_not_nothing_bought(self, tmp_path):
        write_secrets(tmp_path / "data")
        c = FakeClient(fail_on_call={("get_range", 3): KeyboardInterrupt()})
        code, text, c, _ = run(tmp_path, "--days", "2", "--download-only", answer="y", client=c)
        assert code == hist.STOPPED and code != hist.DECLINED
        assert "was paid for and stays on disk" in text and "nothing was downloaded" not in text.lower()
        assert len(list((tmp_path / "data").rglob("*.dbn.zst"))) == 2

    def test_ctrl_c_during_the_replay_after_buying_is_not_nothing_bought(self, tmp_path, monkeypatch):
        write_secrets(tmp_path / "data")

        def replay(*a, **k):
            raise KeyboardInterrupt
        monkeypatch.setattr(hist, "run_test", replay)
        code, text, c, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", client=FakeClient())
        assert c.n("get_range") == 2 and code == hist.STOPPED

    def test_ctrl_c_before_anything_was_bought_is_nothing_bought(self, tmp_path):
        write_secrets(tmp_path / "data")
        c = FakeClient(fail_on_call={("get_cost", 2): KeyboardInterrupt()})
        code, text, c, _ = run(tmp_path, "--days", "2", answer="y", client=c)
        assert code == hist.DECLINED and c.n("get_range") == 0
        assert "nothing was downloaded or charged" in text


# -------------------------------------------------------- limits that hold --

class TestLimitsHold:
    @pytest.mark.parametrize("value", ["nan", "NaN", "inf", "-inf"])
    def test_a_max_cost_that_is_not_a_number_is_refused_before_databento_is_asked(self, tmp_path, value):
        write_secrets(tmp_path / "data")
        made = []
        code, text, _, _ = run(tmp_path, "--days", "1", "--yes", f"--max-cost={value}", "--download-only",
                               factory=lambda key: made.append(1) or FakeClient(cost=1000.0))
        assert code == hist.DECLINED and made == [] and "--max-cost must be a number" in text

    @pytest.mark.parametrize("value", ["nan", "inf", "0", "-1"])
    def test_a_size_budget_that_is_not_a_positive_number_is_refused(self, tmp_path, value):
        write_secrets(tmp_path / "data")
        made = []
        code, text, _, _ = run(tmp_path, "--days", "1", f"--max-gb={value}", answer="y",
                               factory=lambda key: made.append(1) or FakeClient())
        assert code == hist.DECLINED and made == [] and "--max-gb must be" in text

    def test_the_limit_check_itself_fails_closed(self):
        a = NS(max_cost=float("nan"), yes=True)
        with pytest.raises(hist.Refused):
            hist._confirm(1000.0, a, ask=lambda p: "y", isatty=lambda: True, say=lambda *x: None)
        with pytest.raises(hist.Refused):
            hist._confirm(float("nan"), NS(max_cost=50.0, yes=True), ask=lambda p: "y", isatty=lambda: True,
                          say=lambda *x: None)

    def test_a_quote_that_is_not_a_number_buys_nothing(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "50", client=FakeClient(cost=math.nan))
        assert code == hist.SETUP_ERROR and c.n("get_range") == 0 and "could not be read" in text


# ------------------------------------------- what is on disk, exactly --

class TestOnlyWhatIsMissing:
    def test_contracts_added_later_are_bought_and_only_they(self, tmp_path):
        write_secrets(tmp_path / "data")
        data = tmp_path / "data"
        (data / "ian.json").write_text(json.dumps({"instruments": ["6E", "6B"]}))
        code, text, c, _ = run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only")
        assert code == hist.OK and {tuple(k[1]["symbols"]) for k in c.calls if k[0] == "get_range"} == {("6BZ6", "6EZ6")}
        (data / "ian.json").write_text(json.dumps({}))          # back to the 7 futures
        code, text, c, _ = run(tmp_path, "--days", "1", "--download-only", answer="y")
        assert code == hist.OK, text
        assert "Already on disk" not in text
        bought = {(k[1]["schema"], tuple(k[1]["symbols"])) for k in c.calls if k[0] == "get_range"}
        rest = ("6AZ6", "6CZ6", "6JZ6", "6NZ6", "6SZ6")
        assert bought == {("mbp-10", rest), ("trades", rest)}, "only the contracts not on disk are bought"
        assert "only what is missing is bought: mbp-10 07:00-21:00 UTC for 6AZ6 6CZ6 6JZ6 6NZ6 6SZ6" in text
        code, text, c, _ = run(tmp_path, "--days", "1", "--download-only", answer="y")
        assert c.n("get_range") == 0 and "Every day is already on disk" in text

    def test_offline_does_not_test_a_day_without_every_contract(self, tmp_path):
        write_secrets(tmp_path / "data")
        data = tmp_path / "data"
        (data / "ian.json").write_text(json.dumps({"instruments": ["6E", "6B"]}))
        run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only")
        (data / "ian.json").write_text(json.dumps({}))
        code, text, c, _ = run(tmp_path, "--offline", factory=lambda key: pytest.fail("offline"))
        assert code == hist.DATA_ERROR
        assert "Not tested: 2026-10-07 is on disk without mbp-10 07:00-21:00 UTC for 6AZ6 6CZ6 6JZ6 6NZ6 6SZ6" in text

    def test_a_day_bought_in_pieces_replays_exactly_as_the_whole_day(self, tmp_path):
        pytest.importorskip("databento")
        # the fixture starts at 06:58 UTC: two minutes before London opens in this test's hours
        evs = fixture_events(start=dt.datetime(2026, 10, 7, 6, 58, tzinfo=UTC))
        server = FixtureServer(evs)
        whole, pieces = tmp_path / "whole", tmp_path / "pieces"
        write_secrets(whole / "data")
        code, text, _, _ = run(whole, "--days", "1", "--hours", "all", "--yes", "--max-cost", "5", "--download-only",
                               client=FakeClient(serve=server))
        assert code == hist.OK, text
        write_secrets(pieces / "data")
        (pieces / "data" / "ian.json").write_text(json.dumps({"instruments": ["6E", "6B"]}))
        for args in (("--days", "1"), ("--days", "1", "--hours", "all")):     # two contracts, then all hours
            assert run(pieces, *args, "--yes", "--max-cost", "5", "--download-only", client=FakeClient(serve=server))[0] == 0
        (pieces / "data" / "ian.json").write_text(json.dumps({}))
        for args in (("--days", "1"), ("--days", "1", "--hours", "all")):     # the other five, then all hours
            assert run(pieces, *args, "--yes", "--max-cost", "5", "--download-only", client=FakeClient(serve=server))[0] == 0
        # 6E+6B: 07-21, 00-07, 21-24; the other five: 07-21, 00-07, 21-24 - six pieces, none overlapping
        assert len(list((pieces / "data").rglob("mbp-10*.dbn.zst"))) == 6
        s, e = hist.window(DAY, "all")
        syms = {r: hist.front_month(r, DAY).raw_symbol for r in hist.CONTRACTS}

        def replay(root, schemas=("mbp-10", "trades")):
            st = bt.DayStats("d")
            files = {sch: hist.piece_files(root / "data", DAY, sch, s, e) for sch in schemas}
            out = [ev for b in bt.dbn_day_batches(hist.day_dir(root / "data", DAY), s, e, st, files=files,
                                                   symbols=syms.values()) for ev in b]
            return out, st

        a, sa = replay(whole)
        b, sb = replay(pieces)
        assert len(a) == len(b) > 1000 and sa.events == sb.events and sa.trade_records == sb.trade_records
        by = lambda evs: {i: [ev for ev in evs if ev.instrument == i] for i in {ev.instrument for ev in evs}}
        assert by(a) == by(b), "each contract's events are the same, whatever pieces the day was bought in"
        assert hist.on_disk(pieces / "data", DAY, "mbp-10", s, e, syms.values())
        # a contract the run does not test is left out of the replay
        syms2 = {r: x for r, x in syms.items() if r != "6E"}
        st = bt.DayStats("d")
        only = [ev for b in bt.dbn_day_batches(hist.day_dir(pieces / "data", DAY), s, e, st, symbols=syms2.values())
                for ev in b]
        assert only and not any(ev.instrument == "6EZ6" for ev in only) and st.other_contracts > 0


# ------------------------------------------------------ the cheaper first look --

class TestCheaperFirstLook:
    def test_with_start_and_end_it_suggests_a_narrower_range_not_days(self, tmp_path):
        write_secrets(tmp_path / "data")
        code, text, c, _ = run(tmp_path, "--start", "2026-10-01", "--end", "2026-10-07")
        assert code == hist.DECLINED
        assert "--days 2" not in text
        assert "A cheaper first look: --start 2026-10-06 --end 2026-10-07 (the last 2 of these days) would cost " \
               "about $5.00." in text

    def test_it_prices_the_days_it_would_actually_buy(self, tmp_path):
        write_secrets(tmp_path / "data")
        run(tmp_path, "--days", "1", "--yes", "--max-cost", "5", "--download-only")       # 7 Oct is on disk
        code, text, c, _ = run(tmp_path, "--days", "5")
        # --days 2 would be 6 and 7 Oct: 6 Oct to buy ($2.50), 7 Oct already on disk
        assert "A cheaper first look: --days 2 (the last 2 of these days) would cost about $2.50." in text


# ---------------------------------------------------------------- launchers --

MAC = ROOT / "deploy" / "mac" / "TEST-FINANCIAL-IAN.command"
PS1 = ROOT / "deploy" / "TEST-FINANCIAL-IAN.ps1"
CMD = ROOT / "deploy" / "TEST-FINANCIAL-IAN.cmd"


class TestLaunchers:
    def test_the_mac_launcher_is_valid_bash_and_executable(self):
        r = subprocess.run(["bash", "-n", str(MAC)], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        assert os.access(MAC, os.X_OK)

    def test_the_launchers_never_touch_the_running_bots(self):
        assert "TEST-FINANCIAL-IAN.ps1" in CMD.read_text() and CMD.read_bytes().count(b"\r\n") >= 3
        for p in (MAC, PS1, CMD):
            s = p.read_text()
            assert p is CMD or "mintel.ian.research.historical" in s, p
            for bad in ("--ian-feed", "launchctl", "kill ", "Stop-Process", "Stop-Bot", "START-BOT.ps1",
                        "Start Trading Bot.command", "SETUP-AND-START.ps1", "--live", "mintel.ops.modes", ".pid"):
                assert bad not in s, (p, bad)
        assert "nice -n 19" in MAC.read_text()
        assert "Idle" in PS1.read_text() or "BelowNormal" in PS1.read_text()

    def _sandbox(self, tmp_path, with_key):
        """A pretend ~/MarketBot whose venv python logs what it is asked and runs the rest for real."""
        home = tmp_path / "home"
        mb = home / "MarketBot"
        (mb / "venv" / "bin").mkdir(parents=True)
        (mb / "data").mkdir(parents=True)
        (mb / "data" / "config.json").write_text("{}")
        if with_key:
            (mb / "data" / "secrets.json").write_text(json.dumps({"databento_api_key": KEY}))
        app = mb / "app"
        (app / "mintel" / "ian" / "research").mkdir(parents=True)
        (app / "mintel" / "ian" / "research" / "historical.py").write_text("# placeholder\n")
        log = tmp_path / "calls.log"
        py = mb / "venv" / "bin" / "python"
        py.write_text(f"""#!/bin/bash
echo "$*" >> "{log}"
case "$*" in
  *"-m mintel.ian.research.historical"*"--set-key"*) exit 0 ;;
  *"-m mintel.ian.research.historical"*) exit ${{FAKE_TEST_CODE:-0}} ;;
  *"-m pip install"*) exit 0 ;;
  *"import databento"*) exit 1 ;;
esac
exec "{sys.executable}" "$@"
""")
        py.chmod(0o755)
        return home, log

    @pytest.mark.parametrize("with_key", [True, False])
    def test_the_mac_launcher_runs_the_cost_first_test_with_the_defaults(self, tmp_path, with_key):
        if shutil.which("bash") is None:
            pytest.skip("no bash")
        home, log = self._sandbox(tmp_path, with_key)
        desk = home / "Desktop"
        desk.mkdir()
        shutil.copy(MAC, desk / MAC.name)                     # copied to the Desktop, away from the program files
        r = subprocess.run(["bash", str(desk / MAC.name)], capture_output=True, text=True, timeout=60,
                           env={**os.environ, "HOME": str(home), "MINTEL_NO_PAUSE": "1"}, stdin=subprocess.DEVNULL)
        calls = log.read_text().splitlines()
        assert r.returncode == 0, r.stdout + r.stderr
        assert any("-m pip install" in c and "databento" in c for c in calls)          # installed when missing
        test_runs = [c for c in calls if "mintel.ian.research.historical" in c and "--set-key" not in c]
        assert len(test_runs) == 1 and "--upload" in test_runs[0] and "--yes" not in test_runs[0]
        assert "--config " + str(home / "MarketBot" / "data" / "config.json") in test_runs[0]
        set_key = [c for c in calls if "--set-key" in c]
        assert len(set_key) == (0 if with_key else 1)
        assert KEY not in r.stdout + r.stderr

    @pytest.mark.parametrize("code,nothing", [(10, True), (1, False), (2, False), (3, False), (4, False)])
    def test_the_mac_launcher_says_nothing_was_bought_only_when_nothing_was(self, tmp_path, code, nothing):
        if shutil.which("bash") is None:
            pytest.skip("no bash")
        home, log = self._sandbox(tmp_path, True)
        r = subprocess.run(["bash", str(MAC)], capture_output=True, text=True, timeout=60,
                           env={**os.environ, "HOME": str(home), "MINTEL_NO_PAUSE": "1", "FAKE_TEST_CODE": str(code)},
                           stdin=subprocess.DEVNULL)
        assert r.returncode == code
        assert ("Nothing was bought." in r.stdout) is nothing, r.stdout
        if not nothing:
            assert "Anything already downloaded was paid for and stays on disk" in r.stdout

    def test_the_windows_launcher_says_nothing_was_bought_only_for_that_code(self):
        s = PS1.read_text()
        arms = dict(re.findall(r"^\s*(\d+|default) \{ Write-Host \"([^\"]*)\"", s, re.M))
        assert arms["10"] == "  Nothing was bought."
        assert "1" not in arms and set(arms) == {"0", "10", "default"}
        assert "paid for and stays on disk" in arms["default"] and "Nothing" not in arms["default"]
        assert str(hist.DECLINED) == "10"

    def test_the_mac_launcher_passes_a_cheaper_first_look_through(self, tmp_path):
        if shutil.which("bash") is None:
            pytest.skip("no bash")
        home, log = self._sandbox(tmp_path, True)
        r = subprocess.run(["bash", str(MAC), "--days", "2"], capture_output=True, text=True, timeout=60,
                           env={**os.environ, "HOME": str(home), "MINTEL_NO_PAUSE": "1"}, stdin=subprocess.DEVNULL)
        assert r.returncode == 0, r.stdout + r.stderr
        runs = [c for c in log.read_text().splitlines() if "mintel.ian.research.historical" in c]
        assert runs and runs[-1].endswith("--upload --days 2")
