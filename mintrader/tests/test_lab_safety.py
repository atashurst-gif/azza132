"""The lab can NEVER place an order (spec section 8).

* ``PriceFeed`` has no send, modify_stops, close or _rpc, under any name; any
  question outside the seven in ``ALLOWED`` is refused before anything reaches
  the bridge;
* a made-up bridge that records every method, over a whole UK practice day,
  is asked only those seven, and the made-up account it holds is untouched;
* nothing under mintel/lab imports a broker that can trade, an order type, the
  executor, the trader or the bots' modes; only prices.py reaches the bridge;
  no file mentions an order call or the secrets file;
* no heartbeat file named after the lab is ever written (a stale heartbeat the
  trader does not know would stop its entries).

EVERY FIGURE HERE IS MADE-UP TEST DATA.
"""
from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest

from mintel.lab import LabSettings
from mintel.lab.prices import ALLOWED, LabMayNotTrade, PriceFeed
from tests.test_lab_practice import build, run_minutes

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "mintel" / "lab"
UTC = dt.timezone.utc


def test_the_seven_questions_and_nothing_else():
    assert ALLOWED == frozenset({"ping", "server_time", "symbols", "spec", "tick", "ticks", "bars"})


def test_the_price_feed_has_no_way_to_trade():
    for name in ("send", "modify_stops", "close", "_rpc", "positions", "account", "deals_since", "connect",
                 "shutdown"):
        assert name not in dir(PriceFeed), name
    built = []

    class Recorder:
        def __init__(self, **kw):
            built.append(kw)

        def _rpc(self, method, **args):
            raise AssertionError(f"reached the wire with {method}")

    feed = PriceFeed(factory=Recorder)
    for name in ("send", "modify_stops", "close", "_rpc", "positions", "order_check"):
        assert not hasattr(feed, name), name
        assert name not in dir(feed), name
        with pytest.raises(LabMayNotTrade):
            getattr(feed, name)
    for method in ("send", "modify_stops", "close", "positions", "account", "deals_since", "connect",
                   "shutdown_broker", "calc_margin", "ensure_selected"):
        with pytest.raises(LabMayNotTrade):
            feed._ask(method, symbol="EURUSD")
    assert built == []                               # refused before a broker was even built
    with pytest.raises(AttributeError):
        feed.anything_else = 1                       # nothing can be bolted on (fixed slots)


def test_the_kept_wire_call_refuses_a_trading_name_even_when_called_directly():
    sent = []

    class Broker:
        def __init__(self, **kw):
            pass

        def _rpc(self, method, **args):
            sent.append(method)
            return {"backend": "real"}

    feed = PriceFeed(factory=Broker)
    assert feed.ping() == {"backend": "real"}
    for name in ("send", "close", "modify_stops", "positions"):
        with pytest.raises(LabMayNotTrade):
            feed._wire(name, request={})
    assert sent == ["ping"]
    assert not hasattr(feed._wire, "__self__")


def test_the_feed_keeps_only_the_wire_call_and_hard_codes_each_method():
    src = (LAB / "prices.py").read_text()
    for name in sorted(ALLOWED):
        assert f'self._ask("{name}"' in src, name   # each method name written out by hand
    assert "del broker" in src and "self._wire = _guarded(broker._rpc)" in src
    assert re.findall(r"^\s*def (\w+)\(", src, re.M) == [
        "_guarded", "ask", "__init__", "_open", "_ask", "__getattr__", "ping", "server_time", "symbols", "spec", "tick", "ticks",
        "bars", "from_config"]


def test_a_whole_practice_day_asks_the_bridge_for_prices_only(tmp_path):
    paths, led, clock, bridge, eng, cands = build(tmp_path)
    start = dt.datetime(2026, 10, 12, 23, 0, 3, tzinfo=UTC)          # 00:00 UK on Tuesday 13 Oct
    run_minutes(eng, clock, start, start + dt.timedelta(minutes=1439), every_status=30)
    assert led.one("SELECT COUNT(*) FROM practice_trades")[0] == 4    # it did trade (on paper)
    assert set(bridge.calls) <= ALLOWED
    assert {"bars", "ticks", "ping"} <= set(bridge.calls)
    assert bridge.deals == [{"ticket": 5001, "symbol": "EURUSD", "profit": 12.5, "magic": 990311}]
    assert bridge.positions == [{"ticket": 1001, "symbol": "GBPUSD", "volume": 0.1, "magic": 990311}]
    assert {r[0] for r in led.query("SELECT DISTINCT mode FROM practice_trades")} == {"PRACTICE"}
    hb = paths.data_dir / "heartbeats"
    assert not hb.exists() or not [f for f in hb.iterdir() if "lab" in f.name]


def _lab_files():
    files = sorted(LAB.rglob("*.py"))
    assert len(files) >= 25
    return files


def test_nothing_under_the_lab_can_reach_a_trading_broker():
    bad_import = re.compile(r"^\s*(from|import)\s+[\w. ]*\b(OrderRequest|PaperBroker|Mt5Broker|execution|"
                            r"trader|Trader|modes)\b", re.M)
    for p in _lab_files():
        text = p.read_text(encoding="utf-8")
        rel = p.relative_to(LAB).as_posix()
        for word in ("order_send", "modify_stops", "position_close", "secrets.json", "OrderRequest",
                     "PaperBroker", "Mt5Broker", "mt5_adapter", "Heartbeat("):
            assert word not in text, (rel, word)
        for m in bad_import.finditer(text):
            assert m.group(0).strip() == "from ..broker.paper import DEFAULT_COMMISSION, DEFAULT_FALLBACK_FX", \
                (rel, m.group(0))
        if rel != "prices.py":
            assert "BridgeBroker" not in text and "bridge_client" not in text, rel
            assert "_rpc" not in text, rel
            # the feed's kept wire call is used by prices.py alone, and nothing climbs back to its broker
            assert "_wire" not in text and "__self__" not in text and "__closure__" not in text, rel
    prices = (LAB / "prices.py").read_text()
    assert "from ..broker.bridge_client import BridgeBroker" in prices


def test_the_lab_writes_no_heartbeat_and_never_names_one():
    for p in _lab_files():
        text = p.read_text(encoding="utf-8")
        assert "heartbeat.json" not in text and "Heartbeat(" not in text, p.name
        assert "heartbeats" not in re.sub(r"(?s)\"\"\".*?\"\"\"|#[^\n]*", "", text) or p.name == "__init__.py", p.name


def test_the_lab_has_no_live_mode():
    assert LabSettings.from_dict({"mode": "LIVE"}).mode == "PAPER"
    from mintel.lab.__main__ import main
    import io
    import contextlib
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        assert main(["mode", "live", "--config", "/nonexistent/config.json"]) == 2
    assert "no live mode" in err.getvalue()


def test_the_icon_never_asks_about_live():
    text = (ROOT / "deploy" / "mac" / "The Lab.command").read_text()
    assert "mode live" not in text.lower() and "--live" not in text
    assert "mode paper" in text and "mode off" in text
