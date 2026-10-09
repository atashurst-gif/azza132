"""Databento adapter: CME Globex MDP 3.0 (dataset GLBX.MDP3) into the internal format.

Optional in every sense: the ``databento`` package is imported only when this
feed is used, and the API key is read from data/secrets.json
(``"databento_api_key"``). A missing package, a missing key or a refused
login makes the feed NOT CONFIGURED or DOWN with a plain reason - it never
raises into the engine.

Schemas:

* ``mbp-10`` - the top ten price levels each side, after every book event.
  Each record carries the whole ten-level view, so the adapter keeps the
  previous view per instrument and emits only the level CHANGES as
  price-level BookEvents. A level that leaves the ten-level window because
  the book moved (not because it was cancelled) is marked ``cause="view"``,
  as is a deeper level coming into view.
* ``mbo``    - every order (add / modify / cancel / clear), with order ids.
  With ``snapshot=True`` the gateway first sends the current book.
* ``trades`` - every execution, with the aggressor side ('B' = a buyer lifted
  the offer, 'A' = a seller hit the bid, 'N' = not given -> UNKNOWN).

Prices arrive as fixed-point integers (1e-9 units); UNDEF_PRICE (2**63 - 1)
means "no level". Timestamps are nanoseconds since the epoch: ``ts_event`` is
the exchange's matching-engine time, ``ts_recv`` the vendor's capture time.
Record flags: F_MAYBE_BAD_BOOK (4) -> a BAD_BOOK notice; F_BAD_TS_RECV (8)
-> a timestamp problem the quality monitor sees.

Sequence numbers on CME are per CHANNEL, not per instrument, so gaps between
one instrument's records are normal; this feed's ``sequence_scope`` is
"channel" and the vendor's own gap and error messages drive the quality
monitor instead.

Other vendors (Rithmic, CQG, a futures broker's API) fit by writing another
adapter with the same four methods; nothing downstream changes.
"""
from __future__ import annotations

import datetime as dt
import json
import queue
import threading
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from .. import INSTITUTIONAL
from ..book import (ADD, ASK, BID, BUY, CANCEL, CLEAR, MODIFY, SELL, UNKNOWN, BookEvent, Event, FeedNotice,
                    TradeEvent, px)
from .base import CONNECTING, DOWN, LIVE, NOT_CONFIGURED, FeedAdapter, FeedHealth

KEY_NAME = "databento_api_key"
DATASET = "GLBX.MDP3"
UNDEF_PRICE = 2 ** 63 - 1
PRICE_SCALE = 1e-9
F_MAYBE_BAD_BOOK = 1 << 2
F_BAD_TS_RECV = 1 << 3
F_SNAPSHOT = 1 << 5
UTC = dt.timezone.utc


def read_api_key(data_dir: str | Path) -> str:
    try:
        s = json.loads((Path(data_dir) / "secrets.json").read_text())
        return str(s.get(KEY_NAME) or "").strip() if isinstance(s, dict) else ""
    except Exception:
        return ""


def save_api_key(data_dir: str | Path, key: str) -> Path:
    """Into secrets.json, keeping everything else in it. Never logged."""
    import os
    p = Path(data_dir) / "secrets.json"
    existing: dict = {}
    if p.exists():
        try:
            existing = json.loads(p.read_text())
        except Exception:
            existing = {}
        if not isinstance(existing, dict):
            existing = {}
    existing[KEY_NAME] = key.strip()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(existing, indent=2))
    tmp.replace(p)
    try:
        os.chmod(p, 0o600)
    except Exception:
        pass
    return p


def package_available() -> bool:
    try:
        import databento  # noqa: F401
        return True
    except Exception:
        return False


# ------------------------------------------------------------ field access --

def _get(rec: Any, name: str, default=None):
    if isinstance(rec, dict):
        return rec.get(name, default)
    return getattr(rec, name, default)


def _char(v) -> str:
    """'A', b'A', 65, an enum whose value is 'A' -> 'A'."""
    if v is None:
        return ""
    if hasattr(v, "value"):
        v = v.value
    if isinstance(v, bytes):
        v = v.decode(errors="ignore")
    if isinstance(v, int):
        try:
            return chr(v)
        except ValueError:
            return ""
    s = str(v)
    return s[-1:] if s else ""


def _price(v) -> Optional[float]:
    """Fixed-point int (1e-9) or an already-decimal float; None for 'no price'."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        if v == UNDEF_PRICE or v >= UNDEF_PRICE:
            return None
        return px(v * PRICE_SCALE)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f >= 9e9:                       # NaN or the undefined sentinel as a float
        return None
    return px(f)


def _ts(v) -> Optional[dt.datetime]:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    try:
        ns = int(v)
    except (TypeError, ValueError):
        return None
    if ns <= 0 or ns >= 2 ** 63 - 1:
        return None
    return dt.datetime.fromtimestamp(ns // 1_000_000_000, tz=UTC) + dt.timedelta(microseconds=(ns % 1_000_000_000) // 1000)


def _kind(rec: Any) -> str:
    """mbp10 / mbo / trade / mapping / error / system / other, from the record's class or fields."""
    if isinstance(rec, dict):
        k = str(rec.get("_type") or rec.get("rtype_name") or "").lower()
        if k:
            return k
        if "levels" in rec:
            return "mbp10"
        if "order_id" in rec:
            return "mbo"
        if "stype_out_symbol" in rec:
            return "mapping"
        if "err" in rec:
            return "error"
        if "msg" in rec:
            return "system"
        if "price" in rec and "size" in rec:
            return "trade"
        return "other"
    name = type(rec).__name__.lower()
    if "mbp10" in name:
        return "mbp10"
    if "mbo" in name:
        return "mbo"
    if "trade" in name:
        return "trade"
    if "symbolmapping" in name:
        return "mapping"
    if "error" in name:
        return "error"
    if "system" in name:
        return "system"
    return "other"


class RecordMapper:
    """Pure translation of Databento records into internal events. Kept separate from the network so it
    can be tested with recorded or hand-made records (no package, no key, no internet)."""

    def __init__(self, symbols: Optional[dict] = None, source: str = "databento", levels: int = 10):
        self.symbols: dict[int, str] = dict(symbols or {})
        self.source = source
        self.levels = levels
        self.prev: dict[str, tuple[dict, dict]] = {}     # instrument -> (bids {price: size}, asks)

    def symbol(self, rec) -> str:
        iid = _get(rec, "instrument_id")
        s = self.symbols.get(int(iid)) if iid is not None else None
        return str(s or _get(rec, "symbol") or (f"#{iid}" if iid is not None else ""))

    def map(self, rec) -> list[Event]:
        k = _kind(rec)
        if k == "mapping":
            iid = _get(rec, "instrument_id")
            out_sym = _get(rec, "stype_out_symbol") or _get(rec, "symbol")
            if iid is not None and out_sym:
                self.symbols[int(iid)] = str(out_sym)
            return []
        if k in ("error", "system"):
            text = str(_get(rec, "err") or _get(rec, "msg") or "")
            if k == "system" and ("heartbeat" in text.lower() or not text):
                return []
            notice = "ERROR" if k == "error" else ("GAP" if "gap" in text.lower() else "INFO")
            ts = _ts(_get(rec, "ts_event")) or dt.datetime.now(UTC)
            return [FeedNotice("", ts, notice, text[:300], self.source)]
        if k == "mbp10":
            return self._mbp10(rec)
        if k == "mbo":
            return self._mbo(rec)
        if k == "trade":
            ev = self._trade(rec)
            return [ev] if ev is not None else []
        return []

    def _common(self, rec) -> tuple[str, dt.datetime, dt.datetime, int, int]:
        inst = self.symbol(rec)
        te = _ts(_get(rec, "ts_event")) or dt.datetime.now(UTC)
        tr = _ts(_get(rec, "ts_recv")) or te
        seq = int(_get(rec, "sequence", 0) or 0)
        flags = int(_get(rec, "flags", 0) or 0)
        return inst, te, tr, seq, flags

    def _notices(self, inst: str, ts: dt.datetime, flags: int) -> list[Event]:
        if flags & F_MAYBE_BAD_BOOK:
            return [FeedNotice(inst, ts, "BAD_BOOK", "the vendor flagged the book as possibly wrong", self.source)]
        return []

    def _trade(self, rec) -> Optional[TradeEvent]:
        inst, te, tr, seq, _ = self._common(rec)
        p = _price(_get(rec, "price"))
        size = float(_get(rec, "size", 0) or 0)
        if p is None or size <= 0:
            return None
        side = _char(_get(rec, "side"))
        aggr = BUY if side == "B" else SELL if side == "A" else UNKNOWN
        return TradeEvent(inst, te, p, size, aggr, seq, tr, self.source)

    def _mbp10(self, rec) -> list[Event]:
        inst, te, tr, seq, flags = self._common(rec)
        out: list[Event] = list(self._notices(inst, te, flags))
        if _char(_get(rec, "action")) == "T":
            t = self._trade(rec)                     # an mbp-10 trade record also carries the trade
            if t is not None:
                out.append(t)
        levels = _get(rec, "levels") or []
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        for lv in list(levels)[: self.levels]:
            bp, ap = _price(_get(lv, "bid_px")), _price(_get(lv, "ask_px"))
            bs, az = float(_get(lv, "bid_sz", 0) or 0), float(_get(lv, "ask_sz", 0) or 0)
            if bp is not None and bs > 0:
                bids[bp] = bs
            if ap is not None and az > 0:
                asks[ap] = az
        pb, pa = self.prev.get(inst, ({}, {}))
        snap = bool(flags & F_SNAPSHOT) or inst not in self.prev
        if snap:
            # a rebuild (the first record, or the first after a recovery cleared the views): the book is
            # cleared and this view is the whole of it - levels from before a reconnect never linger
            out.append(BookEvent(inst, te, tr, seq, "", 0.0, 0.0, CLEAR, cause="snapshot", source=self.source))
            pb, pa = {}, {}
        out += self._diff(inst, te, tr, seq, BID, pb, bids, snap)
        out += self._diff(inst, te, tr, seq, ASK, pa, asks, snap)
        self.prev[inst] = (bids, asks)
        return out

    def _diff(self, inst, te, tr, seq, side, old: dict, new: dict, snap: bool) -> list[Event]:
        """Level changes between two ten-level views. A level beyond the far end of the other view, when
        that view is full, only moved into or out of the window ("view"), it was not added or cancelled."""
        out: list[Event] = []
        bid = side == BID
        full_old, full_new = len(old) >= self.levels, len(new) >= self.levels
        far_new = (min(new) if bid else max(new)) if new else None
        far_old = (min(old) if bid else max(old)) if old else None
        for p in sorted(set(old) | set(new), reverse=bid):
            a, b = old.get(p, 0.0), new.get(p, 0.0)
            if a == b:
                continue
            cause = "snapshot" if snap else ""
            if b <= 0:
                beyond = far_new is not None and ((p < far_new) if bid else (p > far_new))
                if not cause and full_new and beyond:
                    cause = "view"
                out.append(BookEvent(inst, te, tr, seq, side, p, 0.0, CANCEL, cause=cause, source=self.source))
            elif a <= 0:
                beyond = far_old is not None and ((p < far_old) if bid else (p > far_old))
                if not cause and full_old and beyond:
                    cause = "view"
                out.append(BookEvent(inst, te, tr, seq, side, p, b, ADD, cause=cause, source=self.source))
            else:
                out.append(BookEvent(inst, te, tr, seq, side, p, b, MODIFY, cause=cause, source=self.source))
        return out

    def _mbo(self, rec) -> list[Event]:
        inst, te, tr, seq, flags = self._common(rec)
        out: list[Event] = list(self._notices(inst, te, flags))
        action = _char(_get(rec, "action"))
        side_c = _char(_get(rec, "side"))
        if action in ("T", "F"):
            if action == "T":                          # the trade; a fill ('F') is the resting side of the same one
                t = self._trade(rec)
                if t is not None:
                    out.append(t)
            return out
        cause = "snapshot" if flags & F_SNAPSHOT else ""
        if action == "R":
            out.append(BookEvent(inst, te, tr, seq, "", 0.0, 0.0, CLEAR, cause=cause or "snapshot", source=self.source))
            return out
        side = BID if side_c == "B" else ASK if side_c == "A" else ""
        p = _price(_get(rec, "price"))
        if not side or p is None:
            return out
        a = {"A": ADD, "M": MODIFY, "C": CANCEL}.get(action)
        if a is None:
            return out
        oid = int(_get(rec, "order_id", 0) or 0)
        size = float(_get(rec, "size", 0) or 0)
        out.append(BookEvent(inst, te, tr, seq, side, p, size, a, order_id=oid, cause=cause, source=self.source))
        return out


class DatabentoFeed(FeedAdapter):
    vendor = "databento"
    data_class = INSTITUTIONAL
    synthetic = False
    virtual_time = False
    sequence_scope = "channel"

    def __init__(self, data_dir: str | Path, schema: str = "mbp-10", dataset: str = DATASET, trades: bool = True,
                 api_key: Optional[str] = None, client_factory: Optional[Callable[..., Any]] = None,
                 stype_in: str = "raw_symbol", max_queue: int = 200_000):
        self.data_dir = Path(data_dir)
        self.schema = schema
        self.dataset = dataset
        self.trades = trades
        self.stype_in = stype_in
        self.api_key = api_key if api_key is not None else read_api_key(self.data_dir)
        self.client_factory = client_factory
        self.q: "queue.Queue" = queue.Queue(maxsize=max_queue)
        self.mapper = RecordMapper()
        self.client = None
        self.instruments: list[str] = []
        self.state = NOT_CONFIGURED
        self.reason = ""
        self.last_error = ""
        self.count = 0
        self.dropped = 0
        self.last_event: Optional[dt.datetime] = None
        self.reconnects = 0
        self._lock = threading.Lock()
        self._configured_check()

    def _configured_check(self) -> bool:
        if self.client_factory is None and not package_available():
            self.state, self.reason = NOT_CONFIGURED, "the databento package is not installed (pip install databento)"
            return False
        if not self.api_key:
            self.state, self.reason = NOT_CONFIGURED, ('no Databento API key: put "databento_api_key" in data/secrets.json '
                                                       "(python -m mintel.ian --set-key)")
            return False
        if self.state == NOT_CONFIGURED:
            self.state, self.reason = CONNECTING, "configured, not connected yet"
        return True

    def _make_client(self):
        if self.client_factory is not None:
            return self.client_factory(key=self.api_key)
        import databento as db
        try:
            return db.Live(key=self.api_key, reconnect_policy="reconnect")
        except TypeError:                                 # an older package without reconnect policies
            return db.Live(key=self.api_key)

    def connect(self) -> bool:
        if not self._configured_check():
            return False
        return True                       # the connection is made by subscribe(), which needs the symbols

    def subscribe(self, instruments: Sequence[str]) -> None:
        """(Re)subscribe: any earlier session is closed first, so a roll or a recovery never leaves two
        sessions feeding the same queue."""
        self.instruments = [i for i in instruments if i]
        if not self._configured_check() or not self.instruments:
            return
        self.close()
        try:
            self.client = self._make_client()
            self.client.subscribe(dataset=self.dataset, schema=self.schema, symbols=self.instruments,
                                  stype_in=self.stype_in, **({"snapshot": True} if self.schema == "mbo" else {}))
            if self.trades and self.schema != "trades":
                self.client.subscribe(dataset=self.dataset, schema="trades", symbols=self.instruments,
                                      stype_in=self.stype_in)
            self.client.add_callback(self._on_record, self._on_error)
            self.client.start()
            self.state, self.reason = LIVE, f"{self.dataset} {self.schema}{' + trades' if self.trades else ''}"
        except Exception as exc:
            self.state = DOWN
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.reason = f"could not connect to Databento: {self.last_error}"
            self._put(FeedNotice("", dt.datetime.now(UTC), "DISCONNECTED", self.reason, "databento"))

    def _put(self, ev: Event) -> None:
        try:
            self.q.put_nowait(ev)
        except queue.Full:
            self.dropped += 1
            if self.dropped == 1:
                self.state, self.reason = DOWN, "the engine is not keeping up with the feed (queue full)"

    def _on_record(self, rec) -> None:
        try:
            evs = self.mapper.map(rec)
        except Exception as exc:
            self.last_error = f"mapping failed: {exc}"
            return
        for ev in evs:
            self._put(ev)
        if evs:
            with self._lock:
                self.count += len(evs)
                self.last_event = dt.datetime.now(UTC)

    def _on_error(self, exc) -> None:
        self.last_error = f"{type(exc).__name__}: {exc}"
        self._put(FeedNotice("", dt.datetime.now(UTC), "ERROR", self.last_error, "databento"))

    def events(self, max_events: int = 10_000) -> list[Event]:
        out: list[Event] = []
        try:
            while len(out) < max_events:
                out.append(self.q.get_nowait())
        except queue.Empty:
            pass
        return out

    def health(self) -> FeedHealth:
        self._configured_check()
        state, reason = self.state, self.reason
        if state == LIVE and self.last_event is not None:
            quiet = (dt.datetime.now(UTC) - self.last_event).total_seconds()
            if quiet > 60:
                reason = f"{reason}; nothing received for {quiet:.0f} s"
        return FeedHealth(state, reason, self.vendor, INSTITUTIONAL, False, state == LIVE, self.count,
                          self.last_event.isoformat() if self.last_event else None, self.last_error, self.reconnects,
                          list(self.instruments))

    def recover(self, reason: str = "") -> bool:
        """Drop the session and subscribe again (MBO asks for a fresh snapshot); the book is rebuilt."""
        if not self._configured_check():
            return False
        self.reconnects += 1
        self.mapper.prev.clear()
        self.subscribe(self.instruments)
        return self.state == LIVE

    def close(self) -> None:
        c, self.client = self.client, None
        if c is None:
            return
        for m in ("stop", "terminate"):
            try:
                getattr(c, m)()
                break
            except Exception:
                continue
