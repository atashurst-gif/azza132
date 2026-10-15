"""Practice: every idea on live prices, with no money.

    python -m mintel.lab.practice --config data/config.json

Started and kept alive by the watchdog while ``data/lab.json`` does not say OFF
(nice 10). It reads prices through ``prices.PriceFeed`` and nothing else, so it
CANNOT place an order; its trades are rows in ``data/lab/lab.sqlite`` marked
PRACTICE and nowhere else.

Who practises: with ``practise_all`` on in ``data/lab.json`` (the default since
13 Oct) EVERY idea in the ledger, whatever its stage - binned ones included -
each on every market of its kind that the lab stores (at most 24). The past and
fresh tests still run each night and are shown, but no longer decide who
practises; the stage history is left exactly as the rule wrote it. With it off,
only the nightly's roster (PRACTICE and READY FOR LIVE) practises, as before.
Aaron, 13 Oct: "I just want all in practice unless any earn the right [to real
money]" - the weekly pick (``mintel/ops/weekly.py``) is what judges that.

Every minute, three seconds past it:

* for each market any idea watches the last three one-minute bars are read (a
  missed stretch is read back), and each finished bar goes through the same
  feature code the past tests use (``features.LiveSeries``) ONCE, shared by
  every idea on that market;
* every open practice position is moved through each finished bar by the
  simulator's own ``step`` (the stop first, then the target, the trail, the time
  and the 21:55 UK flat); a feed silent for more than 5 minutes with a position
  open closes it at the first bar back (FEED_GAP);
* the flat comes on the CLOCK (14 Oct): at 21:55 UK (21:00 on a Friday) a
  position whose market printed no bar that closes at the flat (AUS200 stops at
  19:59 UTC) is closed once that market's bars have been read, at its last bar
  before the flat - the past test's own exit, its last bar before the flat -
  booked SESSION_END at the flat; a bar that comes after the flat never walks a
  position the flat should have closed (it is closed at the flat, never a
  FEED_GAP the next morning). On a UK weekend a position still open (the process
  was down over Friday's flat) is closed the same way, and nothing else happens.
  The time exit stays the simulator's: measured on the bars, as the past test;
* each idea's family looks at the newest finished bar. Ideas that differ only in
  their exit, markets or news rule see the very same signals, so a family's
  ``signals`` runs once per (settings, session) a market a bar and the answer is
  shared; a signal is filled on ONE ``ticks`` call for all the markets that want
  one, at the real ask (buys) or bid (sells) plus slippage, under the same caps
  as the past test, per idea (one position per market, three open per idea, a
  five-bar pause after an exit, six entries per market per UK day). Each idea is
  simulated on its own, so there is no cap across the lab: the old 6 slots, 12
  markets and 12 open across the lab only made sense for a handful of ideas.

Nothing on a UK Saturday or Sunday. A restart resumes: open positions come back
from ``practice_open`` (and the small ``practice_state.json`` beside it), and the
bars missed while it was down are read back and walked through.

Every 30 seconds it writes ``data/lab-status.json`` (the watchdog's sign of
life: the lab writes NO heartbeat file) and refreshes the practice figures in
``data/lab/summary.json``.

Aaron, 10 Oct 2026: "each day the winners stand out and we watch them closely
like scouts".
"""
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from ..broker.base import TF
from ..clock import TZ_LONDON
from . import (COOLDOWN_BARS, FEED_GAP_MINUTES, MAX_ENTRIES_PER_DAY, MAX_OPEN_PER_CANDIDATE,
               NEWS_AVOID_MINUTES, PRACTICE, READY_FOR_LIVE, SLIP_MIN_POINTS, SLIP_SPREAD_SHARE, UNIVERSE_CAP,
               UTC, LabPaths, LabSettings, atomic_write_json, day_min, epoch_min, iso_utc, minute_to_dt,
               utc_stamp)
from . import features as FEAT
from . import sim
from .candidate import family_of
from .cut import load_cut
from .ledger import LabLedger
from .report import refresh_practice
from .stages import cand_of
from .status import STATUS_EVERY_S, pid_alive, write_status
from .store import BarStore, bar_row, flat_min, load_universe

TICKET_BASE = 970_000_001
MAX_SYMBOLS = UNIVERSE_CAP                    # every market the lab stores (24), no fewer
PRIME_BARS = max(FEAT.TAIL_BARS.values())     # enough warm-up for the slowest feature (EMA200 on H1)
READ_BACK_MAX = 3000                          # bars read back after a gap (one page)
RESET_AT = 2 * PRIME_BARS                     # a series this long is rebuilt at the UK day's turn
AT_SECOND = 3                                 # the minute's work starts 3 s past the minute


def _uk_date(m: int) -> str:
    return minute_to_dt(m).astimezone(TZ_LONDON).date().isoformat()


def _iso_min(stamp: str) -> int:
    t = dt.datetime.fromisoformat(stamp)
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return epoch_min(t)


@dataclass
class Open:
    """One open practice position and what is needed to finish its record."""
    cid: str
    symbol: str
    pos: object                     # sim.Pos, prices in points
    day: str                        # the UK day it opened on
    signal_bar_utc: str
    tick_json: str
    last_t: int                     # the last bar it was walked through (epoch minutes)
    flat_waiting: bool = False      # its flat has come with no fresh price yet (said once in the log)


@dataclass
class Slot:
    """One idea on the roster."""
    cid: str
    name: str
    cand: object
    family: object
    params: dict
    symbols: list
    stage: str
    next_ok: dict = field(default_factory=dict)       # symbol -> first minute a signal may fire
    entries: dict = field(default_factory=dict)       # "symbol|uk day" -> entries so far
    wants_news: bool = False                          # its family is handed the news minutes
    sig_key: tuple = ()                               # ideas with the same key see the same signals


def wants_news(cand, fam) -> bool:
    """True when a family's ``signals`` is handed the minutes-to-news array:
    an idea that avoids the news, or a news family (as the past test does)."""
    return cand.news == "AVOID_15M" or "NEWS" in fam.code


def signal_key(cand, fam) -> tuple:
    """What a family's signals depend on: its code, the idea's own settings, its
    session and - for a family that reads it (``reads_news``) - whether it is
    handed the news. NOT the exit, the markets or the news rule's skip (those act
    after the signal, per idea): a family reads only its own settings and the
    session from ``p``, and only a news family reads the news array (a test reads
    every family's source to keep it so). Ideas with equal keys share one
    signals call, so 630 ideas are about 70 calls a market a bar, not 630."""
    return (fam.code, tuple(cand.params), cand.session,
            wants_news(cand, fam) if getattr(fam, "reads_news", True) else None)


def kind_symbols(universe: dict, kind: str) -> list:
    """Every stored market of a kind, in the universe's order (at most 24)."""
    return [name for name, e in universe.items() if e.kind == kind][:MAX_SYMBOLS]


class Practice:
    """The practice engine, one minute at a time. ``feed`` answers ``bars`` and
    ``ticks`` (a ``prices.PriceFeed``, or a stand-in in the tests); ``now_fn`` is
    the wall clock."""

    def __init__(self, paths: LabPaths, feed, *, settings: Optional[LabSettings] = None,
                 ledger: Optional[LabLedger] = None, calendar=None,
                 now_fn: Optional[Callable[[], dt.datetime]] = None,
                 log: Optional[Callable[[str], None]] = None, prime_bars: int = PRIME_BARS):
        self.paths = paths
        self.feed = feed
        self.settings = settings or LabSettings.load(paths.data_dir)
        self.ledger = ledger or LabLedger(paths.sqlite)
        self.store = BarStore(paths)
        self.calendar = calendar
        self.now_fn = now_fn or (lambda: dt.datetime.now(UTC))
        self._log = log or (lambda msg: print(msg, flush=True))
        self.prime_bars = int(prime_bars)
        self.universe = {e.name: e for e in load_universe(paths)}
        self.insts: dict = {}
        self.series: dict = {}
        self.slots: dict = {}
        self.by_symbol: dict = {}                     # symbol -> [the slots that watch it]
        self.open: dict = {}                          # (cid, symbol) -> Open
        self._dirty: set = set()                      # open positions whose stop or peak moved this minute
        self.signal_calls = 0                         # family.signals calls (shared across ideas)
        self.done_t: dict = {}                        # symbol -> last bar walked through
        self.last_bar: dict = {}                      # symbol -> newest finished bar seen (minute)
        self.roster_key = None
        self.day = ""
        self.feed_ok = True
        self.last_error = ""
        self.skipped: dict = {}
        self.source = "SYNTHETIC" if getattr(feed, "synthetic", False) else "bridge"
        self._news_days: Optional[set] = None
        self._restored = False

    # ------------------------------------------------------------- helpers --
    def log(self, msg: str) -> None:
        self._log(f"{utc_stamp(self.now_fn())} lab practice: {msg}")

    def skip(self, why: str) -> None:
        self.skipped[why] = self.skipped.get(why, 0) + 1

    def inst(self, symbol: str):
        got = self.insts.get(symbol)
        if got is None:
            e = self.universe[symbol]
            got = self.insts[symbol] = sim.Inst(symbol, e.kind, e.spec)
        return got

    def _state_path(self):
        return self.paths.root / "practice_state.json"

    # ------------------------------------------------------------- roster --
    def _roster_rows(self) -> list:
        """The ideas that practise, each with the markets it watches.

        ``practise_all`` (the default): every idea in the ledger, binned ones
        included - its stage is read, never changed - on every stored market of
        its kind; except those the weekly refresh RETIRED (``refresh.py``). Otherwise the nightly's roster at PRACTICE or READY FOR LIVE,
        within ``practice_slots``, on the markets the nightly gave it."""
        if self.settings.practise_all:
            rows = [dict(r) for r in self.ledger.query(           # the nightly's roster first, in its order
                "SELECT c.id AS id, c.name AS name, c.stage AS stage, c.def_json AS def_json FROM candidates c"
                " LEFT JOIN roster r ON r.candidate_id=c.id WHERE c.retired_utc IS NULL"     # retired: off the pool
                " ORDER BY r.slot IS NULL, r.slot, c.id")]
            for r in rows:
                r["symbols"] = None                    # its kind's markets, once the definition is read
            return rows
        rows = [dict(r) for r in self.ledger.query(
            "SELECT r.*, c.name AS name, c.stage AS stage, c.def_json AS def_json, c.id AS id"
            " FROM roster r JOIN candidates c ON c.id=r.candidate_id ORDER BY r.slot")]
        rows = [r for r in rows if r["stage"] in (PRACTICE, READY_FOR_LIVE)]
        rows = rows[:max(0, int(self.settings.practice_slots))]
        for r in rows:
            try:
                got = json.loads(r["symbols_json"] or "[]")
            except ValueError:
                got = []
            r["symbols"] = [s for s in got if s in self.universe] if isinstance(got, list) else []
        return rows

    def load_roster(self) -> bool:
        """Read who practises. True when it changed (an idea came, went or moved stage)."""
        rows = self._roster_rows()
        key = (bool(self.settings.practise_all),
               tuple((r["id"], r["stage"], tuple(r["symbols"] or ())) for r in rows))
        if key == self.roster_key:
            return False
        self.roster_key = key
        slots = {}
        for r in rows:
            try:
                c = cand_of(r)
                fam = family_of(c)
            except Exception as exc:
                self.log(f"{r['id']} cannot practise: {exc}")
                continue
            syms = r["symbols"] if r["symbols"] is not None else kind_symbols(self.universe, c.markets)
            old = self.slots.get(r["id"])
            slot = Slot(r["id"], r["name"], c, fam, sim.family_params(c), list(syms), r["stage"],
                        wants_news=wants_news(c, fam), sig_key=signal_key(c, fam))
            if old is not None:
                slot.next_ok, slot.entries = old.next_ok, old.entries
            slots[r["id"]] = slot
        self.slots = slots
        self.by_symbol = {}
        for slot in slots.values():
            for sym in slot.symbols:
                self.by_symbol.setdefault(sym, []).append(slot)
        for key2 in [k for k in self.open if k[0] not in self.slots]:
            self.log(f"{key2[0]} left the roster with a position open on {key2[1]}: it closes at the next bar")
        self.log(f"roster: {len(self.slots)} idea(s) on {len(self.symbols())} market(s)"
                 + (" (every idea practises)" if self.settings.practise_all else ""))
        return True

    def symbols(self) -> list:
        out = list(self.by_symbol)
        for (_cid, sym) in self.open:
            if sym not in out and sym in self.universe:
                out.append(sym)
        return out

    # ------------------------------------------------------------- series --
    def _hours(self, symbol: str) -> list:
        cut = load_cut(self.paths)
        if cut is None or not self.store.months(symbol):
            return [0] * 24
        try:
            return self.store.hour_table(symbol, day_min(cut.cut_day))
        except Exception:
            return [0] * 24

    def _news(self, symbol: str):
        if self.calendar is None:
            return None
        try:
            return FEAT.news_inputs(self.calendar, symbol, self.universe[symbol].spec)
        except Exception:
            return None

    def prime(self, symbol: str, now_min: int) -> None:
        """A fresh series for a market: recent stored bars, then the bridge's own
        bars since, up to the last finished minute. Nothing is traded on these."""
        entry = self.universe[symbol]
        ser = FEAT.LiveSeries(entry, FEAT.SETS, news=self._news(symbol), hours=self._hours(symbol))
        point = float(entry.spec.point)
        rows: list = []
        for ym in reversed(self.store.months(symbol)):
            try:
                cols = self.store.read_month(symbol, ym)
            except Exception:
                break
            rows = [(cols.t[i], cols.o[i], cols.h[i], cols.l[i], cols.c[i], cols.s[i], cols.v[i])
                    for i in range(cols.n)] + rows
            if len(rows) >= self.prime_bars:
                break
        rows = [r for r in rows[-self.prime_bars:] if r[0] < now_min]
        last = rows[-1][0] if rows else None
        want = READ_BACK_MAX if last is None else min(READ_BACK_MAX, max(3, now_min - last + 2))
        for r in self._read(symbol, want, point):
            if r[0] < now_min and (last is None or r[0] > last):
                rows.append(r)
                last = r[0]
        self.series[symbol] = ser
        for r in rows:
            ser.push(*r)
            self._walk(symbol, r)              # a position restored after a restart catches up
        if rows:
            self.last_bar[symbol] = rows[-1][0]
            self.done_t.setdefault(symbol, rows[-1][0])
        self.log(f"{symbol}: primed with {len(rows)} bars")

    def _read(self, symbol: str, count: int, point: float) -> list:
        """Bars from the feed as store rows (epoch minute, points ...), oldest first."""
        out = []
        for b in self.feed.bars(symbol, TF.M1, int(count)) or []:
            row = bar_row(b, point)
            if row is not None:
                out.append(row)
        out.sort(key=lambda r: r[0])
        return out

    # -------------------------------------------------------------- restore --
    def restore(self) -> None:
        """Open positions after a restart: the rows in practice_open, with the full
        position state from practice_state.json when it matches; a row the state
        file does not know is rebuilt from the row itself and the idea's exit rule."""
        try:
            state = json.loads(self._state_path().read_text(encoding="utf-8"))
        except (OSError, ValueError):
            state = {}
        saved = state.get("positions") or {}
        for sym, t in (state.get("done_t") or {}).items():
            self.done_t[sym] = int(t)
        for cid, info in (state.get("slots") or {}).items():
            if cid in self.slots:
                self.slots[cid].next_ok = {k: int(v) for k, v in (info.get("next_ok") or {}).items()}
                self.slots[cid].entries = {k: int(v) for k, v in (info.get("entries") or {}).items()}
        for r in self.ledger.query("SELECT * FROM practice_open"):
            r = dict(r)
            sym, cid = r["symbol"], r["candidate_id"]
            if sym not in self.universe:
                continue
            key = f"{cid}|{sym}"
            inst = self.inst(sym)
            pt = inst.point
            got = saved.get(key)
            pos = sim.Pos()
            if got and got.get("opened_utc") == r["opened_utc"]:
                for k, v in got["pos"].items():
                    setattr(pos, k, v)
                o = Open(cid, sym, pos, got["day"], got.get("signal_bar_utc", ""), got.get("tick_json", "{}"),
                         int(got.get("last_t") or _iso_min(r["opened_utc"]) - 1))
            else:
                crow = self.ledger.candidate(cid)
                c = cand_of(crow) if crow else None
                rule = sim.EXIT_RULES.get(c.exit if c else "t1", sim.EXIT_RULES["t1"])
                pos.side, pos.vol = int(r["side"]), float(r["volume"])
                pos.entry, pos.stop = float(r["entry"]) / pt, float(r["stop"]) / pt
                pos.stop0 = pos.stop
                pos.target = None if r["target"] is None else float(r["target"]) / pt
                pos.peak = float(r["peak"]) / pt if r["peak"] is not None else pos.entry
                pos.D = abs(pos.entry - pos.stop) or 1.0
                pos.trail = rule[2]
                pos.trail_dist = rule[3] * pos.D if rule[2] else 0.0
                pos.hold = rule[4]
                pos.t_open = _iso_min(r["opened_utc"])
                pos.j_open = pos.t_open
                pos.spread_in = inst.dflt_spread
                pos.slip_in = max(SLIP_MIN_POINTS, SLIP_SPREAD_SHARE * pos.spread_in)
                pos.assumed = True
                pos.fill_source = "tick"
                o = Open(cid, sym, pos, _uk_date(pos.t_open), "", "{}", pos.t_open - 1)
                self.log(f"{cid} {sym}: open position rebuilt from its row (no saved state)")
            self.open[(cid, sym)] = o
        self._restored = True

    def save_state(self) -> None:
        positions = {}
        for (cid, sym), o in self.open.items():
            p = o.pos
            positions[f"{cid}|{sym}"] = {
                "opened_utc": iso_utc(p.t_open), "day": o.day, "signal_bar_utc": o.signal_bar_utc,
                "tick_json": o.tick_json, "last_t": o.last_t,
                "pos": {k: getattr(p, k) for k in sim.Pos.__slots__ if hasattr(p, k)}}
        atomic_write_json(self._state_path(), {
            "written_utc": utc_stamp(self.now_fn()), "positions": positions,
            "done_t": self.done_t,
            # only the ideas with something to remember (most of ~630 have nothing on a quiet day)
            "slots": {cid: {"next_ok": s.next_ok, "entries": s.entries} for cid, s in self.slots.items()
                      if s.next_ok or s.entries}})

    # -------------------------------------------------------------- records --
    def _ticket(self) -> int:
        got = self.ledger.one("SELECT MAX(ticket) FROM practice_trades")[0]
        return max(TICKET_BASE, int(got or 0) + 1)

    def _book(self, o: Open, fill: tuple, t_exit: int) -> None:
        """Close a practice position: the trade, its day, and the open row gone."""
        slot = self.slots.get(o.cid)
        risk = float(slot.cand.risk_gbp) if slot else float(self.settings.risk_gbp)
        inst = self.inst(o.symbol)
        tr = sim.close_trade(inst, o.pos, o.symbol, risk, fill, t_exit, t_exit, o.day, False)
        spread_paid = o.pos.spread_in if o.pos.side > 0 else fill[3]
        slip_paid = o.pos.slip_in + fill[2]
        commission = tr.gross_gbp - tr.net_gbp
        with self.ledger._tx():
            self.ledger.conn.execute(
                "INSERT INTO practice_trades(ticket, candidate_id, symbol, side, volume, entry, stop, target,"
                " opened_utc, closed_utc, exit_price, exit_reason, gross_gbp, commission_gbp, spread_points,"
                " slippage_points, net_gbp, risk_gbp, r, signal_bar_utc, tick_at_signal_json, fill_source,"
                " mode, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (self._ticket(), o.cid, o.symbol, tr.side, tr.volume, tr.entry, tr.stop, tr.target,
                 tr.opened_utc, tr.closed_utc, tr.exit_price, tr.exit_reason, tr.gross_gbp, commission,
                 spread_paid, slip_paid, tr.net_gbp, risk, tr.r, o.signal_bar_utc, o.tick_json,
                 tr.fill_source, "PRACTICE", self.source))
            self.ledger.conn.execute(
                "INSERT INTO practice_days(candidate_id, day, net_gbp, trades) VALUES (?,?,?,1)"
                " ON CONFLICT(candidate_id, day) DO UPDATE SET net_gbp=net_gbp+excluded.net_gbp,"
                " trades=trades+1", (o.cid, o.day, tr.net_gbp))
            self.ledger.conn.execute("DELETE FROM practice_open WHERE candidate_id=? AND symbol=?",
                                     (o.cid, o.symbol))
        self.open.pop((o.cid, o.symbol), None)
        if slot is not None:
            slot.next_ok[o.symbol] = t_exit + COOLDOWN_BARS
        self.log(f"{o.cid} {o.symbol}: closed {tr.exit_reason} {tr.net_gbp:+.2f} GBP ({tr.r:+.2f} R)")

    def _record_open(self, o: Open) -> None:
        p, pt = o.pos, self.inst(o.symbol).point
        nd = self.inst(o.symbol).digits + 3
        self.ledger.execute(
            "INSERT OR REPLACE INTO practice_open(candidate_id, symbol, side, volume, entry, stop, target, peak,"
            " opened_utc) VALUES (?,?,?,?,?,?,?,?,?)",
            (o.cid, o.symbol, p.side, p.vol, round(p.entry * pt, nd), round(p.stop * pt, nd),
             None if p.target is None else round(p.target * pt, nd), round(p.peak * pt, nd),
             iso_utc(p.t_open)))

    def _update_open(self, o: Open) -> None:
        """Noted, and written with the minute's others in ONE transaction (``_flush_open``):
        with hundreds of ideas open, a commit per position per bar was most of the minute."""
        self._dirty.add((o.cid, o.symbol))

    def _flush_open(self) -> None:
        rows = []
        for key in self._dirty:
            o = self.open.get(key)
            if o is None:                            # closed since: its row is already gone
                continue
            p, pt = o.pos, self.inst(o.symbol).point
            nd = self.inst(o.symbol).digits + 3
            rows.append((round(p.stop * pt, nd), round(p.peak * pt, nd), o.cid, o.symbol))
        self._dirty.clear()
        if rows:
            with self.ledger._tx():
                self.ledger.conn.executemany(
                    "UPDATE practice_open SET stop=?, peak=? WHERE candidate_id=? AND symbol=?", rows)

    def roll_day(self, day: str) -> None:
        """A new UK day: a zero row for each practising idea (a weekday only), in one
        transaction; each idea's entry counts of earlier days are let go."""
        if day == self.day:
            return
        self.day = day
        for slot in self.slots.values():
            slot.entries = {k: v for k, v in slot.entries.items() if k.endswith("|" + day)}
        if dt.date.fromisoformat(day).weekday() >= 5:
            return
        if self.slots:
            with self.ledger._tx():
                self.ledger.conn.executemany(
                    "INSERT OR IGNORE INTO practice_days(candidate_id, day, net_gbp, trades) VALUES (?,?,0,0)",
                    [(cid, day) for cid in self.slots])
        for sym, ser in list(self.series.items()):
            if ser.n > RESET_AT:
                del self.series[sym]                  # rebuilt (shorter) at the next minute

    # ---------------------------------------------------------- the minute --
    def minute(self, now: Optional[dt.datetime] = None) -> dict:
        """One minute's work. Returns what it did (for the tests and the log)."""
        now = now or self.now_fn()
        now_min = epoch_min(now)
        done = {"bars": 0, "entries": 0, "exits": 0, "weekend": False}
        self.load_roster()
        if not self._restored:
            self.restore()
        today = _uk_date(now_min)
        self.roll_day(today)
        if dt.date.fromisoformat(today).weekday() >= 5:
            done["weekend"] = True
            if self.open:                              # held over Friday's flat (the process was down then)
                self._weekend_flat(now_min)
            return done
        wants: list = []
        try:
            for sym in self.symbols():
                if sym not in self.universe:
                    continue
                if sym not in self.series:
                    self.prime(sym, now_min)
                done["bars"] += self._advance(sym, now_min, done)
                wants.extend(self._signals(sym, now_min))
            if wants:
                done["entries"] += self._enter(wants, now_min)
            self.feed_ok, self.last_error = True, ""
        except Exception as exc:                       # the feed went away: say so, try again next minute
            self.feed_ok, self.last_error = False, f"{type(exc).__name__}: {exc}"
            self.log(f"feed: {self.last_error}")
        self._flush_open()
        self.save_state()
        return done

    def _advance(self, sym: str, now_min: int, done: dict) -> int:
        """Push the finished bars not seen yet, and walk the open positions through them."""
        ser = self.series[sym]
        point = float(self.universe[sym].spec.point)
        last = ser.t[ser.n - 1] if ser.n else None
        newest = now_min - 1                           # the last finished minute
        if last is not None and last >= newest:
            rows = []
        else:
            want = 3 if last is None or newest - last <= 2 else min(READ_BACK_MAX, newest - last + 2)
            rows = [r for r in self._read(sym, want, point) if r[0] <= newest and (last is None or r[0] > last)]
        for r in rows:
            ser.push(*r)
            self.last_bar[sym] = r[0]
            self._walk(sym, r)
            self.done_t[sym] = r[0]
        # positions whose market is no longer read still need walking: nothing to walk them with
        self._flat_clock(sym, now_min)                 # its bars are read up to the minute: the flat on the clock
        return len(rows)

    def _bar_spread(self, sym: str, t: int, s) -> float:
        """The spread a bar is walked with: its own, floored at that UTC hour's median (the default for its
        kind when the bar recorded none)."""
        med = float(self.series[sym].hours[(t // 60) % 24])
        return max(float(s), med) if s > 0 else max(self.inst(sym).dflt_spread, med)

    def _walk(self, sym: str, r: tuple) -> None:
        t, o, h, l, c, s, _v = r
        if not self.open:
            return
        for key in [k for k in self.open if k[1] == sym]:
            op = self.open[key]
            if t <= op.last_t:
                continue
            pos = op.pos
            flat = self._flat_of(op)
            if flat is not None and t >= flat and self._flat_close(op, flat):
                continue                               # the flat came before this bar: closed at the flat
            sp = self._bar_spread(sym, t, s)
            gap = t - op.last_t
            if gap > FEED_GAP_MINUTES and op.last_t >= pos.t_open:
                self._book(op, sim.feed_gap_fill(pos, float(c), sp), t)
                continue
            off_roster = op.cid not in self.slots
            last = (flat is None or t + 1 >= flat) or off_roster      # a day that cannot be read: flat at once
            fill = sim.step(pos, t, float(o), float(h), float(l), float(c), sp, last)
            op.last_t = t
            if fill is not None:
                self._book(op, fill, t)
            else:
                self._update_open(op)

    # ------------------------------------------------- the flat, on the clock --
    def _flat_of(self, op) -> Optional[int]:
        """The minute a position's UK day is flat by (21:55 UK, 21:00 on a Friday: ``flat_min``), from the
        day it opened on - its open's own UK day when that cannot be read; None when neither can."""
        try:
            return flat_min(op.day)
        except (TypeError, ValueError):
            pass
        try:
            return flat_min(_uk_date(int(op.pos.t_open)))
        except (AttributeError, TypeError, ValueError, OverflowError):
            return None

    def _flat_clock(self, sym: str, now_min: int) -> None:
        """14 Oct: the 21:55 UK flat (21:00 on a Friday) on the CLOCK, once this market's bars have been read
        up to the minute. A position is otherwise flattened only by a bar that closes at the flat; a market
        that stops printing before it (AUS200's last bar is 19:59 UTC) never gives one, and the position was
        held to its next bar - the next session - and closed FEED_GAP: held overnight."""
        for key in [k for k in self.open if k[1] == sym]:
            op = self.open.get(key)
            if op is None:
                continue
            flat = self._flat_of(op)
            if flat is not None and now_min >= flat:
                self._flat_close(op, flat)

    def _flat_close(self, op, flat: int) -> bool:
        """Close a practice position at the flat: at its market's last bar before the flat, at that bar's
        close with the spread and slip against us (the simulator's own fill for an exit at a bar's close) -
        what the past test does at its last bar before the flat - booked SESSION_END at the flat minute.
        False (still open, tried again next minute) when no bar before the flat is known."""
        ser = self.series.get(op.symbol)
        j = bisect.bisect_left(ser.t, flat) - 1 if ser is not None and ser.n else -1
        if j < 0 or int(ser.t[j]) < int(op.last_t):
            # no bar before the flat, or only bars older than the one it was last walked through (a restart whose
            # bar read came back empty holds only stored days): never book a price from before the position -
            # that would be invented performance (review, 15 Oct). Tried again next minute.
            if not op.flat_waiting:                                    # said once, not every minute of a long wait
                op.flat_waiting = True
                self.log(f"{op.cid} {op.symbol}: the flat has come but no price since it was last walked is known: "
                         f"waiting for one")
            return False
        t, c = int(ser.t[j]), float(ser.c[j])
        price, _gap, slip, spread = sim.feed_gap_fill(op.pos, c, self._bar_spread(op.symbol, t, ser.s[j]))
        when = minute_to_dt(flat).astimezone(TZ_LONDON).strftime("%H:%M")
        self.log(f"{op.cid} {op.symbol}: the {when} UK flat came with no bar from its market since "
                 f"{iso_utc(t)}: closed on the clock at that bar's close")
        self._book(op, (price, "SESSION_END", slip, spread), flat - 1)    # stamped at the flat (an exit at a close)
        return True

    def _weekend_flat(self, now_min: int) -> None:
        """A position still open on a UK Saturday or Sunday: its flat has come. Its market's bars are read
        (the ones it missed walked) and it is closed on the clock; no signal, no entry."""
        try:
            for sym in sorted({k[1] for k in self.open}):
                if sym not in self.universe:
                    continue
                if sym not in self.series:
                    self.prime(sym, now_min)
                self._advance(sym, now_min, {})
            self.feed_ok, self.last_error = True, ""
        except Exception as exc:                       # the feed went away: tried again next minute
            self.feed_ok, self.last_error = False, f"{type(exc).__name__}: {exc}"
            self.log(f"feed: {self.last_error}")
        self._flush_open()
        self.save_state()

    def _signals(self, sym: str, now_min: int) -> list:
        """Every idea's signals on this market's newest finished bar. The bar's
        features, day views and session spans are worked out once; a family's
        ``signals`` runs once for each ``signal_key`` and session span, and
        every idea with that key gets the answer (they would all compute the
        very same list)."""
        slots = self.by_symbol.get(sym)
        ser = self.series[sym]
        if not slots or not ser.n:
            return []
        i = ser.n - 1
        t_i = ser.t[i]
        if t_i != now_min - 1:                         # not the bar that has just finished: too late to act
            return []
        dvs = ser.dayviews()
        dv = dvs[-1] if dvs else None
        if dv is None or not (dv.i0 <= i < dv.i1):
            return []
        if dt.date.fromisoformat(dv.uk).weekday() >= 5:
            return []
        out: list = []
        spans: dict = {}
        shared: dict = {}
        news_known = None
        for slot in slots:
            c = slot.cand
            if c.news == "AVOID_15M":
                if news_known is None:
                    news_known = self._news_known(dv.uk)
                if not news_known:
                    self.skip("news_unknown_day")
                    continue
            span = spans.get(c.session)
            if span is None:
                span = spans[c.session] = dv.span(c.session)
            lo, hi = span
            # on a day still being traded a span of one bar is a session that has just opened (its later
            # bars are still to come), not one too short to trade: the past test, which sees the whole day,
            # takes a signal on a session's first bar (a London breakout on the open), so practice does too
            if not (lo <= i < hi) or (hi - lo < 2 and not dv.open_ended):
                continue
            key = (slot.sig_key, lo, hi)
            sigs = shared.get(key)
            if sigs is None:
                news = ser.f("news_min") if slot.wants_news else None
                self.signal_calls += 1
                try:
                    sigs = [sg for sg in slot.family.signals(dv, slot.params, (lo, hi), news) if sg.i == i]
                except Exception as exc:
                    self.log(f"{slot.cid} {sym}: the family's signals failed: {exc}")
                    sigs = []
                shared[key] = sigs
            for sg in sigs:
                out.append((slot, sym, i, sg, dv))
        return out

    def _news_known(self, day: str) -> bool:
        if self.calendar is None:
            return False
        if self._news_days is None or day not in self._news_days:
            try:
                self._news_days = FEAT.known_days(self.calendar)
            except Exception:
                self._news_days = set()
        return day in self._news_days

    def _enter(self, wants: list, now_min: int) -> int:
        """Fill the signals on ONE ticks call: the real ask for a buy, the bid for a
        sell, plus slippage; the same caps as the past test, per idea (each idea is
        simulated on its own: there is no cap across the lab)."""
        per_idea: dict = {}
        for (cid, _sym) in self.open:
            per_idea[cid] = per_idea.get(cid, 0) + 1
        ok_wants = []
        for slot, sym, i, sg, dv in wants:
            ser = self.series[sym]
            if (slot.cid, sym) in self.open:
                self.skip("busy_or_cooldown")
                continue
            if ser.t[i] < slot.next_ok.get(sym, 0):
                self.skip("busy_or_cooldown")
                continue
            if per_idea.get(slot.cid, 0) >= MAX_OPEN_PER_CANDIDATE:
                self.skip("max_open")
                continue
            dkey = f"{sym}|{dv.uk}"
            if slot.entries.get(dkey, 0) >= MAX_ENTRIES_PER_DAY:
                self.skip("day_cap")
                continue
            if slot.cand.news == "AVOID_15M":
                nm = ser.f("news_min")[i]
                if 0 <= nm <= NEWS_AVOID_MINUTES:
                    self.skip("news_soon")
                    continue
            if ser.t[i] + 1 >= flat_min(dv.uk):
                self.skip("late_entry")
                continue
            ok_wants.append((slot, sym, i, sg, dv))
        if not ok_wants:
            return 0
        ticks = self.feed.ticks(sorted({w[1] for w in ok_wants})) or {}
        n = 0
        for slot, sym, i, sg, dv in ok_wants:
            if per_idea.get(slot.cid, 0) >= MAX_OPEN_PER_CANDIDATE:      # filled earlier in this same minute
                self.skip("max_open")
                continue
            tk = ticks.get(sym)
            if tk is None or not tk.bid or not tk.ask:
                self.skip("no_tick")
                continue
            ser, inst = self.series[sym], self.inst(sym)
            pt = inst.point
            bid, ask = tk.bid / pt, tk.ask / pt
            hour_med = float(ser.hours[(now_min // 60) % 24])
            spread = max(ask - bid, hour_med, 0.0)
            slip = max(SLIP_MIN_POINTS, SLIP_SPREAD_SHARE * spread)
            price = ask + slip if sg.side > 0 else bid - slip
            c = slot.cand
            vw = float(ser.f(f"vwap_{c.session}")[i]) if c.exit == "vwap" else 0.0
            pos = sim.enter(inst, c.exit, c.risk_gbp, sg.side, float(ser.c[i]), sg.stop_points,
                            float(ser.f("atr5")[i]), price, spread, False, ser.day_median_spread(dv),
                            now_min, now_min, vwap=vw, entry_price=price)
            if isinstance(pos, str):
                self.skip(pos)
                continue
            if pos.stop is None:                    # every practice position has a stop
                self.skip("no_stop")
                continue
            pos.fill_source = "tick"
            pos.ref = sg.ref
            o = Open(slot.cid, sym, pos, dv.uk, iso_utc(ser.t[i]),
                     json.dumps({"bid": tk.bid, "ask": tk.ask, "time": tk.time.isoformat() if tk.time else ""}),
                     now_min - 1)
            self.open[(slot.cid, sym)] = o
            per_idea[slot.cid] = per_idea.get(slot.cid, 0) + 1
            dkey = f"{sym}|{dv.uk}"
            slot.entries[dkey] = slot.entries.get(dkey, 0) + 1
            self._record_open(o)
            n += 1
            self.log(f"{slot.cid} {sym}: opened {'BUY' if sg.side > 0 else 'SELL'} {pos.vol} at "
                     f"{price * pt:.{inst.digits}f}, stop {pos.stop * pt:.{inst.digits}f} ({sg.ref})")
        return n

    # --------------------------------------------------------------- status --
    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = now or self.now_fn()
        today = now.astimezone(TZ_LONDON).date().isoformat()
        days = {r["candidate_id"]: r for r in self.ledger.query(
            "SELECT candidate_id, net_gbp, trades FROM practice_days WHERE day=?", (today,))}
        n_open: dict = {}
        for (cid, _sym) in self.open:
            n_open[cid] = n_open.get(cid, 0) + 1
        roster = []
        for slot in self.slots.values():
            row = days.get(slot.cid)
            trades = int(row["trades"]) if row else 0
            if self.settings.practise_all and not trades and not n_open.get(slot.cid):
                continue                               # ~630 ideas: only the ones busy today are listed
            roster.append({"id": slot.cid, "name": slot.name, "stage": slot.stage, "symbols": slot.symbols,
                           "open": n_open.get(slot.cid, 0), "today_trades": trades,
                           "today_gbp": float(row["net_gbp"]) if row else 0.0})
        roster.sort(key=lambda x: (-x["today_gbp"], x["id"]))
        opened = []
        for (cid, sym), o in self.open.items():
            p, pt = o.pos, self.inst(sym).point
            opened.append({"id": cid, "name": self.slots[cid].name if cid in self.slots else "", "symbol": sym,
                           "side": int(p.side), "volume": p.vol,
                           "entry": round(p.entry * pt, 8), "stop": round(p.stop * pt, 8),
                           "target": None if p.target is None else round(p.target * pt, 8),
                           "opened_utc": iso_utc(p.t_open)})
        newest = max(self.last_bar.values()) if self.last_bar else None
        age = None if newest is None else round((now - minute_to_dt(newest + 1)).total_seconds(), 1)
        weekend = now.astimezone(TZ_LONDON).weekday() >= 5
        return {"mode": self.settings.mode, "practice": True, "roster": roster, "practising": len(self.slots),
                "practise_all": bool(self.settings.practise_all),
                "markets": len(self.by_symbol), "open_positions": opened,
                "last_bar_utc": {s: iso_utc(t) for s, t in sorted(self.last_bar.items())},
                "feed_ok": self.feed_ok, "feed_age_s": age, "last_error": self.last_error,
                "resting": "the weekend: nothing practises on a Saturday or Sunday" if weekend else "",
                "synthetic": self.source == "SYNTHETIC", "skipped": dict(self.skipped),
                "note": "practice: live prices, no money; the lab cannot place an order"}

    def write(self, now: Optional[dt.datetime] = None) -> None:
        """The status file (the watchdog's sign of life) and the practice figures."""
        now = now or self.now_fn()
        write_status(self.paths, self.status(now), now)
        try:
            refresh_practice(self.paths, self.ledger, list(self.universe.values()), now)
        except Exception as exc:
            self.log(f"summary refresh failed: {exc}")

    # ------------------------------------------------------------------ run --
    def run(self, *, sleep: Callable[[float], None] = time.sleep, max_ticks: Optional[int] = None) -> int:
        """Every 30 seconds: the status; at 3 s past each minute, the minute's work
        first. Stops (exit 0) when data/lab.json says OFF."""
        ticks = 0
        self.write()
        while True:
            self.settings = LabSettings.load(self.paths.data_dir)
            if self.settings.mode == "OFF":
                self.log("data/lab.json says OFF: practice stops (open practice positions resume on the next start)")
                self.write()
                return 0
            now = self.now_fn()
            sec = now.second + now.microsecond / 1e6
            nxt = AT_SECOND if sec < AT_SECOND else (AT_SECOND + STATUS_EVERY_S if sec < AT_SECOND + STATUS_EVERY_S
                                                   else 60 + AT_SECOND)
            sleep(max(0.0, nxt - sec))
            now = self.now_fn()
            if int(round(nxt)) % 60 == AT_SECOND:
                try:
                    self.minute(now)
                except Exception as exc:
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    self.log(f"minute failed: {self.last_error}")
            self.write(now)
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                return 0


def _take_pid(paths: LabPaths) -> bool:
    """Write our pid; refuse when another practice process holds it."""
    try:
        other = int(paths.pid.read_text().strip())
    except (OSError, ValueError):
        other = 0
    if other and other != os.getpid() and pid_alive(other):
        try:
            with open(f"/proc/{other}/cmdline", "rb") as fh:
                if b"mintel.lab.practice" not in fh.read():
                    other = 0
        except OSError:
            pass
    if other and other != os.getpid() and pid_alive(other):
        return False
    paths.pid.parent.mkdir(parents=True, exist_ok=True)
    paths.pid.write_text(str(os.getpid()))
    return True


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="the lab's practice: live prices, no money")
    ap.add_argument("--config", default="data/config.json")
    args = ap.parse_args(argv)
    from ..config import Config
    cfg = Config.load(args.config)
    paths = LabPaths(cfg.ops.data_dir, cfg.ops.log_dir)
    settings = LabSettings.load(paths.data_dir)
    if settings.mode == "OFF":
        print("the lab is OFF (data/lab.json): practice does not start", flush=True)
        return 0
    if not paths.universe.exists():
        print("the lab has no markets yet (data/lab/universe.json): practice waits for the first night",
              flush=True)
        return 0
    if not _take_pid(paths):
        print("another lab practice process is running", flush=True)
        return 3
    try:
        os.nice(10)
    except (AttributeError, OSError):
        pass
    from .prices import from_config
    feed = from_config(cfg)
    feed.ping()                                     # learns whether the bridge is a simulated one
    calendar = None
    p = cfg.news.store_path
    cal_path = p if os.path.isabs(p) else str(paths.data_dir / os.path.basename(p))
    if os.path.exists(cal_path):
        try:
            from ..news.calendar_store import CalendarStore
            calendar = CalendarStore(cal_path)
        except Exception:
            calendar = None
    eng = Practice(paths, feed, settings=settings, calendar=calendar)
    eng.log("started (nice 10): live prices, no money")
    try:
        return eng.run()
    finally:
        try:
            if paths.pid.read_text().strip() == str(os.getpid()):
                paths.pid.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
