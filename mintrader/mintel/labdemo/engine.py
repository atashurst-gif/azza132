"""Lab ideas on the DEMO account: the promoted ones place genuine orders.

Runs inside the trader (``mintel/engine/trader.py``), which owns the broker
connection; built only when config.json's ``mode`` is DEMO, and never in a dry
run. Aaron, 14 Oct 2026: "lab ideas really trade on the demo".

THE SAME RULES AS PRACTICE. The engine is the lab's own practice engine
(``mintel.lab.practice.Practice``) pointed at the broker's prices instead of the
read-only feed, for the promoted ideas alone: the very same feature code
(``features.LiveSeries``), family ``signals``, caps (one position per market,
three open per idea, a five-bar pause after an exit, six entries per market per
UK day), news-avoid rule, and simulator exits (``sim.step``: the stop first, the
target, the trail, the time exit, the 21:55 UK flat - 21:00 on Fridays - and a
feed gap). The flat comes on the CLOCK, as practice (14 Oct): a position whose
market stops printing before it (AUS200's last bar is 19:59 UTC) is closed at
the broker at the flat - its stop untouched until the close fills; a close the
broker refuses (the market is shut) is sent again every minute, weekends
included, and booked from the deal when it fills. The lab itself is untouched and still cannot place an order; its
practice process keeps practising every idea, these included, on paper, so the
paper shadow and the demo can be compared side by side.

WHAT IS DIFFERENT ON THE DEMO:

* the entry is a genuine market order sent through ``botexec.LiveExecutor``
  with the STOP AT THE BROKER (and the fixed target, when the exit has one, as
  the broker's take-profit). The executor fails closed: a position whose stop
  the broker did not take is closed again at once;
* every send, change and close first asks the broker for the account and
  refuses unless it says ``is_demo`` AND config.json's mode is DEMO
  (``DemoOnlyExecutor``): real money is impossible from this path;
* each idea version trades under its own magic number from the reserved
  range 992001-992999 (``roster.py``), so no idea sees another's positions or a
  bot's;
* size = the idea's demo risk tier (0.5%, 1% or 2% of the broker's equity, set
  at its promotion) / (stop distance x money a point a lot from the broker's own
  instrument spec), floored to the lot step. That size is then made SMALLER,
  never larger, to fit the account's combined open risk (every position on the
  account, at its stop) under ``demo_risk_cap_pct`` (data/weekly.json, default
  10% of equity), to use at most half the broker's free margin, and to keep the
  account's margin level at or above config.json's risk.min_margin_level_pct
  (300%: the trader's MARGIN_LEVEL breaker, which would shut every bot's entries)
  with the new position counted at its own stop and 2% to spare. Skipped, with
  the reason logged and kept in ``demo_skips``, when even the broker's smallest
  lot would risk more than the tier allows, would not fit the cap or the
  margin, when the broker cannot say the margin, or when a position on the
  account has no stop. The trade's R is measured on the money really at its
  stop;
* a trail tightens the broker's stop (only ever in the trade's favour); a target,
  time, session-end (the flat, on the clock), feed-gap or demotion exit is a market close; a stop or
  take-profit the broker filled is noticed when the position is gone;
* every closed position is booked from the BROKER's closed-deal record
  (``LiveExecutor.closed_deal``) into data/labdemo/labdemo.sqlite, keyed by idea
  and version, with its £1,500-account equivalent beside it (``record.py``);
* a restart re-adopts every open position from ``demo_open`` (the simulator's
  state saved with it); a position gone while the trader was down is booked
  from its deal; a position under a lab-demo magic that no row accounts for is
  closed at once (fail closed, never adopted);
* an idea taken out of the roster (demoted, vetoed) has its positions closed at
  once: the clean cut of the bots' LIVE -> PAPER switch. The roster is re-read
  whenever it changes: no restart is needed;
* no new entry while data/HALT exists, the trader's account gate is shut (the
  daily-loss stop, the drawdown breaker, safe mode: ``Trader._bots_may_enter``)
  or the roster cannot be read; managing open positions is never gated.

Nothing here doubles after a loss, averages into a loser or grids: an idea's
size comes from its tier and the stop distance alone, and the caps above hold.
"""
from __future__ import annotations

import bisect
import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from ..botexec import Fill, LiveExecutor
from ..broker.base import Side
from ..clock import TZ_LONDON, to_utc
from ..lab import (COOLDOWN_BARS, FEED_GAP_MINUTES, MAX_ENTRIES_PER_DAY, MAX_OPEN_PER_CANDIDATE, NEWS_AVOID_MINUTES,
                   SLIP_MIN_POINTS, SLIP_SPREAD_SHARE, LabPaths, LabSettings, atomic_write_json, epoch_min,
                   iso_utc, minute_to_dt, utc_stamp)
from ..lab import sim
from ..lab.candidate import family_of
from ..lab.practice import AT_SECOND, PRIME_BARS, Open, Practice, Slot, kind_symbols, signal_key, wants_news
from ..lab.stages import cand_of
from ..lab.store import flat_min
from . import record as REC
from . import roster as R
from .roster import TIERS

log = logging.getLogger("mintel.labdemo")

TAG = "LAB"
RISK_CAP_PCT = 10.0                  # combined open risk on the account, % of equity (data/weekly.json demo_risk_cap_pct)
RISK_CAP_MAX = 20.0                  # the most a hand-edited setting can ask for
MARGIN_USE_MAX = 0.5                 # a new position may use at most half the free margin
MIN_MARGIN_LEVEL_PCT = 300.0         # ... and never take the margin level below this (config.json risk.min_margin_level_pct)
MARGIN_LEVEL_HEADROOM = 0.02         # ... with 2% to spare (300% -> 306%), the position counted at its own stop
SETTINGS = "weekly.json"


def risk_cap_pct(data_dir) -> float:
    """data/weekly.json ``demo_risk_cap_pct`` (0.5 to 20), else 10. A bad value reads as the default."""
    try:
        raw = json.loads((Path(data_dir) / SETTINGS).read_text(encoding="utf-8"))
        v = float(raw["demo_risk_cap_pct"])
    except Exception:
        return RISK_CAP_PCT
    return v if 0.5 <= v <= RISK_CAP_MAX else RISK_CAP_PCT


class NotDemo(Exception):
    pass


class DemoOnlyExecutor(LiveExecutor):
    """``LiveExecutor`` that asks the broker for the account immediately before
    EVERY send, stop change and close, and refuses unless the broker says the
    account is a demo one AND config.json's mode is DEMO. Fails closed: an
    account that cannot be read is not a demo account."""

    def __init__(self, broker, magic: int, tag: str, cfg_mode: str):
        super().__init__(broker, magic, tag)
        self.cfg_mode = str(cfg_mode or "").upper()

    def demo_ok(self) -> tuple[bool, str]:
        if self.cfg_mode != "DEMO":
            return False, f"config.json's mode is {self.cfg_mode or 'not set'}, not DEMO"
        try:
            acct = self.broker.account()
        except Exception as exc:
            return False, f"the account could not be read ({exc})"
        if getattr(acct, "is_demo", None) is not True:
            return False, f"account {getattr(acct, 'login', '?')} is not a demo account"
        return True, ""

    def _refused(self, why: str) -> Fill:
        self.last_error = f"refused: {why}"
        return Fill(False, message=self.last_error)

    def open(self, symbol, side, volume, stop, tp, tick, spec, now, comment=""):
        ok, why = self.demo_ok()
        if not ok:
            return self._refused(why)
        if not stop:
            return self._refused("no stop")                       # every order carries its stop
        return super().open(symbol, side, volume, stop, tp, tick, spec, now, comment)

    def modify_stop(self, ticket, stop, tp=None):
        ok, why = self.demo_ok()
        if not ok:
            self.last_error = f"refused: {why}"
            return False
        return super().modify_stop(ticket, stop, tp)

    def close(self, ticket, tick, spec, at_price=None, comment=""):
        ok, why = self.demo_ok()
        if not ok:
            return self._refused(why)
        return super().close(ticket, tick, spec, at_price, comment)


class BrokerPrices:
    """The two price questions the shared practice machinery asks, answered by the
    trader's broker (bars and ticks only)."""
    synthetic = False

    def __init__(self, broker):
        self._broker = broker

    def bars(self, symbol, tf, count, end=None):
        return list(self._broker.bars(symbol, tf, int(count)) or [])

    def ticks(self, symbols) -> dict:
        out = {}
        for s in symbols:
            try:
                out[s] = self._broker.tick(s)
            except Exception:
                out[s] = None
        return out


class _NoLedger:
    """Practice's ledger is the lab's sqlite; the demo engine never writes there
    (PAPER and DEMO never mixed). Every method that would use it is overridden."""

    def query(self, *a, **k):
        return []

    def one(self, *a, **k):
        return (None,)


@dataclass
class DemoOpen(Open):
    ticket: int = 0
    magic: int = 0
    version: int = 1
    risk_gbp: float = 0.0
    tier_pct: float = 0.0
    equity: float = 0.0
    plan: str = ""
    risk_allowed: float = 0.0        # the tier's money (equity x tier); risk_gbp is what is really at the stop
    closing: str = ""                # an exit sent (or the broker's), its deal not booked yet
    exit_retry: str = ""             # an exit the broker refused: tried again each minute
    broker_stop: float = 0.0         # the stop the broker holds (price)
    orphan_of: str = ""              # a position with no record, closed at once: the idea its magic belongs to
    exit_t: int = 0                  # the bar the exit happened on (the pause after it runs from there, as practice)


@dataclass
class DemoSlot(Slot):
    magic: int = 0
    version: int = 1
    tier_pct: float = 1.0
    plan: str = ""


class LabDemo(Practice):
    """The promoted lab ideas on the DEMO account. ``step(now)`` from the trader's
    loop; the minute's work runs once a minute, three seconds past it."""

    def __init__(self, data_dir, broker, *, cfg_mode: str,
                 entries_allowed: Optional[Callable[[], bool]] = None,
                 now_fn: Optional[Callable[[], dt.datetime]] = None,
                 log_fn: Optional[Callable[[str], None]] = None, calendar=None,
                 prime_bars: int = PRIME_BARS, primes_per_step: int = 3,
                 min_margin_level_pct: float = MIN_MARGIN_LEVEL_PCT):
        paths = LabPaths(data_dir)
        super().__init__(paths, BrokerPrices(broker), settings=LabSettings(), ledger=_NoLedger(),
                         calendar=calendar, now_fn=now_fn, log=log_fn or (lambda m: log.info("%s", m)),
                         prime_bars=prime_bars)
        self.broker = broker
        self.data_dir = Path(data_dir)
        self.cfg_mode = str(cfg_mode or "").upper()
        if self.cfg_mode != "DEMO":
            raise NotDemo(f"the lab-demo engine runs only when config.json's mode is DEMO (it is {self.cfg_mode})")
        self.entries_allowed = entries_allowed or (lambda: True)
        self.record = REC.DemoRecord(data_dir)
        self.execs: dict = {}
        self.roster_ok = True
        self._stamp = None
        self._minute_done = None
        self.primes_per_step = max(1, int(primes_per_step))
        self.source = "broker"
        self.skips_log: list = []
        # the account's own margin-level floor (config.json risk.min_margin_level_pct): an entry never takes the
        # account below it, or the trader's MARGIN_LEVEL breaker would shut every bot's entries
        self.min_margin_level_pct = float(min_margin_level_pct)
        self._saved_caps: dict = {}      # state.json's pauses and day caps for ideas not (yet) in the roster

    # ------------------------------------------------------------- helpers --
    def log(self, msg: str) -> None:
        self._log(f"{utc_stamp(self.now_fn())} lab demo: {msg}")

    def executor(self, magic: int) -> DemoOnlyExecutor:
        ex = self.execs.get(magic)
        if ex is None:
            ex = self.execs[magic] = DemoOnlyExecutor(self.broker, magic, TAG, self.cfg_mode)
        return ex

    def magics(self) -> tuple:
        """Every magic this engine trades or manages (the daily-loss stop counts them)."""
        out = {s.magic for s in self.slots.values()} | {o.magic for o in self.open.values()}
        return tuple(sorted(m for m in out if m))

    def _note_skip(self, slot, sym: str, why: str, now_min: int) -> None:
        self.skip(why.split(":")[0])
        msg = f"{slot.cid} {sym}: no entry - {why}"
        self.log(msg)
        try:
            self.record.skip(iso_utc(now_min), slot.cid, sym, why)
        except sqlite3.Error:
            pass

    def _broker_spec(self, sym: str):
        try:
            return self.broker.spec(sym)
        except Exception:
            return None

    # ------------------------------------------------------------- roster --
    def _idea_rows(self, ids) -> dict:
        """The promoted ideas' stored definitions, read-only from the lab's sqlite."""
        if not ids or not self.paths.sqlite.exists():
            return {}
        q = ",".join("?" for _ in ids)
        try:
            con = sqlite3.connect(f"file:{self.paths.sqlite}?mode=ro", uri=True, timeout=5.0)
            con.row_factory = sqlite3.Row
            try:
                return {r["id"]: dict(r) for r in con.execute(
                    f"SELECT id, name, stage, version, def_json FROM candidates WHERE id IN ({q})", tuple(ids))}
            finally:
                con.close()
        except sqlite3.Error as exc:
            raise R.RosterUnreadable(f"the lab's ideas could not be read ({exc})") from exc

    def load_roster(self) -> bool:
        """Re-read data/labdemo/roster.json when it changed. A roster (or the lab's
        own file) that cannot be read leaves every slot as it was and shuts new
        entries until it can: nothing is decided on a guess."""
        st = R.stamp(self.data_dir)
        if st == self._stamp and self.roster_key is not None:
            return False
        try:
            ideas = R.read_roster(self.data_dir, strict=True)
            rows = self._idea_rows(sorted(ideas))
        except R.RosterUnreadable as exc:
            if self.roster_ok:
                self.log(f"{exc}: no new entries until it can be read")
            self.roster_ok = False
            return False
        self.roster_ok = True
        self._stamp = st
        key = tuple(sorted((k, int(v["magic"]), float(v.get("tier_pct") or 0), int(v.get("version") or 1))
                           for k, v in ideas.items()))
        if key == self.roster_key:
            return False
        self.roster_key = key
        slots = {}
        for cid, ent in ideas.items():
            row = rows.get(cid)
            if row is None:
                self.log(f"{cid} is on the roster but not in the lab: not traded")
                continue
            if int(row.get("version") or 1) != int(ent.get("version") or 1):
                self.log(f"{cid}: the roster names version {ent.get('version')}, the lab holds "
                         f"{row.get('version')}: not traded")
                continue
            try:
                c = cand_of(row)
                fam = family_of(c)
            except Exception as exc:
                self.log(f"{cid} cannot trade: {exc}")
                continue
            tier = float(ent.get("tier_pct") or 0.0)
            if tier not in TIERS:
                self.log(f"{cid}: risk tier {tier}% is not one of {TIERS}: not traded")
                continue
            old = self.slots.get(cid)
            slot = DemoSlot(cid, row.get("name") or cid, c, fam, sim.family_params(c),
                            kind_symbols(self.universe, c.markets), row.get("stage") or "",
                            wants_news=wants_news(c, fam), sig_key=signal_key(c, fam),
                            magic=int(ent["magic"]), version=int(ent.get("version") or 1), tier_pct=tier,
                            plan=str(ent.get("plan") or ""))
            if old is not None:
                slot.next_ok, slot.entries = old.next_ok, old.entries
            elif cid in self._saved_caps:                 # restored before the roster could be read: kept
                self._apply_caps(slot, self._saved_caps.pop(cid))
            slots[cid] = slot
        gone = [o for (cid, _s), o in self.open.items() if cid not in slots]
        self.slots = slots
        self.by_symbol = {}
        for slot in slots.values():
            for sym in slot.symbols:
                self.by_symbol.setdefault(sym, []).append(slot)
        self.log(f"roster: {len(slots)} idea(s) on the demo, {len(self.by_symbol)} market(s)")
        for o in gone:                                    # demoted: the clean cut, closed at once
            if not o.closing:
                self._exit(o, "DEMOTED", self._now_min())
        return True

    def _now_min(self) -> int:
        return epoch_min(to_utc(self.now_fn()))

    # ------------------------------------------------------------ restore --
    def restore(self) -> None:
        """After a restart: every open row is re-adopted when the broker still holds
        it (its simulator state comes back with it), booked from its deal when it
        is gone, and any position under a lab-demo magic that no row accounts for
        is closed (fail closed; never adopted). Nothing is restored on a failed read."""
        try:
            at_broker = self._broker_positions()
        except Exception as exc:
            self.log(f"restore waits: the broker's positions could not be read ({exc})")
            return
        try:
            saved = json.loads((R.ldir(self.data_dir) / "state.json").read_text(encoding="utf-8")).get("slots") or {}
        except (OSError, ValueError, AttributeError):
            saved = {}
        for cid, info in saved.items():
            if not isinstance(info, dict):
                continue
            if cid in self.slots:
                self._apply_caps(self.slots[cid], info)
            else:                                          # the roster not read yet: applied when it is
                self._saved_caps[cid] = info
        for r in self.record.open_rows():
            sym, cid = r["symbol"], r["idea_id"]
            pos = sim.Pos()
            try:
                for k, v in json.loads(r["pos_json"] or "{}").items():
                    setattr(pos, k, v)
            except ValueError:
                pass
            o = DemoOpen(cid, sym, pos, r["day"] or "", r.get("signal_bar_utc") or "", "{}",
                         int(r.get("last_t") or 0), ticket=int(r["ticket"]), magic=int(r["magic"]),
                         version=int(r["version"]), risk_gbp=float(r["risk_gbp"] or 0.0),
                         tier_pct=float(r["tier_pct"] or 0.0), equity=float(r["equity_at_entry"] or 0.0),
                         plan=str(r.get("plan") or ""), closing=str(r.get("closing") or ""),
                         broker_stop=float(r["stop"] or 0.0), risk_allowed=float(r.get("risk_allowed_gbp") or 0.0))
            self.open[(cid, sym)] = o
            q = at_broker.get(o.ticket)
            if q is None:
                self.log(f"{cid} {sym}: ticket {o.ticket} closed while the trader was away: booked from its deal")
                o.closing = o.closing or "BROKER"
                self._settle(o)
            else:
                if q.sl and abs(float(q.sl) - o.broker_stop) > 0:
                    o.broker_stop = float(q.sl)
                self.log(f"{cid} {sym}: ticket {o.ticket} re-adopted (stop {q.sl})")
        self._sweep(at_broker)
        self._restored = True

    @staticmethod
    def _apply_caps(slot, info: dict) -> None:
        """An idea's saved pause after an exit and its entries a market a day (state.json)."""
        try:
            slot.next_ok = {k: int(v) for k, v in (info.get("next_ok") or {}).items()}
            slot.entries = {k: int(v) for k, v in (info.get("entries") or {}).items()}
        except (AttributeError, TypeError, ValueError):
            pass

    def _broker_positions(self) -> dict:
        """{ticket: position} of every position under a lab-demo magic. Raises when unreadable."""
        out = {}
        for q in self.broker.positions(None) or ():
            m = int(getattr(q, "magic", 0) or 0)
            if R.MAGIC_FIRST <= m <= R.MAGIC_LAST:
                out[int(q.ticket)] = q
        return out

    def _sweep(self, at_broker: dict) -> None:
        """A position under a lab-demo magic that no open row accounts for is closed at once."""
        mine = {o.ticket for o in self.open.values()}
        for t, q in at_broker.items():
            if t in mine:
                continue
            ex = self.executor(int(q.magic))
            tick = self._tick(q.symbol)
            fill = ex.close(t, tick, self._broker_spec(q.symbol), comment=f"{TAG} orphan") if tick else None
            ok = bool(fill and fill.ok)
            self.log(f"ticket {t} ({q.symbol}, magic {q.magic}) has no record here: "
                     + ("closed (fail closed, never adopted)" if ok else
                        f"the close failed ({fill.message if fill else 'no price'}); tried again next minute"))
            if ok:
                key = R.all_magics(self.data_dir).get(int(q.magic), "?@v1")
                idea, _, ver = key.partition("@v")
                o = DemoOpen(f"orphan:{t}", q.symbol, sim.Pos(), "", "", "{}", 0, ticket=t, magic=int(q.magic),
                             version=int(ver or 1), closing="ORPHAN", broker_stop=float(q.sl or 0.0), orphan_of=idea)
                o.pos.side = 1 if q.side is Side.BUY else -1
                o.pos.vol = float(q.volume)
                o.pos.t_open = epoch_min(to_utc(q.open_time)) if q.open_time else self._now_min()
                o.pos.entry = o.pos.stop0 = 0.0
                self.open[(f"orphan:{t}", q.symbol)] = o
                self._settle(o)

    def _tick(self, sym: str):
        try:
            return self.broker.tick(sym)
        except Exception:
            return None

    # ------------------------------------------------------------ records --
    def save_state(self) -> None:
        """Every open position's simulator state into demo_open (one transaction)."""
        rows = []
        for o in self.open.values():
            if not o.ticket or o.cid.startswith("orphan:"):
                continue
            p = o.pos
            rows.append((json.dumps({k: getattr(p, k) for k in sim.Pos.__slots__ if hasattr(p, k)}), int(o.last_t),
                         o.closing, o.ticket))
        if rows:
            with self.record.conn:
                self.record.conn.executemany("UPDATE demo_open SET pos_json=?, last_t=?, closing=? WHERE ticket=?",
                                             rows)
        # each idea's pause after an exit and its entries a market a day, so a restart keeps the caps; never
        # before the roster has been read once (no slots yet: the saved caps would be wiped)
        if self.roster_key is None:
            return
        slots = {cid: {"next_ok": sl.next_ok, "entries": sl.entries} for cid, sl in self.slots.items()
                 if sl.next_ok or sl.entries}
        if slots != getattr(self, "_saved_slots", None):
            try:
                atomic_write_json(R.ldir(self.data_dir) / "state.json", {"slots": slots})
                self._saved_slots = json.loads(json.dumps(slots))
            except OSError as exc:
                self.log(f"state not written: {exc}")

    def _flush_open(self) -> None:
        self._dirty.clear()                                # save_state writes every open row each minute

    def roll_day(self, day: str) -> None:
        if day == self.day:
            return
        self.day = day
        for slot in self.slots.values():
            slot.entries = {k: v for k, v in slot.entries.items() if k.endswith("|" + day)}
        for sym, ser in list(self.series.items()):
            if ser.n > 2 * self.prime_bars and not any(k[1] == sym for k in self.open):
                del self.series[sym]                       # rebuilt (shorter) at the next minute

    # ------------------------------------------------------------- the step --
    def step(self, now: Optional[dt.datetime] = None) -> list[str]:
        """Called from the trader's loop: the minute's work once a minute, at three
        seconds past it (the bar just finished is then in the broker's list)."""
        now = to_utc(now or self.now_fn())
        m = epoch_min(now)
        if self._minute_done == m or now.second < AT_SECOND:
            return []
        self._minute_done = m
        try:
            done = self.minute(now)
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.log(f"minute failed: {self.last_error}")
            return [f"lab demo: {self.last_error}"]
        self.write(now)
        return [f"lab demo: {done['entries']} entr{'y' if done['entries'] == 1 else 'ies'}"] if done["entries"] else []

    def minute(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.now_fn())
        now_min = epoch_min(now)
        done = {"bars": 0, "entries": 0, "exits": 0, "weekend": False}
        self.load_roster()
        if not self._restored:
            self.restore()
            if not self._restored:
                return done
        today = minute_to_dt(now_min).astimezone(TZ_LONDON).date().isoformat()
        self.roll_day(today)
        self._reconcile(now_min)
        if not self.slots and not self.open:
            return done
        if dt.date.fromisoformat(today).weekday() >= 5:
            done["weekend"] = True
            self._flat_all(now_min)                        # held over Friday's flat: closed (sent) now
            self.save_state()
            return done
        wants: list = []
        primed = 0
        try:
            for sym in self.symbols():
                if sym not in self.universe:
                    continue
                if sym not in self.series:
                    if primed >= self.primes_per_step:
                        continue                           # the trader's loop is never held up long
                    self.prime(sym, now_min)
                    primed += 1
                done["bars"] += self._advance(sym, now_min, done)
                wants.extend(self._signals(sym, now_min))
            if wants:
                done["entries"] += self._enter(wants, now_min)
            self.feed_ok, self.last_error = True, ""
        except Exception as exc:
            self.feed_ok, self.last_error = False, f"{type(exc).__name__}: {exc}"
            self.log(f"prices: {self.last_error}")
        self._flat_all(now_min)                            # a market not read this minute: the flat still comes
        self.save_state()
        return done

    # ---------------------------------------------------------- management --
    def _reconcile(self, now_min: int) -> None:
        """Every minute: a position the broker no longer holds was closed there (its
        stop or take-profit): booked from its deal. An exit refused earlier is
        tried again; a deal not back yet is looked for again. Nothing is decided
        on a failed read of the broker's list."""
        if not self.open:
            try:
                self._sweep(self._broker_positions())
            except Exception:
                pass
            return
        try:
            at_broker = self._broker_positions()
        except Exception as exc:
            self.log(f"the broker's positions could not be read ({exc}); nothing changed")
            return
        roster_known = self.roster_ok and self.roster_key is not None
        managed = {o.ticket for o in self.open.values()}   # closed and booked below: never an orphan of this read
        for o in list(self.open.values()):
            if o.closing and o.ticket in at_broker and not o.cid.startswith("orphan:"):
                # the broker still holds it: the earlier absence (an empty read - the adapter answers an error
                # with an empty list - or a lagging list) was not a close. Managed again; an exit the engine had
                # decided is sent again
                prev = o.closing
                o.closing = ""
                self.record.update_open(o.ticket, closing="")
                if prev != "BROKER" and not o.exit_retry:
                    o.exit_retry = prev
                if not o.exit_retry:
                    o.exit_t = 0                           # nothing was decided: the next exit sets the pause
                self.log(f"{o.cid} {o.symbol}: ticket {o.ticket} is still open at the broker: managed again"
                         + (f", its {o.exit_retry} exit sent again" if o.exit_retry else ""))
                ser = self.series.get(o.symbol)
                if o.exit_retry:
                    # the exit is decided already: the bars behind it decide nothing, and the quiet spell was the
                    # broker's list, not the feed (never a FEED_GAP that did not happen)
                    if ser is not None and ser.n:
                        o.last_t = max(int(o.last_t), int(ser.t[ser.n - 1]))
                elif not (roster_known and o.cid not in self.slots):   # off the roster: the demotion below
                    self._catch_up(o)                      # the bars skipped while marked, walked as paper
                    if o.closing or o.exit_retry or self.open.get((o.cid, o.symbol)) is not o:
                        continue                           # an exit the catch-up decided was sent (booked if back)
            if o.closing:
                self._settle(o)
            elif o.ticket not in at_broker:
                o.closing = o.closing or "BROKER"          # the broker's stop or target: its bar from the deal
                self._settle(o)
            elif o.exit_retry:
                self._exit(o, o.exit_retry, now_min)
            elif roster_known and o.cid not in self.slots:
                self._exit(o, "DEMOTED", now_min)          # off the roster (also after a restart): the clean cut
            else:
                q = at_broker[o.ticket]
                if q.sl and float(q.sl) != o.broker_stop and self._better(o, float(q.sl)):
                    o.broker_stop = float(q.sl)            # the broker holds a better stop: kept
        # a position closed in this pass is still in this (older) read: swept, if anything is left, next minute
        self._sweep({t: q for t, q in at_broker.items() if t not in managed})

    @staticmethod
    def _better(o: DemoOpen, price: float) -> bool:
        return price > o.broker_stop if o.pos.side > 0 else (o.broker_stop == 0 or price < o.broker_stop)

    def _walk(self, sym: str, r: tuple) -> None:
        """The simulator's own exits on each finished bar, acted on at the broker."""
        for key in [k for k in self.open if k[1] == sym]:
            op = self.open.get(key)
            if op is not None:
                self._walk_one(op, sym, r)

    def _walk_one(self, op: DemoOpen, sym: str, r: tuple, move_stop: bool = True) -> None:
        """One position through one finished bar: the simulator's step, its exit sent to the broker."""
        t, o_, h, l, c, s, _v = r
        if op.closing or t <= op.last_t:
            return
        pos = op.pos
        flat = self._flat_of(op)
        if flat is not None and t >= flat:
            # the flat came before this bar (as practice, Practice._walk): a SESSION_END at the flat, sent now -
            # never a FEED_GAP or a step on a bar the position should not have been held to. An exit already
            # decided and refused is sent again by _reconcile, never twice in a minute
            op.last_t = t
            if not op.exit_retry:
                self._exit(op, "SESSION_END", flat - 1)
            return
        sp = self._bar_spread(sym, t, s)
        gap = t - op.last_t
        if gap > FEED_GAP_MINUTES and op.last_t >= pos.t_open:
            op.last_t = t
            self._exit(op, "FEED_GAP", t)
            return
        # off the roster only when the roster is known (as _reconcile): an unread one decides nothing
        off = self.roster_ok and self.roster_key is not None and op.cid not in self.slots
        last = (flat is None or t + 1 >= flat) or off      # a day that cannot be read: flat at once
        before = pos.stop
        fill = sim.step(pos, t, float(o_), float(h), float(l), float(c), sp, last)
        op.last_t = t
        if fill is not None:
            self._exit(op, fill[1], t)
        elif pos.stop != before and move_stop:
            self._move_stop(op)

    def _flat_clock(self, sym: str, now_min: int) -> None:
        """Practice's flat on the clock (``Practice._flat_clock``), at the broker: once this market's bars are
        read, a position still open at 21:55 UK (21:00 on a Friday) is closed with a market close. The broker
        holds its stop until the close fills; one refused (the market is shut) is sent again every minute."""
        for key in [k for k in self.open if k[1] == sym]:
            o = self.open.get(key)
            if o is not None:
                self._flat_due(o, now_min)

    def _flat_all(self, now_min: int) -> None:
        """The flat for every open position, whether or not its market's bars could be read this minute (not
        primed yet, an error, the weekend): the close is priced by the broker, so no bar is needed, and a
        position is never held past the flat for want of one."""
        for o in list(self.open.values()):
            try:
                self._flat_due(o, now_min)
            except Exception as exc:                       # never stops the minute; tried again next minute
                self.log(f"{o.cid} {o.symbol}: the flat's close failed ({type(exc).__name__}: {exc}); "
                         "tried again next minute")

    def _flat_due(self, o: DemoOpen, now_min: int) -> None:
        if o.closing or o.exit_retry or o.cid.startswith("orphan:") or self.open.get((o.cid, o.symbol)) is not o:
            return                                         # closed, or an exit already decided (sent again)
        flat = self._flat_of(o)
        if flat is None or now_min < flat:
            return
        when = minute_to_dt(flat).astimezone(TZ_LONDON).strftime("%H:%M")
        self.log(f"{o.cid} {o.symbol}: the {when} UK flat has come with ticket {o.ticket} open (no bar from its "
                 f"market closed it): closed at the broker on the clock")
        self._exit(o, "SESSION_END", flat - 1)             # the pause from the flat's bar, as practice

    def _catch_up(self, o: DemoOpen) -> None:
        """A closing mark cleared (the broker still held the position): the bars it was not walked through
        while marked are walked now, through the same simulator step, so its trail, time, session-end and
        feed-gap decisions are paper's. The quiet spell was the broker's list, never the feed: a true gap in
        the bars is still a FEED_GAP exit here, as on paper. A market not primed yet has nothing to walk
        (``prime`` walks the position through every bar it reads)."""
        ser = self.series.get(o.symbol)
        if ser is None or not ser.n:
            return
        key = (o.cid, o.symbol)
        for i in range(bisect.bisect_right(ser.t, o.last_t), ser.n):
            if o.closing or o.exit_retry or self.open.get(key) is not o:
                break                                      # an exit was decided on the way and sent
            self._walk_one(o, o.symbol, (ser.t[i], ser.o[i], ser.h[i], ser.l[i], ser.c[i], ser.s[i], ser.v[i]),
                           move_stop=False)
        if self.open.get(key) is not o or o.closing:
            return
        if o.exit_retry:
            # the exit was decided on the way and its close refused (sent again): the bars behind it decide
            # nothing, as the clear-time exit_retry branch in _reconcile - the next bar is never a FEED_GAP that
            # did not happen (exit_t keeps the decided bar for the pause)
            o.last_t = max(int(o.last_t), int(ser.t[ser.n - 1]))
            return
        self._move_stop(o)                                 # the trail's stop, once (only ever tighter)

    def _move_stop(self, o: DemoOpen) -> None:
        """The trail moved the simulator's stop: the broker's follows, only ever in the trade's favour."""
        spec = self._broker_spec(o.symbol)
        if spec is None:
            return
        price = spec.normalise_price(o.pos.stop * self.inst(o.symbol).point)
        if not self._better(o, price):
            return
        ex = self.executor(o.magic)
        if ex.modify_stop(o.ticket, price):
            o.broker_stop = price
            self.record.update_open(o.ticket, stop=price)
            self.log(f"{o.cid} {o.symbol}: stop moved to {price} (trail)")
        else:
            self.log(f"{o.cid} {o.symbol}: the trail's stop {price} was refused ({ex.last_error}); kept at "
                     f"{o.broker_stop}, tried again at the next bar")

    def _exit(self, o: DemoOpen, reason: str, t: int) -> None:
        """An exit the simulator (or a demotion) decided: a market close at the
        broker, unless the broker has already closed it (its stop, its target)."""
        ex = self.executor(o.magic)
        o.exit_t = o.exit_t or int(t)                      # the bar it was decided on (kept through any retry)
        try:
            held = {int(q.ticket): float(q.volume or 0.0) for q in ex.positions()}
        except Exception as exc:
            o.exit_retry = reason
            self.log(f"{o.cid} {o.symbol}: {reason} exit waits, the broker's list could not be read ({exc})")
            return
        if o.ticket not in held:
            o.closing = reason if reason in ("STOP", "TRAIL", "TARGET") else "BROKER"
            o.exit_retry = reason                          # kept: sent again should the broker still hold it
            self._settle(o)
            return
        tick = self._tick(o.symbol)
        fill = ex.close(o.ticket, tick, self._broker_spec(o.symbol), comment=f"{TAG} {reason}"[:31]) if tick else None
        if fill is None or not fill.ok:
            o.exit_retry = reason
            self.log(f"{o.cid} {o.symbol}: the {reason} close was refused "
                     f"({fill.message if fill else 'no price'}); still open with its stop, tried again next minute")
            return
        got = float(getattr(fill, "volume", 0.0) or 0.0)
        if 0 < got < held[o.ticket] - 1e-9:
            # partly filled (MetaTrader's DONE_PARTIAL is ok): nothing is booked yet; the rest keeps its stop at
            # the broker and is closed next minute, so the position is booked once, whole, with its R
            o.exit_retry = reason
            self.log(f"{o.cid} {o.symbol}: the {reason} close filled {got:g} of {held[o.ticket]:g} lots; the rest "
                     "keeps its stop at the broker and is closed next minute")
            return
        o.exit_retry = ""
        o.closing = reason
        self._settle(o)

    def _settle(self, o: DemoOpen) -> None:
        """Book a closed position from the broker's closed-deal record; a deal not
        back yet keeps the row (marked closing) and is looked for next minute."""
        ex = self.executor(o.magic)
        deal = ex.closed_deal(o.ticket, tries=2, pause=0.05)
        if deal is None or deal.get("pnl") is None:
            if o.ticket and not o.cid.startswith("orphan:"):
                self.record.update_open(o.ticket, closing=o.closing or "BROKER")
            self.log(f"{o.cid} {o.symbol}: ticket {o.ticket} is closed; its deal is not back yet")
            return
        net = round(float(deal["pnl"]), 2)
        reason = o.closing or "BROKER"
        broker_reason = str(deal.get("reason") or "").upper().lstrip("[ ")[:2]     # MetaTrader: "[sl 1.08374]"
        if reason == "BROKER":
            # the broker's stop or target; else, when the closing deal is our own close ("LAB TIME") and an exit
            # was pending (its answer lost to a lagging list, sent again and refused), the exit the engine decided
            ours = str(deal.get("reason") or "").upper().startswith(f"{TAG} ")
            reason = {"SL": "STOP", "TP": "TARGET"}.get(broker_reason) or (
                (o.exit_retry or "BROKER_CLOSE") if ours else "BROKER_CLOSE")
        risk = float(o.risk_gbp or 0.0)
        r = round(net / risk, 4) if risk > 0 else None
        when = deal.get("time")
        closed = to_utc(when).isoformat() if isinstance(when, dt.datetime) else utc_stamp(self.now_fn())
        pt = self.inst(o.symbol).point if o.symbol in self.universe else 0.0
        trade = {"ticket": int(o.ticket), "idea_id": (o.orphan_of or "?") if o.cid.startswith("orphan:") else o.cid,
                 "version": int(o.version), "magic": int(o.magic), "symbol": o.symbol, "side": int(o.pos.side or 0),
                 "volume": float(getattr(o.pos, "vol", 0.0) or 0.0),
                 "entry": round(float(getattr(o.pos, "entry", 0.0) or 0.0) * pt, 8),
                 "stop": round(float(getattr(o.pos, "stop0", 0.0) or 0.0) * pt, 8),
                 "target": (None if getattr(o.pos, "target", None) is None else round(float(o.pos.target) * pt, 8)),
                 "opened_utc": iso_utc(int(o.pos.t_open)) if getattr(o.pos, "t_open", None) else None,
                 "closed_utc": closed, "exit_price": float(deal.get("exit_price") or 0.0), "exit_reason": reason,
                 "net_gbp": net, "entry_commission_gbp": deal.get("entry_commission"),
                 "risk_gbp": risk, "risk_allowed_gbp": float(o.risk_allowed or 0.0),
                 "tier_pct": float(o.tier_pct or 0.0), "equity_at_entry": float(o.equity or 0.0),
                 "r": r, "equiv_1500_gbp": REC.equiv_1500(r), "deal_source": "broker closed deal", "plan": o.plan,
                 "recorded_utc": utc_stamp(self.now_fn())}
        if o.cid.startswith("orphan:"):
            trade["exit_reason"] = "ORPHAN"
            trade["risk_gbp"] = 0.0
            trade["r"] = trade["equiv_1500_gbp"] = None
        self.record.book(trade)
        self.open.pop((o.cid, o.symbol), None)
        slot = self.slots.get(o.cid)
        if slot is not None and getattr(o.pos, "t_open", None):
            # from the exit bar, as practice (Practice._book: t_exit + COOLDOWN_BARS)
            slot.next_ok[o.symbol] = self._exit_bar(o, when) + COOLDOWN_BARS
        self.log(f"{o.cid} {o.symbol}: ticket {o.ticket} closed {reason}, £{net:+.2f} (the broker's figure)"
                 + (f", {r:+.2f} R, £{trade['equiv_1500_gbp']:+.2f} on a £1,500 account" if r is not None else ""))

    def _exit_bar(self, o: DemoOpen, when) -> int:
        """The bar a closed position's pause runs from, as practice's t_exit: the bar the engine decided the
        exit on; for a close the broker made (its stop or target, or one found after a restart), the bar its
        closing deal fell in - MetaTrader stamps a bar with its opening minute, so a fill at 09:02:01 is bar
        09:02 however late the step that notices it runs. Never before the last bar walked with the position
        held nor after now (a broker clock that cannot be right); now when the deal has no time (later, never
        earlier)."""
        now_min = self._now_min()
        if o.exit_t:
            return int(o.exit_t)
        try:
            return min(max(epoch_min(to_utc(when)), int(o.last_t or 0)), now_min)
        except (AttributeError, TypeError, ValueError, OverflowError):
            return now_min

    # --------------------------------------------------------------- entry --
    def _may_enter(self) -> tuple[bool, str]:
        if not self.roster_ok:
            return False, "the roster cannot be read"
        if (self.data_dir / "HALT").exists():
            return False, "data/HALT"
        try:
            if not self.entries_allowed():
                return False, "the account gate is shut (daily-loss stop, drawdown or safe mode)"
        except Exception as exc:
            return False, f"the account gate could not be asked ({exc})"
        return True, ""

    def _open_risk(self, account) -> Optional[float]:
        """Money at risk to the stop of every position on the account (all bots,
        all ideas); None when a position has no stop or cannot be valued."""
        total = 0.0
        for q in self.broker.positions(None) or ():
            if not q.sl:
                return None
            spec = self._broker_spec(q.symbol)
            if spec is None:
                return None
            dist = (float(q.entry_price) - float(q.sl)) if q.side is Side.BUY else (float(q.sl) - float(q.entry_price))
            pp = float(spec.tick_value) / float(spec.tick_size)          # money per price unit per lot
            total += max(0.0, dist) * pp * float(q.volume)
        return total

    def _enter(self, wants: list, now_min: int) -> int:
        ok, why = self._may_enter()
        if not ok:
            for slot, sym, *_ in wants:
                self._note_skip(slot, sym, f"entries closed: {why}", now_min)
            return 0
        per_idea: dict = {}
        for (cid, _sym) in self.open:
            per_idea[cid] = per_idea.get(cid, 0) + 1
        ok_wants = []
        for slot, sym, i, sg, dv in wants:                 # the practice engine's own caps, per idea
            ser = self.series[sym]
            if (slot.cid, sym) in self.open or ser.t[i] < slot.next_ok.get(sym, 0):
                self.skip("busy_or_cooldown")
                continue
            if per_idea.get(slot.cid, 0) >= MAX_OPEN_PER_CANDIDATE:
                self.skip("max_open")
                continue
            if slot.entries.get(f"{sym}|{dv.uk}", 0) >= MAX_ENTRIES_PER_DAY:
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
        try:
            account = self.broker.account()
        except Exception as exc:
            for slot, sym, *_ in ok_wants:
                self._note_skip(slot, sym, f"the account could not be read ({exc})", now_min)
            return 0
        ticks = self.feed.ticks(sorted({w[1] for w in ok_wants})) or {}
        cap = risk_cap_pct(self.data_dir)
        n = 0
        for slot, sym, i, sg, dv in ok_wants:
            if per_idea.get(slot.cid, 0) >= MAX_OPEN_PER_CANDIDATE:
                self.skip("max_open")
                continue
            tk = ticks.get(sym)
            spec = self._broker_spec(sym)
            if tk is None or not tk.bid or not tk.ask or spec is None:
                self._note_skip(slot, sym, "no price or instrument spec from the broker", now_min)
                continue
            got = self._enter_one(slot, sym, i, sg, dv, tk, spec, account, cap, now_min)
            if got:
                per_idea[slot.cid] = per_idea.get(slot.cid, 0) + 1
                n += 1
                try:
                    account = self.broker.account()        # the next one is sized on the new equity and margin
                except Exception:
                    break
        return n

    def _enter_one(self, slot: DemoSlot, sym, i, sg, dv, tk, spec, account, cap: float, now_min: int) -> bool:
        ser = self.series[sym]
        inst = sim.Inst(sym, self.universe[sym].kind, spec)       # the broker's own money facts
        pt = inst.point
        bid, ask = tk.bid / pt, tk.ask / pt
        spread = max(ask - bid, float(ser.hours[(now_min // 60) % 24]), 0.0)
        slip = max(SLIP_MIN_POINTS, SLIP_SPREAD_SHARE * spread)
        price = ask + slip if sg.side > 0 else bid - slip
        c = slot.cand
        equity = float(getattr(account, "equity", 0.0) or 0.0)
        risk = round(equity * slot.tier_pct / 100.0, 2)
        if risk <= 0:
            self._note_skip(slot, sym, "no equity to size from", now_min)
            return False
        vw = float(ser.f(f"vwap_{c.session}")[i]) if c.exit == "vwap" else 0.0
        pos = sim.enter(inst, c.exit, risk, sg.side, float(ser.c[i]), sg.stop_points, float(ser.f("atr5")[i]),
                        price, spread, False, ser.day_median_spread(dv), now_min, now_min, vwap=vw,
                        entry_price=price)
        if isinstance(pos, str):
            words = {"smallest_size": f"smallest_size: the broker's smallest lot ({inst.vmin}) would risk more than "
                                      f"the £{risk:.2f} allowed ({slot.tier_pct}% of £{equity:,.2f})"}.get(pos, pos)
            self._note_skip(slot, sym, words, now_min)
            return False
        if pos.stop is None:
            self._note_skip(slot, sym, "no_stop", now_min)
            return False
        # the tier's size, made SMALLER (never larger) to fit the account's combined open-risk cap and the
        # broker's margin; skipped only when even the broker's smallest lot will not fit
        per_lot = pos.D * inst.pp                                 # money at the stop for one lot
        try:
            open_risk = self._open_risk(account)
        except Exception as exc:
            self._note_skip(slot, sym, f"the account's positions could not be read ({exc})", now_min)
            return False
        if open_risk is None:
            self._note_skip(slot, sym, "risk_cap: a position on the account has no stop or no spec", now_min)
            return False
        room = cap / 100.0 * equity - open_risk
        vol_cap = sim.floor_to_step(room / per_lot, inst.vstep, inst.vmax) if room > 0 else 0.0
        if vol_cap < inst.vmin - 1e-12:
            self._note_skip(slot, sym, f"risk_cap: £{open_risk:.2f} already at risk; even {inst.vmin} lot "
                                       f"(£{inst.vmin * per_lot:.2f}) would pass {cap:g}% of equity "
                                       f"(£{cap / 100.0 * equity:.2f})", now_min)
            return False
        side = Side.BUY if sg.side > 0 else Side.SELL
        try:
            need1 = self.broker.calc_margin(sym, side, 1.0, price * pt)
        except Exception:
            need1 = None
        free = float(getattr(account, "margin_free", 0.0) or 0.0)
        if need1 is None or float(need1) <= 0:
            self._note_skip(slot, sym, "margin: the broker could not say the margin needed", now_min)
            return False
        vol_margin = sim.floor_to_step(MARGIN_USE_MAX * free / float(need1), inst.vstep, inst.vmax)
        if vol_margin < inst.vmin - 1e-12:
            self._note_skip(slot, sym, f"margin: even {inst.vmin} lot needs £{inst.vmin * float(need1):.2f}, more "
                                       f"than half the £{free:.2f} free", now_min)
            return False
        # the margin LEVEL must still clear the account's minimum (as Trend & Breakout's sizing, risk.py), or the
        # trader's MARGIN_LEVEL breaker would shut every bot's entries while this one is open - with real headroom:
        # on the equity left with this position all the way at its own stop (pos.D runs from the fill estimate,
        # ask + slip for a buy, so the fill's spread and slip are in it) less its entry commission, and kept
        # MARGIN_LEVEL_HEADROOM above the floor (a worse fill, the margin's own drift with the price):
        #   (equity - worst1 x vol) / (used + need1 x vol) >= level
        #   <=>  vol <= (equity - level x used) / (level x need1 + worst1)
        level = max(self.min_margin_level_pct, 100.0) / 100.0 * (1.0 + MARGIN_LEVEL_HEADROOM)
        used = float(getattr(account, "margin", 0.0) or 0.0)
        worst1 = per_lot + max(0.0, float(inst.comm_side))                 # money lost a lot by its stop
        room_level = equity - level * used
        vol_level = (sim.floor_to_step(room_level / (level * float(need1) + worst1), inst.vstep, inst.vmax)
                     if room_level > 0 else 0.0)
        if not vol_level >= inst.vmin - 1e-12:
            self._note_skip(slot, sym, f"margin level: even {inst.vmin} lot needs £{inst.vmin * float(need1):.2f} "
                                       f"of margin, which would take the account below its "
                                       f"{self.min_margin_level_pct:g}% minimum (with its own stop and "
                                       f"{MARGIN_LEVEL_HEADROOM:.0%} headroom)", now_min)
            return False
        allowed = risk
        fit = min(vol_cap, vol_margin, vol_level)
        if fit < pos.vol:
            why = ("the risk cap" if vol_cap <= min(vol_margin, vol_level) else
                   "the margin" if vol_margin <= vol_level else "the margin level")
            self.log(f"{slot.cid} {sym}: {pos.vol} lot cut to {fit} to fit {why}")
            pos.vol = round(fit, 8)
        stop_px = spec.normalise_price(pos.stop * pt)
        tp_px = spec.normalise_price(pos.target * pt) if pos.target is not None else 0.0
        ex = self.executor(slot.magic)
        when = minute_to_dt(now_min) + dt.timedelta(seconds=AT_SECOND)
        fill = ex.open(sym, side, pos.vol, stop_px, tp_px, tk, spec, when,
                       comment=f"{TAG} {slot.cid[4:]}")
        if not fill.ok:
            self._note_skip(slot, sym, f"order: {fill.message}", now_min)
            return False
        # the broker's fill is the entry (R and the trail's start are measured from it); the R of the trade is
        # its net over the money really at its stop (the broker's fill, the stop, the volume filled)
        pos.entry = pos.peak = float(fill.price) / pt
        pos.vol = float(fill.volume or pos.vol)
        risk = round(abs(pos.entry - pos.stop) * inst.pp * pos.vol, 2) or allowed
        pos.fill_source = "broker"
        pos.ref = sg.ref
        o = DemoOpen(slot.cid, sym, pos, dv.uk, iso_utc(ser.t[i]),
                     json.dumps({"bid": tk.bid, "ask": tk.ask}), now_min - 1, ticket=int(fill.ticket),
                     magic=slot.magic, version=slot.version, risk_gbp=risk, tier_pct=slot.tier_pct, equity=equity,
                     plan=slot.plan, broker_stop=stop_px, risk_allowed=allowed)
        self.open[(slot.cid, sym)] = o
        dkey = f"{sym}|{dv.uk}"
        slot.entries[dkey] = slot.entries.get(dkey, 0) + 1
        self.record.put_open({
            "ticket": o.ticket, "idea_id": slot.cid, "version": slot.version, "magic": slot.magic, "symbol": sym,
            "side": sg.side, "volume": pos.vol, "entry": float(fill.price), "stop": stop_px,
            "target": tp_px or None, "opened_utc": iso_utc(now_min), "risk_gbp": risk, "risk_allowed_gbp": allowed,
            "tier_pct": slot.tier_pct,
            "equity_at_entry": equity, "day": dv.uk, "signal_bar_utc": o.signal_bar_utc,
            "pos_json": json.dumps({k: getattr(pos, k) for k in sim.Pos.__slots__ if hasattr(pos, k)}),
            "last_t": o.last_t, "closing": "", "exit_hint": "", "plan": slot.plan})
        self.log(f"{slot.cid} {sym}: DEMO {side.value} {pos.vol} at {fill.price}, stop {stop_px} at the broker"
                 + (f", target {tp_px}" if tp_px else "") + f" (risk £{risk:.2f} of £{allowed:.2f} allowed, tier "
                 f"{slot.tier_pct}%, ticket "
                 f"{fill.ticket}, {sg.ref})")
        return True

    # --------------------------------------------------------------- status --
    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.now_fn())
        return {"written_utc": utc_stamp(now), "mode": "DEMO", "roster_ok": self.roster_ok,
                "ideas": [{"id": s.cid, "version": s.version, "magic": s.magic, "tier_pct": s.tier_pct,
                           "markets": s.symbols} for s in self.slots.values()],
                "open": [{"id": o.cid, "symbol": o.symbol, "ticket": o.ticket, "magic": o.magic,
                          "side": int(o.pos.side or 0), "volume": getattr(o.pos, "vol", None),
                          "stop": o.broker_stop, "closing": o.closing, "risk_gbp": o.risk_gbp}
                         for o in self.open.values()],
                "skipped": dict(self.skipped), "feed_ok": self.feed_ok, "last_error": self.last_error,
                "note": "lab ideas on the DEMO account: genuine orders, the stop at the broker"}

    def write(self, now: Optional[dt.datetime] = None) -> None:
        try:
            atomic_write_json(R.ldir(self.data_dir) / R.STATUS, self.status(now))
        except OSError as exc:
            self.log(f"status not written: {exc}")


def from_trader(data_dir, broker, cfg, *, entries_allowed=None, clock=None, calendar=None) -> Optional[LabDemo]:
    """The engine for the trader, or None: not DEMO, or the lab has no markets yet."""
    if str(getattr(cfg, "mode", "") or "").upper() != "DEMO":
        return None
    if not LabPaths(data_dir).universe.exists():
        return None
    return LabDemo(data_dir, broker, cfg_mode=cfg.mode, entries_allowed=entries_allowed, now_fn=clock,
                   calendar=calendar, min_margin_level_pct=min_margin_level(cfg))


def min_margin_level(cfg) -> float:
    """config.json risk.min_margin_level_pct (the trader's MARGIN_LEVEL breaker); 300 when not set or unreadable."""
    v = getattr(getattr(cfg, "risk", None), "min_margin_level_pct", None)
    try:
        return MIN_MARGIN_LEVEL_PCT if v is None else float(v)
    except (TypeError, ValueError):
        return MIN_MARGIN_LEVEL_PCT
