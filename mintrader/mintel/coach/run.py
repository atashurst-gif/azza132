"""The coach's run loop (spec sections 8, 10.2 and 14): the 60-second tick,
the 22:20 UK nightly review, the 06:30 UK release and the policy writer.

    python -m mintel.coach run            (kept alive by the watchdog at nice 10)

Every tick (every day: Financial Ian trades at weekends):
  data checks; the page's standing block, weather.json, approvals and new
  closes; the affected keys' estimates; the brakes; D1, D2, D4, D5, S1, S2,
  K1; last night's plan if it is due (06:30 UK); the policy written (always:
  a 180-second lease, so if the coach dies its grants lapse and its
  restrictions stay); status.json; the audit of the gate logs. A tick never
  loosens anything.

The nightly review, 22:20 UK (after the 22:10 upload, before the lab at
23:10; if the coach was down then, at its next start before the release):
  the full refit and tau; the luck check; the standing rules; G1, G2, G3,
  D3, Formula 1's judgement and the demo ladder; the scorecard; tomorrow's
  game plan (``data/coach/plan-<date>.json``) and the digest.

The writer, in this order (a failure at any step aborts the write):
  1 build the policy; 2 run the validator the gate runs; 3 diff it against
  the previous policy; 4 commit one ledger ``changes`` row per difference
  (with its sentence; a difference without one aborts) and the segments, in
  one transaction; 5 a temporary file, fsync, the current file copied to
  policy.prev.json, os.replace; 6 the ``policies`` row marked valid;
  7 status.json.

The coach writes only under ``data/coach/``. It never writes config.json,
the envelope, HALT, COACH-OFF or the approvals (those are Aaron's, through
his CLI), never opens the secrets file, writes no heartbeat, holds no broker
object and never places, closes or modifies an order or starts or stops a
process.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import math
import time
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from . import (BOT_LABEL, BOT_TNB, F1_KEY, LEASE_SECONDS, MODE_GUARD, MODE_OFF, MODE_PILOT, MODE_WATCH,
               NIGHTLY_UK, ROUTE_REAL, SCORECARD_GUARD_P, SCORECARD_MIN_DAYS, SCORECARD_MIN_TRADES, STATE_LIVE,
               STATE_LIVE_TRIAL, STATE_PRACTICE, B9_MAX_POLICY_CHANGES_24H, B9_MAX_TICK_ERRORS,
               B9_MAX_TRIAL_STARTS_WEEK, DEMO_MAX_TRIALS, TZ_UK, UTC, Floor, Gates, Judgement, Paths, Policy, as_utc,
               is_uk_trading_day, next_release_utc, parse_utc, uk_day)
from . import brakes as B
from . import decide as D
from . import envelope as envelope_mod
from . import estimate as ES
from . import feed as F
from . import policy as P
from . import records as R
from . import report as REPORT
from . import rules as RU
from . import status as S
from . import weather as W
from .ledger import Change, Ledger
from .words import gbp, key_words, say, short

log = logging.getLogger("mintel.coach.run")

WATCHDAY_RECON_SHARE = 0.95


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def nightly_instant(day: dt.date) -> dt.datetime:
    hh, mm = (int(x) for x in NIGHTLY_UK.split(":"))
    return dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=TZ_UK).astimezone(UTC)


def due_nightly(now: dt.datetime, last_date: Optional[str]) -> Optional[dt.date]:
    """The UK date whose 22:20 review is due now: tonight's once it is 22:20 UK, or last night's if the coach
    was down then and the plan's release (06:30 UK on the next UK trading day) has not come yet."""
    now = as_utc(now)
    today = uk_day(now)
    cand = today if now >= nightly_instant(today) else today - dt.timedelta(days=1)
    if last_date == cand.isoformat():
        return None
    n = nightly_instant(cand)
    if now < n or now >= next_release_utc(n):
        return None
    return cand


def _fin(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(float(x))


class Coach:
    """One coach for one data folder. ``fetch``: how the page's /api is read (tests pass a fake); ``clock``:
    the time; ``upload``: a callable(files, date) that sends the night's files (None: they stay on disk)."""

    def __init__(self, data_dir: "str | Path", *, cfg: Any = None, clock: Optional[Callable[[], dt.datetime]] = None,
                 fetch: Optional[Callable[[str], str]] = None, upload: Optional[Callable] = None,
                 ledger: Optional[Ledger] = None, shuffles: Optional[int] = None):
        self.data_dir = Path(data_dir)
        self.paths = Paths.of(self.data_dir)
        self.paths.coach_dir.mkdir(parents=True, exist_ok=True)
        self.cfg_given = cfg
        self.clock = clock or utcnow
        self.fetch = fetch
        self.upload = upload
        self.ledger = ledger or Ledger(self.paths.ledger)
        self.shuffles = shuffles
        self.fit = None
        self.fit_n = -1
        self.board = W.BoardMemory()
        self.last_policy: Optional[dict] = self._load_last_policy()
        self.last_status: Optional[dict] = None
        try:
            from ..version import RUNNING_STAMP
            self.build = str(RUNNING_STAMP)
        except Exception:
            self.build = ""

    # ------------------------------------------------------------- small --
    def _load_last_policy(self) -> Optional[dict]:
        try:
            obj = json.loads(self.paths.policy.read_text())
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def config(self) -> Any:
        return self.cfg_given if self.cfg_given is not None else R.baseline_config(self.data_dir)

    def close(self) -> None:
        self.ledger.close()

    # -------------------------------------------------------------- tick --
    def tick(self, now: Optional[dt.datetime] = None) -> dict:
        """One tick. Never raises: an error is a failed tick (three in a row put the coach in GUARD)."""
        now = as_utc(now or self.clock())
        t0 = time.perf_counter()
        try:
            out = self._tick(now)
            self.ledger.add_tick(now, True, (time.perf_counter() - t0) * 1000.0)
            return out
        except Exception as exc:
            log.exception("the coach's tick failed")
            if isinstance(exc, P.PolicyError):
                wd = self.ledger.get("watchday") or {}
                wd["validation_errors"] = int(wd.get("validation_errors", 0)) + 1
                self.ledger.set("watchday", wd, now)
            self.ledger.add_tick(now, False, (time.perf_counter() - t0) * 1000.0, f"{type(exc).__name__}: {exc}")
            n = self.ledger.errors_in_a_row()
            if n >= B9_MAX_TICK_ERRORS:
                self._guard(now, f"{n} ticks in a row failed ({type(exc).__name__})")
            try:
                S.write(self.paths, S.base(now, "ERROR", f"The coach's last tick failed ({type(exc).__name__}: {exc}). "
                                                         f"The bots keep its last instructions for 3 minutes, then "
                                                         f"your own settings with its restrictions.",
                                           error=str(exc)[:500]))
            except Exception:
                pass
            return {"ok": False, "error": str(exc)}

    def _guard(self, now: dt.datetime, why: str) -> None:
        g = self.ledger.get("guard")
        if g:
            return
        self.ledger.commit([Change("mode", "coach", say("B9", why=why), "PILOT", MODE_GUARD, "B9", {"why": why})],
                           meta={"guard": {"why": why, "since": now.isoformat()}}, now=now)

    def _off_status(self, now: dt.datetime, words: str, mode: str = MODE_OFF, **extra) -> dict:
        doc = S.base(now, mode, words, **extra)
        S.write(self.paths, doc)
        self.last_status = doc
        return {"ok": True, "mode": mode, "policy": None, "status": doc}

    def _tick(self, now: dt.datetime) -> dict:
        fd = F.read(self.data_dir, now, cfg=self.config(), fetch=self.fetch)
        cfg = fd.cfg
        if self.paths.coach_off.exists():
            return self._off_status(now, "Switched off - the bots are on your own settings.")
        if not fd.envelope.ok:
            if not fd.envelope.exists:
                return self._off_status(now, "Switched off (no envelope yet) - the bots are on your own settings.")
            return self._off_status(now, f"The coach's envelope could not be used ({fd.envelope.error}): the bots are "
                                         f"on your own settings.", envelope_error=fd.envelope.error)
        env = fd.envelope.envelope
        env_mode, mode_why = envelope_mod.effective_mode(env, now)
        if env_mode == MODE_OFF:
            return self._off_status(now, "Switched off - the bots are on your own settings.")
        # ---- evidence and the data checks (section 4.4)
        prev_counts = self.ledger.get("counts")
        ev = R.read_evidence(self.data_dir, cfg=cfg, now=now, previous_counts=prev_counts)
        recon = R.reconcile(ev.trades, fd.page.today, now)
        failed = list(ev.failed)
        if not fd.page.ok:
            failed.append(fd.page.why)
        elif not recon["ok"]:
            failed.append(recon["why"])
        if fd.open_risk is None:
            failed.append(f"open real risk not known ({fd.open_risk_why})")
        data_ok = not failed
        meta: dict = {}
        if ev.data_ok:
            meta["counts"] = ev.counts()
        # ---- the demo test (section 21), the same test the gate runs
        demo_ok, demo_why = self._demo_test(cfg, now)
        segs = self.ledger.segments()
        judged = self._judged()
        stop = D.stop_level(segs, cfg, demo_ok)
        ln = B.lines(env, float(cfg.risk.max_daily_loss_pct), stop, demo_ok)
        # ---- the account (broker truth, every bot) for the brakes
        acct, e0_meta = self._account(fd, ev, now)
        if e0_meta:
            meta["e0"] = e0_meta
        changes_hist = self.ledger.changes(kinds=["state"], limit=20000)
        trial_losses = self._trial_losses(ev.trades, changes_hist, now)
        prev = B.BrakeState.from_json(self.ledger.get("brakes"))
        saved_peak = self.ledger.peak()
        if saved_peak is not None and (prev.peak is None or saved_peak > prev.peak):
            prev.peak = saved_peak
        due = due_nightly(now, self.ledger.get("nightly_date"))
        br, brake_moves = B.evaluate(prev, acct, ln, now=now, data_ok=data_ok, trial_losses_gbp=trial_losses,
                                     nightly=due is not None)
        meta["brakes"] = br.to_json()
        new_peak = br.peak if (br.peak is not None and (saved_peak is None or br.peak > saved_peak + 1e-9)) else None
        # ---- the audit of the gate logs and B9 (before the mode, so a finding takes effect at once)
        meta.update(self._audit_meta(now, cfg, env, fd.page.equity, data_ok, demo=demo_ok))
        # ---- the coach's own mode
        mode, mode_words = self._mode(env, env_mode, mode_why, now, fd, meta)
        # ---- estimates
        self._refresh_fit(ev.trades, segs, judged, env, now, full=due is not None)
        w = self._world(now, cfg, env, mode, ev, data_ok, br, demo_ok, fd, judged, stop, segs)
        moves: list = []
        notes: list[Change] = []
        moves += D.sync(segs, w)
        # ---- the release (06:30 UK)
        plan_row = self.ledger.latest_plan()
        rel_moves, vetoed, floors = D.release(segs, plan_row, w, fd.approvals, self._vetoed_plans(plan_row, fd))
        if plan_row and not plan_row.get("released_utc") and parse_utc((plan_row.get("plan") or {}).get("release_utc")) \
                and now >= parse_utc(plan_row["plan"]["release_utc"]):
            meta_release = (plan_row["date"], vetoed)
        else:
            meta_release = None
        for m in rel_moves:
            if m.rule in ("VETO", "RELEASE_SKIP"):
                notes.append(Change("plan", m.key, m.words, rule=m.rule, numbers={"item": m.item}))
            else:
                moves.append(m)
            if m.rule in ("F1_KEEP", "F1_FAIL") and m.numbers.get("judged"):
                judged[m.key] = Judgement(m.numbers["verdict"], int(m.numbers["live_trades"]),
                                          str(m.numbers["judged_utc"]))
                meta["judged"] = {k: j.to_dict() for k, j in judged.items()}
        if meta_release is not None or any(m.rule in ("F1_KEEP", "F1_FAIL") for m in rel_moves):
            w = self._world(now, cfg, env, mode, ev, data_ok, br, demo_ok, fd, judged, stop, segs)
        applied = D.apply(segs, moves, now)
        # ---- the day's checks: only ever restrict
        tick_moves, needs = D.tick(segs, w)
        applied += D.apply(segs, tick_moves, now)
        # ---- the nightly review
        plan = None
        night = None
        if due is not None:
            night = self._nightly(segs, w, ev, fd, due, now)
            plan = night["plan"]
            applied += D.apply(segs, night["downs"], now)
            meta["nightly_date"] = due.isoformat()
        # floors released with the plan
        floors_now = self.ledger.floors()
        floor_changes = []
        for f in floors:
            k = f["key"]
            cur = floors_now.get(k, {})
            vals = {"min_score": cur.get("min_score"), "min_rr": cur.get("min_rr")}
            if f["action"] == "revert":
                vals[f["field"]] = None
            else:
                before = vals.get(f["field"])
                vals[f["field"]] = f["value"] if before is None else max(float(before), float(f["value"]))
            floor_changes.append((k, vals))
            notes.append(Change("floor", k, say("FLOOR", what=key_words(k), action="switched on" if f["action"] == "switch"
                                                else "reverted", field=f["field"], value=f["value"],
                                                p=(f.get("numbers") or {}).get("p")), rule="TWEAK",
                                numbers=f.get("numbers") or {}))
        # ---- the policy
        s2 = D.s2_blocks(w)
        ents = D.entries(segs, w)
        floors_after = dict(floors_now)
        for k, vals in floor_changes:
            if vals["min_score"] is None and vals["min_rr"] is None:
                floors_after.pop(k, None)
            else:
                floors_after[k] = {**vals, "since_utc": now.isoformat()}
        pol_floors = {}
        for k, f in floors_after.items():
            if w.pinned(k) is not None:
                continue
            try:
                pol_floors[k] = Floor(f.get("min_score"), f.get("min_rr"))
            except Exception:
                continue
        demo_block = D.demo_limits(segs, w, ents)
        seq = max(self.ledger.next_seq(), int((self.last_policy or {}).get("seq", 0)) + 1)
        login = int(getattr(cfg, "account_login", 0) or 0) or int((fd.weather.raw or {}).get("account_login") or 0)
        equity = fd.page.equity
        policy = Policy(
            seq=seq, written_utc=now.isoformat(), envelope_sha=fd.envelope.sha256, mode=mode,
            grants_allowed=bool(data_ok), entries=ents, valid_s=LEASE_SECONDS, account_login=login, build=self.build,
            data={"ok": bool(data_ok), "failed": [short(x, 200) for x in failed][:20],
                  "broker_check": {"records_today": recon.get("records_today"), "page_today": recon.get("page_today")}},
            account={"equity": equity, "e0": acct.e0, "real_today": acct.real_today,
                     "open_real_risk": acct.open_real_risk, "peak": br.peak},
            brakes=br.policy_block(), headroom_gbp=br.headroom_gbp, floors=pol_floors, blocks=D.blocks(s2),
            gates=Gates(), demo=demo_block, judged=dict(judged))
        # state changes and brake changes as ledger rows
        rows: list[Change] = []
        for m in applied:
            kind = "pause" if m.pause_until else "state"
            rows.append(Change(kind, m.key, m.words, m.from_text, m.to_text if not m.pause_until else f"PAUSED:{m.pause_until}",
                               m.rule, dict(m.numbers, kind=m.kind, item=m.item)))
        for name, a, b, words_, nums in brake_moves:
            rows.append(Change("brake", name, words_, a, b, {"day": "B1" if b == "LINE" else "B2", "week": "B3",
                                                            "drawdown": "B4"}.get(name, "brake")
                               if b != "OPEN" else "release", nums))
        rows += notes
        written = self._write(policy, fd, now, rows, segs, meta, floor_changes, new_peak, meta_release)
        # ---- status.json (no heartbeat)
        doc = self._status(now, w, fd, written, written, br, needs, plan or self._plan_view(), recon, failed,
                           mode_words, demo_why, segs)
        S.write(self.paths, doc)
        self.last_status = doc
        if night is not None:
            self._night_files(due, doc, night, written)
        return {"ok": True, "mode": mode, "policy": written, "status": doc, "applied": applied, "plan": plan}

    # ---------------------------------------------------------- pieces --
    def _demo_test(self, cfg, now) -> tuple[bool, str]:
        try:
            from .gate import CoachGate
            g = CoachGate(BOT_TNB, self.data_dir, cfg, clock=lambda: now, log_decisions=False)
            return g.demo_test(now)
        except Exception as exc:
            return False, f"the demo test could not run ({exc})"

    def _judged(self) -> dict:
        out = {}
        for k, v in (self.ledger.get("judged") or {}).items():
            try:
                out[k] = Judgement(str(v["verdict"]), int(v["live_trades"]), str(v["judged_utc"]))
            except Exception:
                continue
        return out

    def _account(self, fd: F.Feed, ev, now) -> tuple[B.Account, Optional[dict]]:
        page = fd.page
        today = uk_day(now).isoformat()
        e0m = self.ledger.get("e0") or {}
        e0 = None
        new = None
        if e0m.get("day") == today and _fin(e0m.get("value")):
            e0 = float(e0m["value"])
        elif page.equity is not None and page.real_today is not None:
            e0 = round(page.equity - page.real_today, 2)
            new = {"day": today, "value": e0}
        week = None
        if page.real_today is not None:
            back = R.trading_days_back(uk_day(now), 5)
            past = sum(t.net_gbp for t in ev.trades if t.real and back <= t.uk_day < uk_day(now))
            week = round(past + page.real_today, 2)
        return B.Account(page.equity, e0, page.real_today, fd.open_risk, week), new

    def _trial_losses(self, trades, changes, now) -> float:
        wk = uk_day(now).isocalendar()[:2]
        net = 0.0
        for t in trades:
            if not t.real or t.coach_route != "REAL" or t.uk_day.isocalendar()[:2] != wk:
                continue
            st, _ = D.state_at(changes, t.key, t.opened_utc)
            if st != STATE_LIVE_TRIAL:
                gl = None
                for c in changes:
                    if c.target != t.key and D.matches(c.target, t.key):
                        gl = D.state_at(changes, c.target, t.opened_utc)[0]
                        break
                if gl != STATE_LIVE_TRIAL:
                    continue
            net += t.net_gbp
        return round(max(0.0, -net), 2)

    def _mode(self, env, env_mode, mode_why, now, fd, pending: Optional[dict] = None) -> tuple[str, str]:
        mode, words = env_mode, ""
        guard = self.ledger.get("guard")
        if guard:
            cleared = any(a["id"] == "guard" and a["action"] == "approve" and a.get("ts", "") > guard.get("since", "")
                          for a in fd.approvals)
            if cleared:
                self.ledger.commit([Change("mode", "coach", say("MODE", to="out of its own GUARD",
                                                                why="you approved it"), MODE_GUARD, env_mode, "B9",
                                           by="aaron")], meta={"guard": None}, now=now)
            else:
                mode = envelope_mod.stricter(mode, MODE_GUARD)
                words = f"In GUARD by its own rule ({guard.get('why')}): it may only restrict."
        sc = (pending or {}).get("self_check") or self.ledger.get("self_check")
        if isinstance(sc, dict) and sc.get("passed") is False:
            mode = envelope_mod.stricter(mode, MODE_WATCH)
            words = say("WATCH_FAIL", why=sc.get("why", ""))
        if not words:
            words = {MODE_PILOT: "in charge within your limits", MODE_GUARD: "may only restrict (GUARD)",
                     MODE_WATCH: f"Watching only - it changes nothing today{(' (' + mode_why + ')') if mode_why else ''}."
                     }.get(mode, mode)
        return mode, words

    def _refresh_fit(self, trades, segs, judged, env, now, *, full: bool) -> None:
        pins = P.active_pins(env, judged)
        registered = {k: parse_utc(s.registered_utc) for k, s in segs.items() if s.registered_utc and not k.startswith("@")}
        live_keys = R.coach_real_keys(trades) | {k for k, s in segs.items() if s.state in (STATE_LIVE, STATE_LIVE_TRIAL)}
        taus = self.ledger.get("taus") or None
        if taus:
            taus = {int(k): float(v) for k, v in taus.items()}
        haircuts = self._haircuts(trades, update=full)
        self.haircuts = haircuts
        if full:
            # decisions tonight with tau held at last night's value; tonight's estimate is held from tomorrow
            f, new_taus = ES.nightly_fit(trades, now, held_taus=taus, registered=registered, live_keys=live_keys,
                                         pins=pins, haircuts=haircuts)
            self.ledger.set("taus", {str(k): v for k, v in new_taus.items()}, now)
            self.fit, self.fit_n = f, len(trades)
            return
        if self.fit is None:
            f = ES.fit(trades, now, registered=registered, live_keys=live_keys, pins=pins, haircuts=haircuts,
                       taus=taus, reestimate_tau=False)
            self.fit, self.fit_n = f, len(trades)
            return
        if len(trades) == self.fit_n:
            return
        gov = [k for k, s in segs.items() if s.state in (STATE_LIVE, STATE_LIVE_TRIAL) and not k.startswith("@")]
        for k in gov:
            if k in self.fit.keys:
                e = ES.refit_key(trades, k, now, self.fit.taus, registered=registered, live_keys=live_keys, pins=pins,
                                 haircuts=haircuts)
                if e is not None:
                    self.fit.keys[k] = e
        self.fit_n = len(trades)

    def _haircuts(self, trades, update: bool) -> dict:
        prev = getattr(self, "haircuts", None)
        if prev is not None and not update:
            return prev
        stored = self.ledger.get("haircuts") or {}
        by_key: dict = {}
        for t in trades:
            by_key.setdefault(t.key, []).append(t)
        out = {}
        save = {}
        for k, rows in by_key.items():
            if not any(t.real for t in rows) or all(t.real for t in rows):
                continue
            p = stored.get(k)
            before = R.Haircut(*p) if isinstance(p, list) and len(p) == 6 else None
            h = R.haircut(rows, before)
            out[k] = h
            save[k] = [h.h, h.measured, h.gap, h.se, h.real_n, h.practice_n]
        if update:
            self.ledger.set("haircuts", save)
        return out

    def _world(self, now, cfg, env, mode, ev, data_ok, br, demo_ok, fd, judged, stop, segs) -> D.World:
        return D.World(now, cfg, env, mode, ev.trades, data_ok=data_ok, unreadable_bots=tuple(ev.unreadable_bots()),
                       fit=self.fit, brakes=br, demo=demo_ok, equity=fd.page.equity, ian_mode=fd.ian_mode,
                       judged=dict(judged), stop_pct=stop, data_dir=str(self.data_dir),
                       haircuts=getattr(self, "haircuts", {}) or {})

    # ----------------------------------------------------------- nightly --
    def _vetoed_plans(self, plan_row: Optional[dict], fd: F.Feed) -> dict:
        """The earlier plans Aaron's vetoes name (date -> plan), for a veto of a plan replaced before its
        release (Friday's, replaced by the weekend's nightlies for the same Monday release)."""
        out: dict = {}
        if not plan_row:
            return out
        for a in fd.approvals or ():
            pd = str(a.get("plan") or "")
            if a.get("action") != "veto" or not pd or pd == plan_row.get("date") or pd in out:
                continue
            try:
                row = self.ledger.plan(pd)
            except Exception:
                row = None
            if row is not None and not row.get("released_utc"):
                out[pd] = row.get("plan") or {}
        return out

    def _nightly(self, segs, w: D.World, ev, fd: F.Feed, day: dt.date, now: dt.datetime) -> dict:
        registered = {k: parse_utc(s.registered_utc) for k, s in segs.items() if s.registered_utc and not k.startswith("@")}
        live_keys = R.coach_real_keys(ev.trades) | {k for k, s in segs.items() if s.state in (STATE_LIVE, STATE_LIVE_TRIAL)}
        luck = None
        try:
            kw = {} if self.shuffles is None else {"shuffles": self.shuffles}
            luck = RU.luck_check(ev.trades, now, taus=self.fit.taus if self.fit else None, registered=registered,
                                 live_keys=live_keys, pins=w.pins, haircuts=w.haircuts, **kw)
            self.ledger.put_luck(day.isoformat(), luck.shuffles, luck.rate, luck.blocked)
        except Exception as exc:
            log.warning("the luck check failed: %s", exc)
        g3_changes = self.ledger.changes(kinds=["state"], limit=2000)
        s2 = D.s2_blocks(w)
        downs, plan = D.nightly(segs, w, luck=luck, changes=g3_changes, approvals=fd.approvals,
                                floors_now=self.ledger.floors(), blocked_symbols=[r.target for r in s2])
        if luck is None:
            plan.notes.append("The luck check could not run tonight: no promotion.")
            plan.items = [i for i in plan.items if i.get("direction") != D.UP]
        plan.date = day.isoformat()
        plan.release_utc = next_release_utc(nightly_instant(day) + dt.timedelta(minutes=1)).isoformat()
        self.ledger.put_plan(plan.date, plan.as_dict())
        # the scorecard (section 6.4)
        sizes = w.sizes
        sc = D.scorecard(ev.trades, ev.gate_log, sizes)
        self.ledger.put_scorecard(plan.date, sc["saved_gbp"], sc["added_gbp"], sc["trades"])
        if (sc["days"] >= SCORECARD_MIN_DAYS and sc["trades"] >= SCORECARD_MIN_TRADES and sc["net_gbp"] < 0
                and (sc["p_negative"] or 0) > SCORECARD_GUARD_P):
            self._guard(now, say("SCORECARD_GUARD", days=sc["days"], trades=sc["trades"], net=gbp(sc["net_gbp"], True)))
        # the shadow figure (section 21)
        try:
            self.ledger.put_shadow(D.shadow_rows(ev.trades, g3_changes, sizes, ev.gate_log))
        except Exception as exc:
            log.warning("the shadow figures could not be stored: %s", exc)
        # the page's real figure per UK day (never estimated)
        prog = fd.page.progress if fd.page.ok else None
        rows = (prog or {}).get("days") if isinstance(prog, dict) else None
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, dict) and r.get("day"):
                    v = r.get("net")
                    self.ledger.put_daily(str(r["day"]), float(v) if _fin(v) else None,
                                          "the page's coach_progress (MetaTrader, every bot, after commission)")
        else:
            self.ledger.put_daily(day.isoformat(), None, "could not be counted: the page's progress rows were not read")
        return {"plan": plan, "downs": downs, "luck": luck, "scorecard": sc}

    def _night_files(self, day: dt.date, status_doc: dict, night: dict, policy_obj: Optional[dict]) -> None:
        try:
            since = nightly_instant(day) - dt.timedelta(days=1)
            changes = self.ledger.changes(since=since, limit=2000)
            luck = night["luck"].numbers if night.get("luck") is not None else None
            REPORT.write_night(self.paths, day.isoformat(), status=status_doc, plan=night["plan"].as_dict(),
                                       changes=changes, luck=luck, scorecard=night.get("scorecard"),
                                       policy_obj=policy_obj, suggestions=status_doc.get("suggestions") or [])
            if self.upload is not None:
                self.upload(REPORT.upload_files(self.paths, day.isoformat()), day.isoformat())
        except Exception as exc:
            log.warning("the coach's nightly files could not be written or sent: %s", exc)

    # ------------------------------------------------------------ writer --
    def _diff(self, old: Optional[dict], new: dict) -> list[Change]:
        """One change row per difference between two policies (the instructions to the bots)."""
        old = old or {}
        out: list[Change] = []
        oe, ne = old.get("entries") or {}, new.get("entries") or {}

        def sig(e):
            return (e.get("route"), e.get("state"), e.get("money_gbp"), e.get("step"), e.get("rule"), e.get("until_utc"))
        for k in sorted(set(oe) | set(ne)):
            a, b = oe.get(k), ne.get(k)
            if a is not None and b is not None and sig(a) == sig(b):
                continue
            if b is None:
                words = say("ENTRY_GONE", what=key_words(k), why="its instruction ended")
                out.append(Change("entry", k, words, a.get("route"), "BASELINE", a.get("rule")))
                continue
            words = b.get("why") or say("BACK_TO_SETTINGS", what=key_words(k), why="the coach's instruction changed")
            if len(words) < 20:
                words = say("BACK_TO_SETTINGS", what=key_words(k), why=words)
            out.append(Change("entry", k, words, (a or {}).get("route", "BASELINE"), b.get("route"), b.get("rule"),
                              {k2: v for k2, v in b.items() if k2 in ("money_gbp", "risk_pct", "step", "state")}))
        ob = {(tuple(x.get("bots") or ()), tuple(x.get("symbols") or ()), x.get("rule")) for x in old.get("blocks") or []}
        nb = {(tuple(x.get("bots") or ()), tuple(x.get("symbols") or ()), x.get("rule")) for x in new.get("blocks") or []}
        osyms = {s for b in ob for s in b[1]}
        nsyms = {s for b in nb for s in b[1]}
        for s in sorted(nsyms - osyms):
            out.append(Change("block", s, f"{s} is practice-only for the coach's keys (Aaron's rule S2).", "", "PRACTICE",
                              "S2"))
        for s in sorted(osyms - nsyms):
            out.append(Change("block", s, say("BLOCK_OFF", symbol=s, why="Aaron's rule no longer fires on it"),
                              "PRACTICE", "", "S2"))
        for k in sorted(set(old.get("floors") or {}) | set(new.get("floors") or {})):
            a, b = (old.get("floors") or {}).get(k), (new.get("floors") or {}).get(k)
            if a != b:
                out.append(Change("floor", k, say("FLOOR", what=key_words(k), action="set" if b else "removed",
                                                  field="floor", value=json.dumps(b or a), p="-"),
                                  json.dumps(a), json.dumps(b), "TWEAK"))
        if old.get("mode") != new.get("mode"):
            out.append(Change("mode", "coach", say("MODE", to=new.get("mode"), why="its envelope, its WATCH day or its "
                                                                                     "own GUARD"),
                              old.get("mode"), new.get("mode"), "MODE"))
        if old.get("grants_allowed") != new.get("grants_allowed"):
            out.append(Change("grants", "coach", say("GRANTS", to="allowed" if new.get("grants_allowed") else "stopped",
                                                     why="; ".join((new.get("data") or {}).get("failed") or [])
                                                     or "the data checks passed"),
                              str(old.get("grants_allowed")), str(new.get("grants_allowed")), "B8"))
        if (old.get("demo") or {}) != (new.get("demo") or {}):
            out.append(Change("demo", "account", say("DEMO", what="changed", detail=json.dumps(new.get("demo") or {})
                                                     if new.get("demo") else "back to your own (real-money) limits"),
                              json.dumps(old.get("demo")), json.dumps(new.get("demo")), "S21"))
        if (old.get("judged") or {}) != (new.get("judged") or {}):
            out.append(Change("judged", F1_KEY, say("BACK_TO_SETTINGS", what="Formula 1's judgement",
                                                    why=json.dumps(new.get("judged") or {})),
                              json.dumps(old.get("judged")), json.dumps(new.get("judged")), "F1"))
        return out

    def _write(self, policy: Policy, fd: F.Feed, now, rows: list, segs: dict, meta: dict, floor_changes,
               new_peak, meta_release) -> dict:
        ctx = P.CheckContext.from_config(fd.cfg, fd.envelope.envelope, fd.envelope.sha256, now=now,
                                         last_seq=int((self.last_policy or {}).get("seq", -1)))
        sealed = P.seal(policy)
        obj = P.parse_bytes(P.encode(sealed))
        P.validate(obj, ctx, raw_size=len(P.encode(sealed)))            # 2: the validator the gate runs
        diffs = self._diff(self.last_policy, obj)                        # 3
        changed = bool(diffs)
        all_rows = rows + diffs
        ids = self.ledger.commit(all_rows, segs.values(), meta, policy_seq=policy.seq, now=now)   # 4
        # why_id: each entry's latest sentence
        why = {}
        for c, i in zip(all_rows, ids):
            why[c.target] = i
        old_why = {k: e.get("why_id") for k, e in ((self.last_policy or {}).get("entries") or {}).items()}
        ents = {}
        for k, e in policy.entries.items():
            wid = why.get(k, old_why.get(k))
            ents[k] = dataclasses.replace(e, why_id=int(wid) if wid is not None else None)
        policy = dataclasses.replace(policy, entries=ents)
        sealed = P.seal(policy)
        body = P.encode(sealed)
        self.ledger.add_policy(sealed.seq, sealed.written_utc, sealed.sha256, body if changed or self.last_policy is None
                               else None)
        for k, vals in floor_changes:
            self.ledger.set_floor(k, vals.get("min_score"), vals.get("min_rr"), now.isoformat())
        if new_peak is not None:
            self.ledger.set_peak(now, new_peak)
        if meta_release is not None:
            self.ledger.mark_released(meta_release[0], now, meta_release[1])
        P.write(self.paths, sealed, ctx)                                  # 5 (validates once more)
        self.ledger.mark_valid(sealed.seq)                                # 6
        self.ledger.prune_policies(now)
        self.last_policy = json.loads(body.decode("ascii"))
        return self.last_policy

    # ------------------------------------------------------------- audit --
    def _audit_meta(self, now, cfg, env, equity, data_ok: bool, demo: bool = False) -> dict:
        """The audit (every real decision in the gate logs <= min(ceiling x E, its grant), against the policy in
        force), B9's counts and the WATCH-day tallies. Returns meta to commit."""
        out = {}
        seen = self.ledger.get("audit_seen") or {}
        violations = []
        eq = float(equity or 0.0)
        r = cfg.risk
        ceiling = float(r.max_risk_pct)
        last = self.last_policy or {}
        demo_cap = (last.get("demo") or {}).get("max_risk_pct")
        if _fin(demo_cap):
            ceiling = max(ceiling, float(demo_cap))
        grants = last.get("entries") or {}
        would_over = 0
        gate_dir = self.paths.gate_dir
        if gate_dir.is_dir() and eq > 0:
            for f in sorted(gate_dir.glob(f"*-{uk_day(now).isoformat()}.jsonl")):
                try:
                    lines = f.read_text(errors="replace").splitlines()
                except OSError:
                    continue
                start = int(seen.get(f.name, 0))
                for line in lines[start:]:
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    if d.get("event") == "refused_at_ceiling":
                        violations.append(f"a real order was refused at the ceiling ({d.get('symbol')})")
                        continue
                    if d.get("route") == ROUTE_REAL and _fin(d.get("money")):
                        grant = grants.get(str(d.get("key"))) or {}
                        cap = ceiling * eq / 100.0
                        if _fin(grant.get("money_gbp")):
                            cap = min(cap, float(grant["money_gbp"]) * 1.05 + 0.01)
                        if float(d["money"]) > cap + 0.01:
                            violations.append(f"a real decision of {gbp(d['money'])} on {d.get('key')} was above "
                                              f"{gbp(cap)}")
                    wd = d.get("would") or {}
                    if wd.get("route") == ROUTE_REAL and _fin(wd.get("money")) and float(wd["money"]) > ceiling * eq / 100 + 0.01:
                        would_over += 1
                seen[f.name] = len(lines)
        out["audit_seen"] = {k: v for k, v in seen.items() if uk_day(now).isoformat() in k}
        # B9
        why9 = []
        if violations:
            why9.append("audit: " + violations[0])
        n24 = self.ledger.policy_changes_since(now - dt.timedelta(hours=24))
        if n24 > B9_MAX_POLICY_CHANGES_24H:
            why9.append(f"{n24} policy changes in 24 hours")
        wk = uk_day(now).isocalendar()[:2]
        starts = sum(1 for c in self.ledger.changes(kinds=["state"], limit=2000)
                     if c.rule == "G1" and c.ts_utc and uk_day(parse_utc(c.ts_utc)).isocalendar()[:2] == wk)
        # section 21 allows up to DEMO_MAX_TRIALS trials at once on the demo; B9 is scaled to it there, so
        # the coach's own breaker does not trip before the demo's limit can be reached in one week
        most_starts = max(B9_MAX_TRIAL_STARTS_WEEK, DEMO_MAX_TRIALS) if demo else B9_MAX_TRIAL_STARTS_WEEK
        if starts > most_starts:
            why9.append(f"{starts} trial starts this week")
        if why9 and not self.ledger.get("guard"):
            self._guard(now, "; ".join(why9))
        # WATCH day
        until = parse_utc(env.watch_until_utc) if env.watch_until_utc else None
        if env.mode == MODE_PILOT and until is not None:
            wd = self.ledger.get("watchday") or {}
            if now < until:
                wd["ticks"] = int(wd.get("ticks", 0)) + 1
                wd["recon_ok"] = int(wd.get("recon_ok", 0)) + (1 if data_ok else 0)
                wd["over_cap"] = int(wd.get("over_cap", 0)) + would_over + len(violations)
                out["watchday"] = wd
            elif self.ledger.get("self_check") is None:
                ticks = int(wd.get("ticks", 0))
                why = []
                if int(wd.get("validation_errors", 0)):
                    why.append(f"{wd['validation_errors']} policy validation errors")
                if int(wd.get("over_cap", 0)):
                    why.append(f"{wd['over_cap']} would-be decisions above a cap")
                share = (int(wd.get("recon_ok", 0)) / ticks) if ticks else 0.0
                if share < WATCHDAY_RECON_SHARE:
                    why.append(f"the broker check was ok on {share:.0%} of ticks (95% needed)")
                out["self_check"] = {"passed": not why, "why": "; ".join(why), "at": now.isoformat()}
        return out

    # ------------------------------------------------------------ status --
    def _plan_view(self) -> Optional[D.Plan]:
        row = self.ledger.latest_plan()
        if not row or row.get("released_utc"):
            return None
        p = row["plan"]
        return D.Plan(p.get("date", row["date"]), p.get("release_utc", ""), p.get("items") or [], p.get("luck") or {},
                      p.get("notes") or [], p.get("floors") or [])

    def _status(self, now, w: D.World, fd: F.Feed, policy_obj: dict, written: dict, br: B.BrakeState, needs: list,
                plan: Optional[D.Plan], recon: dict, failed: list, mode_words: str, demo_why: str, segs: dict) -> dict:
        ents = policy_obj.get("entries") or {}
        keys = []
        for k, seg in sorted(segs.items()):
            if k.startswith("@"):
                continue
            e = w.est(k) if seg.state != STATE_PRACTICE else (w.fit.keys.get(k) if w.fit else None)
            ent = ents.get(k) or {}
            if seg.state == STATE_PRACTICE and not ent and e is None:
                continue
            keys.append({"key": k, "words": key_words(k), "state": ent.get("state") or seg.state, "step": seg.step,
                         "route": ent.get("route", "BASELINE"), "money": ent.get("money_gbp"), "rule": seg.rule or None,
                         "why": ent.get("why"),
                         "p_plus": None if e is None else round(e.p_plus, 3),
                         "lcb80": None if e is None else round(e.lcb80, 3),
                         "p_plus_real": None if e is None else round(e.p_plus_real, 3),
                         "trades": None if e is None else e.n, "real_trades": None if e is None else e.n_real})
        rank = {"REAL": 0, "PIN": 1, "BASELINE": 2, "PRACTICE": 3, "SKIP": 3}
        keys.sort(key=lambda x: (rank.get(x["route"], 4), -(x["p_plus"] or 0), x["key"]))
        recent = self.ledger.changes(limit=S.MAX_CHANGES)
        needs_you = list(dict.fromkeys(needs))
        if br.drawdown == "PAUSE":
            needs_you.append(br.why.get("drawdown", "The deeper drawdown brake is on."))
        for k, seg in segs.items():
            if seg.rule == "K1":
                needs_you.append(f"{key_words(k)}: the plan's rule says back to paper - switching it to PAPER is your "
                                 f"call.")
        for b in w.unreadable_bots:
            needs_you.append(f"{BOT_LABEL.get(b, b)}'s own record could not be read: paused if it is governed.")
        guard = self.ledger.get("guard")
        if guard:
            needs_you.append(f"The coach put itself in GUARD ({guard.get('why')}). 'python -m mintel.coach approve "
                             f"guard' lets it out once you have looked.")
        sugg = []
        for r in D.s2_blocks(w):
            sugg.append(f"Add {r.target} to universe.excluded_symbols at the next install ({r.words()}).")
        yes = []
        f1 = D.formula1_live(w)
        if F1_KEY not in w.judged:
            yes.append(f"Formula 1's judgement after 20 live trades ({max(0, 20 - f1['n'])} to go).")
        elif w.judged[F1_KEY].verdict == "KEEP":
            yes.append("Formula 1 was kept: jump to the top size (1.5%) is your choice - 'python -m mintel.coach "
                       "approve f1-jump'.")
        ian_real = sum(1 for t in w.trades if t.bot == "ian" and t.real)
        yes.append(f"Let the 3% stop also count Financial Ian (it does not today; {ian_real} real Financial Ian "
                   f"trades on record).")
        yes.append("Apply the headroom check to Formula 1 too, so stacked stops can never take a day past 3% "
                   "(not measured yet).")
        yes += self._lab_ready()
        demo_line = ("Demo mode: limits are lifted step by step as the bots earn it (your instruction of 12 Oct). "
                     "Every trade still has a stop." if w.demo else f"Real-money limits in force ({demo_why}).")
        shadow = self.ledger.shadow(since=dt.datetime.combine(uk_day(now), dt.time(0), tzinfo=TZ_UK))
        if shadow:
            demo_line += (f" Coach-sized trades today {gbp(sum(r['net_gbp'] for r in shadow), True)}; at real-money "
                          f"sizes {gbp(sum(r['real_money_net_gbp'] for r in shadow), True)}.")
        pol = {"seq": written.get("seq"), "written_utc": written.get("written_utc"), "valid_s": written.get("valid_s"),
               "sha256": written.get("sha256")}
        luck = self.ledger.luck()
        sc = self.ledger.scorecard()
        return S.base(
            now, policy_obj.get("mode"), mode_words, policy=pol, grants_allowed=policy_obj.get("grants_allowed"),
            data={"ok": not failed, "failed": failed, "broker_check": recon}, account=policy_obj.get("account"),
            brakes=br.policy_block(), brake_words=br.why, headroom_gbp=br.headroom_gbp,
            stop_pct=w.stop_pct, demo={"test": w.demo, "why": demo_why, "limits": policy_obj.get("demo")},
            demo_line=demo_line, keys=keys[:S.MAX_KEYS],
            plan=None if plan is None else {"date": plan.date, "release_utc": plan.release_utc,
                                            "items": [{"item": i.get("item"), "words": i.get("words"), "rule": i.get("rule"),
                                                       "key": i.get("key")} for i in plan.items],
                                            "notes": plan.notes, "floors": plan.floors, "luck": plan.luck},
            changes=[{"ts_utc": c.ts_utc, "kind": c.kind, "target": c.target, "rule": c.rule, "words": c.words,
                      "by": c.by} for c in recent],
            needs_you=needs_you, needs_your_yes=yes, suggestions=sugg, market_rule=D.market_watch(w),
            luck=luck, scorecard=sc[-1] if sc else None,
            board=W.words(fd.weather, now, self.board.update(fd.weather.symbols, now) if fd.weather.ok else None),
            phase=W.session_phase(now))

    def _lab_ready(self) -> list[str]:
        """The lab's READY FOR LIVE candidates, with the lab's own sentence unchanged (data/lab/summary.json,
        read only; the coach never imports the lab)."""
        try:
            obj = json.loads((self.data_dir / "lab" / "summary.json").read_text())
        except Exception:
            return ["Lab ideas ready for live: none yet."]
        out = []
        for c in (obj.get("candidates") or []):
            if isinstance(c, dict) and str(c.get("stage") or "").upper() == "READY FOR LIVE":
                out.append(f"Lab idea {c.get('name') or c.get('id')}: {c.get('headline') or c.get('words') or ''} "
                           f"- a live trial is your word only.")
        if obj.get("synthetic"):
            out = [f"TEST DATA - not a result: {x}" for x in out]
        return out or ["Lab ideas ready for live: none yet."]

    # -------------------------------------------------------------- loop --
    def run_forever(self, sleep: Callable[[float], None] = time.sleep, stop: Optional[Callable[[], bool]] = None) -> None:
        """The 60-second tick, for ever (the watchdog keeps the process alive)."""
        while not (stop and stop()):
            self.tick()
            sleep(60.0)


# =====================================================================
# Replay (section 18, test_coach_replay): each day decided from earlier data only
# =====================================================================
def load_reports_csv(path: "str | Path") -> list:
    """Trend & Breakout's closed trades from a reports-branch ``trades.csv`` copy (tests/fixtures): planned
    risk is gross over price-R (the record has no risk column), the median for the few with no usable price-R
    (flagged ``risk_estimated``). Numbers only from the record."""
    import csv
    import statistics
    from ..engine.tactics import approach_of
    rows = [r for r in csv.DictReader(l for l in Path(path).open() if not l.startswith("#"))]

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
    risks = [(num(r["net_money"]) - num(r["commission"])) / num(r["realised_r"]) for r in rows
             if abs(num(r["realised_r"])) >= 0.1]
    median = statistics.median([x for x in risks if x > 0])
    out = []
    for r in rows:
        net, com, pr = num(r["net_money"]), num(r["commission"]), num(r["realised_r"])
        risk = (net - com) / pr if abs(pr) >= 0.1 else 0.0
        out.append(R.Trade("journal", "tnb", int(r["ticket"]), approach_of(r["tactic"]), R.kind_of(r["symbol"]),
                           r["regime"].upper(), r["symbol"], r["side"], R.parse_utc(r["opened_utc"]),
                           R.parse_utc(r["closed_utc"]), r["mode"] != "PAPER", net, abs(com),
                           risk if risk > 0 else median, num(r.get("score")) or None,
                           flags=(() if risk > 0 else (R.F_RISK_ESTIMATED,))
                           + ((R.F_TWIN,) if str(r["tactic"]).strip().upper().endswith("_2X") else ())))
    return R.assign_clusters(out)


def replay(trades: Sequence, start: dt.date, end: dt.date, cfg: Any, env: Any, *, shuffles: Optional[int] = None,
           equity: float = 1865.61) -> list[dict]:
    """What the coach would have done each UK trading day from ``start`` to ``end``: every night's review
    sees only trades closed by then; the plan is released the next morning. The keys were registered when the
    replay starts, as on the day of the install. Each row: the date, what was planned and released, the keys
    real under the coach, and the record's real net that day - numbers only from the records. Stamped
    "flatters": the segments were picked by people looking at this same record."""
    led = Ledger(":memory:")
    segs: dict = {}
    out = []
    day = start
    judged: dict = {}
    taus = None
    trades = list(trades)
    first = True
    while day <= end:
        if not is_uk_trading_day(day):
            day += dt.timedelta(days=1)
            continue
        night = nightly_instant(day)
        seen = [t for t in trades if t.closed_utc <= night]
        morning = next_release_utc(night + dt.timedelta(minutes=1))
        # the release of last night's plan (06:30 this morning)
        row = led.latest_plan()
        released = []
        if row and not row.get("released_utc"):
            rel = parse_utc(row["plan"]["release_utc"])
            wr = D.World(rel, cfg, env, MODE_PILOT, [t for t in trades if t.closed_utc <= rel], data_ok=True,
                         fit=None, equity=equity, judged=judged)
            mv, vetoed, _ = D.release(segs, row, wr, [])
            applied = D.apply(segs, [m for m in mv if m.rule not in ("VETO", "RELEASE_SKIP")], rel)
            released = [f"{m.rule} {m.key} -> {m.to_text}" for m in applied]
            led.mark_released(row["date"], rel, vetoed)
        f, next_taus = ES.nightly_fit(seen, night, held_taus=taus,
                                      registered={k: parse_utc(s.registered_utc) for k, s in segs.items()
                                                  if s.registered_utc and not k.startswith("@")},
                                      pins=P.active_pins(env, judged))
        taus = next_taus                                     # held from the next night
        w = D.World(night, cfg, env, MODE_PILOT, seen, data_ok=True, fit=f, equity=equity, judged=judged)
        if first:
            D.sync(segs, w)
            first = False
        else:
            D.apply(segs, D.sync(segs, w), night)
        tick_moves, _ = D.tick(segs, w)
        downs_day = D.apply(segs, tick_moves, night)
        kw = {} if shuffles is None else {"shuffles": shuffles}
        luck = RU.luck_check(seen, night, taus=f.taus, pins=w.pins,      # tonight's decisions: held tau
                             registered={k: parse_utc(s.registered_utc) for k, s in segs.items()
                                         if s.registered_utc and not k.startswith("@")}, **kw)
        downs, plan = D.nightly(segs, w, luck=luck)
        downs_night = D.apply(segs, downs, night)
        plan.release_utc = morning.isoformat()
        led.put_plan(day.isoformat(), plan.as_dict())
        real_today = round(sum(t.net_gbp for t in seen if t.real and t.uk_day == day), 2)
        out.append({"date": day.isoformat(), "trades_seen": len(seen), "real_net_that_day": real_today,
                    "released_this_morning": released,
                    "planned_tonight": [f"{i['rule']} {i['key']} -> {i['to_state']}" for i in plan.items],
                    "moves_down": [f"{m.rule} {m.key} -> {m.to_text}" for m in downs_day + downs_night],
                    "real_under_coach": sorted(k for k, s in segs.items() if s.state in (STATE_LIVE, STATE_LIVE_TRIAL)),
                    "luck": luck.numbers, "stamp": REPORT.FLATTERS})
        day += dt.timedelta(days=1)
    led.close()
    return out
