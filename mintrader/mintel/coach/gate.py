"""The gate: the coach's instruction, obeyed inside each bot (spec sections 10.3, 10.4 and 21).

``CoachGate(bot, data_dir, cfg).decide(...)`` is asked once, at each entry
decision. It never raises: a bug inside it gives BASELINE (Aaron's own
settings) and an error line in the log. Cost: four ``stat()`` calls a
decision (``data/HALT``, ``data/COACH-OFF``, ``data/coach.json``,
``data/coach/policy.json``), plus a parse only when a file has changed.

The algorithm, in order:

1. ``data/HALT`` exists: no real entry. Trend & Breakout's goes to PRACTICE,
   any other bot's is skipped. Practice is unaffected. HALT is Aaron's switch
   and holds whatever the coach, the envelope or COACH-OFF say.
2. ``data/COACH-OFF`` exists, or the envelope is missing, OFF or invalid, or
   the bot is not governed: BASELINE exactly (today's code path).
3. The policy is loaded and checked (``policy.py``: V1-V13, the same checks
   the coach's writer runs). Any failure makes the whole file invalid.
4. WATCH (or PILOT before the WATCH day ends): BASELINE; the would-be
   decision is logged.
5. A valid policy: the most specific entry for the LIVE condition (exact,
   ``..|*``, ``bot|*|KIND|*``, ``bot|*|*|*``), then floors, blocks, the
   market-hours gates, B5 headroom and B6 one idea. GUARD: never more than
   BASELINE (a REAL grant becomes BASELINE).
6. Invalid only through V6 (stale, integrity fine), or a last good policy in
   memory, or ``policy.prev.json`` passing V1-V5 and V13: BASELINE tightened
   by that policy's restrictions only (PRACTICE, SKIP, money caps that only
   lower, floors, blocks whose ``until_utc`` has not passed). Grants lapse.
7. No policy ever seen and none on disk (first install): BASELINE.
8. A REAL grant is clamped: ``min(money_gbp, risk_pct x E, the ceiling x E,
   the envelope's step cap x E, headroom)``. The ceiling is config.json's
   ``risk.max_risk_pct`` unless the section 21 demo test passes and the lease
   carries a demo step.
9. Any exception: BASELINE, logged.
10. Each decision for a real or would-be-real entry is appended to
    ``data/coach/gate/<bot>-<UK date>.jsonl``.

Aaron's pins (Formula 1 until its 20-live-trade judgement) give PIN: today's
code path byte for byte; only HALT applies.

Never UNCLEAR: a grant is never used in an UNCLEAR market.

``bot_may_enter(name)``: the gate for the bots that run inside the trader
(Momentum Runner, Band Breaker, Crowd Fader). Always True for a bot whose own
mode is not LIVE: practice is never gated.

``effective_limits()``: the account-level limits the trader's daily-loss
check reads. config.json's values unless a valid PILOT lease carries demo
values AND the demo test passes: config.json says DEMO and the broker's
account is a demo, as the trader writes it into ``data/coach/weather.json``
(``account_demo``), no older than 180 s and not contradicted. Anything else
(missing, stale, contradictory): the real-money values.

Imports: the standard library, this package and ``mintel.contracts`` only.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from . import (BOT_IAN, BOT_LABEL, BOT_MAGIC, BOT_RUNNER, BOT_TNB, CONDITION_UNCLEAR, DEMO_DRAWDOWN_BREAKER_PCT,
               DEMO_MAX_DAILY_STOP_PCT, DEMO_MAX_STEP_PCT, DEMO_TOTAL_MAX_PCT, DEMO_WEATHER_MAX_AGE_S,
               FUTURE_SKEW_SECONDS, MODE_GUARD, MODE_OFF, MODE_PILOT, MODE_WATCH, ONE_IDEA_SECONDS,
               ROUTE_BASELINE, ROUTE_PIN, ROUTE_PRACTICE, ROUTE_REAL, ROUTE_SKIP, SOURCE_ERROR, SOURCE_FALLBACK,
               SOURCE_HALT, SOURCE_OFF, SOURCE_POLICY, SOURCE_WATCH, STATE_LIVE_TRIAL, TZ_NY, WILD, Decision,
               Entry, Envelope, Key, Limits, Paths, Policy, RiskCtx, as_utc, parse_utc, uk_day)
from . import envelope as envelope_mod
from . import policy as policy_mod

log = logging.getLogger("mintel.coach.gate")

_RESTRICT = (ROUTE_PRACTICE, ROUTE_SKIP)
_LIVE_WORDS = ("LIVE",)


def _now_utc() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def _sig(path: Path) -> Optional[tuple]:
    """A file's identity for the reload test (None when it is not there)."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size, st.st_ino)


def _side_text(side: Any) -> str:
    v = getattr(side, "value", side)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return "BUY" if v > 0 else "SELL"
    return str(v or "").strip().upper()


def _money(x: float) -> float:
    """Rounded DOWN to the penny: a clamp never rounds up."""
    return math.floor(float(x) * 100.0 + 1e-9) / 100.0


def _gbp(x: Optional[float]) -> str:
    return "-" if x is None else f"£{x:,.2f}"


def _approach(name: Any) -> str:
    return policy_mod._approach(name)


@dataclass
class _View:
    """Everything one decision needs, read once."""
    off_why: str = ""
    env: Optional[Envelope] = None
    env_sha: str = ""
    mode: str = MODE_OFF
    mode_why: str = ""
    policy: Optional[Policy] = None           # valid: grants and restrictions
    fallback: Optional[Policy] = None         # restrictions only
    fallback_why: str = ""
    problems: list = field(default_factory=list)


class CoachGate:
    """The coach's gate for one bot (``"tnb"`` in the trader, ``"ian"`` in Financial Ian's process)."""

    def __init__(self, bot: str, data_dir: "str | Path", cfg: Any, *,
                 clock: Callable[[], dt.datetime] = _now_utc, account_login: int = 0, log_decisions: bool = True):
        self.bot = str(bot)
        self.paths = Paths.of(data_dir)
        self.cfg = cfg
        self.clock = clock
        self.account_login = int(account_login or getattr(cfg, "account_login", 0) or 0)
        self.log_decisions = bool(log_decisions)
        self._lock = threading.RLock()
        self._env_cache: tuple = (None, envelope_mod.EnvelopeState(None, "", "not read yet", exists=False))
        self._pol_cache: dict = {"sig": None, "obj": None, "error": "", "size": 0, "checked": None}
        self._prev_cache: dict = {"sig": None, "obj": None, "error": "", "size": 0, "checked": None}
        self._weather_cache: tuple = (None, None)
        self._last_seq = -1
        self._last_good: Optional[Policy] = None
        self.last_view: Optional[_View] = None
        self.errors = 0
        self._logged: dict = {}                    # the same decision is logged once a minute, not every scan

    # ---------------------------------------------------------------- files --
    def halted(self) -> bool:
        """``data/HALT`` (Aaron's switch) exists."""
        return os.path.exists(self.paths.halt)

    def coach_off(self) -> bool:
        return os.path.exists(self.paths.coach_off)

    def _risk_cfg(self):
        return self.cfg.risk

    def envelope_state(self) -> envelope_mod.EnvelopeState:
        with self._lock:
            sig = _sig(self.paths.envelope)
            if sig != self._env_cache[0] or sig is None:
                r = self._risk_cfg()
                state = envelope_mod.read(self.paths.envelope, float(r.max_risk_pct), float(r.max_daily_loss_pct))
                self._env_cache = (sig, state)
            return self._env_cache[1]

    def _check_ctx(self, env: Envelope, env_sha: str, last_seq: int) -> policy_mod.CheckContext:
        return policy_mod.CheckContext.from_config(self.cfg, env, env_sha, now=None, last_seq=last_seq,
                                                   account_login=self.account_login)

    def _read_checked(self, path: Path, cache: dict, env: Envelope, env_sha: str) -> dict:
        """The file's object and its time-independent problems (cached until the file, the envelope or the
        last sequence number seen changes)."""
        sig = _sig(path)
        key = (sig, env_sha, self._last_seq)
        if cache.get("key") == key:
            return cache
        cache.clear()
        cache.update({"key": key, "sig": sig, "obj": None, "error": "", "size": 0, "problems": []})
        if sig is None:
            cache["error"] = "missing"
            return cache
        raw = policy_mod.read_bytes(path)
        if raw is None:
            cache["error"] = "missing"
            return cache
        cache["size"] = len(raw)
        try:
            obj = policy_mod.parse_bytes(raw)
        except policy_mod.PolicyError as exc:
            cache["error"] = str(exc)
            cache["problems"] = [(exc.code, exc.message)]
            return cache
        cache["obj"] = obj
        cache["problems"] = policy_mod.problems(obj, self._check_ctx(env, env_sha, self._last_seq),
                                                raw_size=len(raw))
        return cache

    def _view(self, now: dt.datetime) -> _View:
        """Read the envelope and the policy once for this decision."""
        with self._lock:
            v = _View()
            if self.coach_off():
                v.off_why = "COACH-OFF is on (Aaron's switch)"
                self.last_view = v
                return v
            st = self.envelope_state()
            if not st.ok:
                v.off_why = st.error or "no valid envelope"
                self.last_view = v
                return v
            v.env, v.env_sha = st.envelope, st.sha256
            env_mode, v.mode_why = envelope_mod.effective_mode(v.env, now)
            if env_mode == MODE_OFF:
                v.off_why = "the envelope says OFF"
                self.last_view = v
                return v
            cur = self._read_checked(self.paths.policy, self._pol_cache, v.env, v.env_sha)
            lease = ""
            if cur["obj"] is not None and not cur["problems"]:
                lease = policy_mod.lease_problem(cur["obj"], now)
            if cur["obj"] is not None and not cur["problems"] and not lease:
                pol = policy_mod.to_policy(cur["obj"])
                v.policy = pol
                self._last_good = pol
                if pol.seq > self._last_seq:
                    self._last_seq = pol.seq
                    cur["key"] = (cur["sig"], v.env_sha, self._last_seq)     # unchanged file stays checked
                v.mode = envelope_mod.stricter(env_mode, pol.mode)
                if v.mode == MODE_OFF:
                    v.off_why = "the coach has switched itself off"
                elif v.mode == MODE_WATCH and env_mode != MODE_WATCH:
                    v.mode_why = "the coach itself is watching only"
            else:
                v.problems = list(cur["problems"]) + ([("V6", lease)] if lease else [])
                v.mode = env_mode
                if cur["obj"] is not None and not cur["problems"] and lease:
                    v.fallback = policy_mod.to_policy(cur["obj"])
                    v.fallback_why = f"the coach's instructions have lapsed ({lease})"
                elif self._last_good is not None:
                    v.fallback = self._last_good
                    v.fallback_why = ("the coach's instructions could not be used "
                                      f"({self._first_problem(cur)}); its last good restrictions stay")
                else:
                    prev = self._read_checked(self.paths.policy_prev, self._prev_cache, v.env, v.env_sha)
                    codes = {c for c, _ in prev["problems"]}
                    if prev["obj"] is not None and not (codes & {"V1", "V2", "V3", "V4", "V5", "V13"}):
                        try:
                            v.fallback = policy_mod.to_policy(prev["obj"])
                            v.fallback_why = ("the coach's instructions could not be used "
                                              f"({self._first_problem(cur)}); the previous ones' restrictions stay")
                        except Exception:
                            v.fallback = None
                    if v.fallback is None:
                        v.fallback_why = ("no coach instructions yet" if cur["error"] == "missing"
                                          else f"the coach's instructions could not be used "
                                               f"({self._first_problem(cur)})")
            self.last_view = v
            return v

    @staticmethod
    def _first_problem(cache: dict) -> str:
        if cache.get("problems"):
            code, why = cache["problems"][0]
            return f"{code}: {why}"
        return str(cache.get("error") or "unknown")

    # ------------------------------------------------------------- helpers --
    def _active_pins(self, v: _View):
        pol = v.policy or v.fallback
        return policy_mod.active_pins(v.env, pol.judged if pol is not None else None) if v.env else ()

    @staticmethod
    def _lookup(entries: dict, key: Key, now: dt.datetime) -> tuple[Optional[Entry], str]:
        for k in key.lookup_chain():
            e = entries.get(str(k))
            if e is None:
                continue
            until = parse_utc(e.until_utc) if e.until_utc else None
            if until is not None and now >= until:
                continue                                          # expired: look further down the chain
            return e, str(k)
        return None, ""

    def _runner_rule(self) -> tuple[bool, tuple, tuple]:
        rc = getattr(self.cfg, "runner", None)
        live = bool(rc is not None and getattr(rc, "enabled", True)
                    and str(getattr(rc, "mode", "") or "").strip().upper() == "LIVE")
        skip = tuple(_approach(t) for t in (getattr(rc, "skip_tactics", ()) or ())) if rc is not None else ()
        markets = tuple(str(m).upper() for m in (getattr(rc, "markets", ()) or ())) if rc is not None else ()
        return live, skip, markets

    def _static_live(self, name: str) -> bool:
        """Is this bot LIVE on Aaron's own settings (config.json as this process read it)?"""
        if name == BOT_TNB:
            return True
        block = getattr(self.cfg, name, None)
        if block is None:
            return False
        if not getattr(block, "enabled", True):
            return False
        return str(getattr(block, "mode", "") or "").strip().upper() in _LIVE_WORDS

    # ------------------------------------------------------------ the demo --
    def _weather(self) -> Optional[dict]:
        with self._lock:
            return self._weather_locked()

    def _weather_locked(self) -> Optional[dict]:
        sig = _sig(self.paths.weather)
        if sig is None:
            return None
        if sig != self._weather_cache[0]:
            try:
                with open(self.paths.weather, "rb") as fh:
                    raw = fh.read(2_000_001)
                obj = policy_mod.loads(raw) if len(raw) <= 2_000_000 else None
            except Exception:
                obj = None
            self._weather_cache = (sig, obj if isinstance(obj, dict) else None)
        return self._weather_cache[1]

    def demo_test(self, now: Optional[dt.datetime] = None, pol: Optional[Policy] = None) -> tuple[bool, str]:
        """Section 21's fail-safe test: (True, "") only while BOTH config.json's mode is DEMO and the broker
        reports a demo account (the trader's account check, written into weather.json no more than 180 s
        ago). Missing, stale or contradictory: (False, why) - the real-money values apply."""
        now = as_utc(now or self.clock())
        cfg = self.cfg
        mode = str(getattr(cfg, "mode", "") or "").strip().upper()
        if mode != "DEMO" or bool(getattr(cfg, "is_live", False)):
            return False, "config.json is not on DEMO"
        w = self._weather()
        if w is None:
            return False, "the account check (weather.json) is missing"
        when = parse_utc(w.get("as_of_utc"))
        if when is None:
            return False, "the account check has no time"
        age = (now - when).total_seconds()
        if age > DEMO_WEATHER_MAX_AGE_S:
            return False, f"the account check is {age:.0f} s old"
        if age < -FUTURE_SKEW_SECONDS:
            return False, "the account check is dated in the future"
        if w.get("account_demo") is not True:
            return False, "the broker does not report a demo account"
        if w.get("account_is_demo") is not True:
            return False, "the broker's trade mode is not demo"
        if "demo" not in str(w.get("account_server") or "").lower():
            return False, "the broker's server name does not say Demo"
        if str(w.get("config_mode") or "").strip().upper() != "DEMO":
            return False, "the trader's settings did not say DEMO"
        login = w.get("account_login")
        known = int(getattr(cfg, "account_login", 0) or 0) or self.account_login
        try:
            login = int(login or 0)
        except (TypeError, ValueError):
            return False, "the account check names no account"
        if known and login and login != known:
            return False, f"the account check is for account {login}, not {known}"
        if pol is not None and pol.account_login and login and pol.account_login != login:
            return False, "the coach's instructions were written for another account"
        return True, ""

    def effective_limits(self, now: Optional[dt.datetime] = None) -> Limits:
        """The account-level limits the trader reads (its daily-loss check). config.json's own values unless
        a valid PILOT lease carries demo values AND the demo test passes; each demo value only ever lifts."""
        r = self._risk_cfg()
        base = Limits.of(r)
        try:
            now = as_utc(now or self.clock())
            if self.halted():
                return Limits.of(r, "HALT is on: your own limits")
            v = self._view(now)
            if v.off_why or v.env is None or not v.env.governs(self.bot):
                return base
            if v.policy is None or v.mode != MODE_PILOT or v.policy.demo is None:
                return base
            ok, why = self.demo_test(now, v.policy)
            if not ok:
                return Limits.of(r, f"Real-money limits in force: {why}")
            d, env = v.policy.demo, v.env

            def lift(name: str, bound: float) -> float:
                mine = float(getattr(r, name))
                want = getattr(d, name)
                if want is None or not math.isfinite(float(want)):
                    return mine
                return max(mine, min(float(want), bound))

            out = Limits(
                max_daily_loss_pct=lift("max_daily_loss_pct", min(env.demo_max_daily_stop_pct,
                                                                  DEMO_MAX_DAILY_STOP_PCT)),
                max_risk_pct=lift("max_risk_pct", min(env.demo_max_step_pct, DEMO_MAX_STEP_PCT)),
                max_drawdown_pct=lift("max_drawdown_pct", DEMO_DRAWDOWN_BREAKER_PCT),
                max_correlated_risk_pct=lift("max_correlated_risk_pct", DEMO_TOTAL_MAX_PCT),
                max_total_risk_pct=lift("max_total_risk_pct", DEMO_TOTAL_MAX_PCT),
                demo=True, why="Demo mode: limits are lifted step by step as the bots earn it (your instruction "
                               "of 12 Oct). Every trade still has a stop.")
            if not out.lifted(r):
                return Limits.of(r, out.why)
            return out
        except Exception as exc:                                    # never raises: your own limits
            self.errors += 1
            log.error("the coach's gate could not work out the limits (%s): your own settings", exc)
            return base

    # ------------------------------------------------------------- decide --
    def decide(self, approach: str = WILD, kind: str = WILD, condition: str = WILD, symbol: str = "",
               side: Any = None, score: Optional[float] = None, rr: Optional[float] = None,
               now: Optional[dt.datetime] = None, static_real: bool = False,
               risk_ctx: Optional[RiskCtx] = None, *, bot: Optional[str] = None) -> Decision:
        """The route for one entry. Never raises (an error gives BASELINE and a log line)."""
        try:
            return self._decide(approach, kind, condition, symbol, side, score, rr, now, static_real,
                                risk_ctx or RiskCtx(), bot or self.bot)
        except Exception as exc:
            self.errors += 1
            log.error("the coach's gate failed for %s %s (%s): your own settings", bot or self.bot, symbol, exc)
            d = Decision(ROUTE_BASELINE, why=f"The coach's gate failed ({type(exc).__name__}): your own settings.",
                         source=SOURCE_ERROR)
            self._log(d, bot or self.bot, symbol, side, static_real, now)
            return d

    def _decide(self, approach, kind, condition, symbol, side, score, rr, now, static_real: bool,
                ctx: RiskCtx, bot: str) -> Decision:
        now = as_utc(now or self.clock())
        approach = _approach(approach) or WILD
        key = Key.make(bot, approach, kind or WILD, condition or WILD)
        static_real = bool(static_real)
        # 1 - HALT
        if self.halted():
            if static_real:
                d = Decision(self._restrict_route(ctx), key=str(key), source=SOURCE_HALT, rule="HALT",
                             why="HALT is on (Aaron's switch): no new real-money entries; practice carries on.")
            else:
                d = Decision(ROUTE_BASELINE, key=str(key), source=SOURCE_HALT, rule="HALT",
                             why="HALT is on: practice is unaffected.")
            self._log(d, bot, symbol, side, static_real, now)
            return d
        # 2 - COACH-OFF, the envelope, governed
        v = self._view(now)
        if v.off_why or v.env is None or not v.env.governs(bot):
            why = v.off_why or f"{BOT_LABEL.get(bot, bot)} is not governed by the envelope"
            d = Decision(ROUTE_BASELINE, key=str(key), source=SOURCE_OFF,
                         why=f"The coach is off for this ({why}): your own settings.")
            self._log(d, bot, symbol, side, static_real, now)
            return d
        pol_for_seq = v.policy or v.fallback
        seq = int(pol_for_seq.seq) if pol_for_seq is not None else 0
        # Aaron's pins: today's path exactly, whatever the policy says
        pin = policy_mod.pinned(key, self._active_pins(v))
        if pin is not None:
            source = SOURCE_WATCH if v.mode == MODE_WATCH else (SOURCE_POLICY if v.policy else SOURCE_FALLBACK)
            d = Decision(ROUTE_PIN, key=str(key), source=source, seq=seq, rule="PIN",
                         why=f"Pinned by Aaron ({pin.why or pin.key}): the coach leaves it alone.")
            self._log(d, bot, symbol, side, static_real, now)
            return d
        # 4 - WATCH: baseline, the would-be decision logged
        if v.mode == MODE_WATCH:
            would = None
            if v.policy is not None:
                would = self._from_policy(v, v.policy, key, symbol, side, score, rr, now, static_real, ctx,
                                          full=True, guard=False)
            elif v.fallback is not None:
                would = self._from_policy(v, v.fallback, key, symbol, side, score, rr, now, static_real, ctx,
                                          full=False, guard=False)
            d = Decision(ROUTE_BASELINE, key=str(key), source=SOURCE_WATCH, seq=seq,
                         why=f"Watching only ({v.mode_why or 'WATCH'}): it changes nothing today.")
            self._log(d, bot, symbol, side, static_real, now, would=would)
            return d
        # 5 - a valid policy
        if v.policy is not None:
            d = self._from_policy(v, v.policy, key, symbol, side, score, rr, now, static_real, ctx,
                                  full=True, guard=(v.mode == MODE_GUARD))
        # 6 - the last good restrictions
        elif v.fallback is not None:
            d = self._from_policy(v, v.fallback, key, symbol, side, score, rr, now, static_real, ctx,
                                  full=False, guard=True)
            if d.route == ROUTE_BASELINE and not d.money_gbp:
                d = Decision(ROUTE_BASELINE, key=d.key, source=SOURCE_FALLBACK, seq=d.seq,
                             why=f"{v.fallback_why[:1].upper()}{v.fallback_why[1:]}: your own settings.")
        # 7 - first install
        else:
            d = Decision(ROUTE_BASELINE, key=str(key), source=SOURCE_FALLBACK,
                         why=f"{v.fallback_why[:1].upper()}{v.fallback_why[1:]}: your own settings.")
        self._log(d, bot, symbol, side, static_real, now)
        return d

    def _restrict_route(self, ctx: RiskCtx) -> str:
        """No real money: practice when the bot has a paper path (the coach never stops practice), else
        no new entry."""
        return ROUTE_PRACTICE if ctx.has_paper else ROUTE_SKIP

    def _headroom(self, pol: Policy, ctx: RiskCtx) -> Optional[float]:
        """B5: the policy's headroom less this bot's own real risk opened after it was written, and the bot's
        own live headroom; the smaller. None when neither is known."""
        h: Optional[float] = None
        if pol.headroom_gbp is not None and math.isfinite(float(pol.headroom_gbp)):
            written = parse_utc(pol.written_utc)
            after = 0.0
            for opened, risk in ctx.own_open or ():
                try:
                    if opened is not None and written is not None and as_utc(opened) > written:
                        after += max(0.0, float(risk or 0.0))
                except Exception:
                    continue
            h = max(0.0, float(pol.headroom_gbp) - after)
        live = ctx.live_headroom_gbp
        if live is not None and math.isfinite(float(live)):
            h = max(0.0, float(live)) if h is None else min(h, max(0.0, float(live)))
        return h

    def _one_idea(self, ctx: RiskCtx, bot: str, symbol: str, side: Any, now: dt.datetime) -> str:
        """B6: another bot holds a real position on this symbol the other way, or opened one this way in the
        last 5 minutes ("" when neither)."""
        if ctx.others is None or not symbol:
            return ""
        mine = _side_text(side)
        own_magic = BOT_MAGIC.get(bot)
        for p in ctx.others() or ():
            if str(getattr(p, "symbol", "")).upper() != symbol.upper():
                continue
            try:
                if own_magic is not None and int(getattr(p, "magic", 0) or 0) == own_magic:
                    continue
            except (TypeError, ValueError):
                pass
            theirs = _side_text(getattr(p, "side", ""))
            if mine and theirs and theirs != mine:
                return f"another bot holds a real {theirs.lower()} position on {symbol}"
            opened = getattr(p, "open_time", None)
            if isinstance(opened, dt.datetime):
                if (now - as_utc(opened)).total_seconds() <= ONE_IDEA_SECONDS:
                    return f"another bot opened a real position on {symbol} in the last 5 minutes"
        return ""

    def _market_hours(self, gates, kind: str, now: dt.datetime) -> str:
        """The two market-hours gates for a coach grant ("" when neither applies)."""
        if gates is None:
            return ""
        ny = now.astimezone(TZ_NY)
        hm = ny.hour * 60 + ny.minute
        try:
            a, b = gates.rollover_ny
            lo = int(a[:2]) * 60 + int(a[3:5])
            hi = int(b[:2]) * 60 + int(b[3:5])
            inside = lo <= hm < hi if lo <= hi else (hm >= lo or hm < hi)
            if inside:
                return "no new granted entry in the New York rollover"
            cut = int(gates.friday_cutoff_ny[:2]) * 60 + int(gates.friday_cutoff_ny[3:5])
        except Exception:
            return ""
        if ny.weekday() == 4 and hm >= cut and (kind.startswith("FX") or kind == "INDEX"):
            return "no new granted FX or index entry after 16:00 New York on a Friday"
        return ""

    def _from_policy(self, v: _View, pol: Policy, key: Key, symbol: str, side: Any, score, rr,
                     now: dt.datetime, static_real: bool, ctx: RiskCtx, *, full: bool, guard: bool) -> Decision:
        env = v.env
        r = self._risk_cfg()
        source = SOURCE_POLICY if full else SOURCE_FALLBACK
        entry, ekey = self._lookup(pol.entries, key, now)
        route, rule, state, cap = ROUTE_BASELINE, "", "", None
        notes: list[str] = []
        if entry is not None:
            route, rule, state = entry.route, entry.rule or "", entry.state
            if route == ROUTE_PIN:
                route = ROUTE_BASELINE
            if not full and route == ROUTE_REAL:
                route = ROUTE_BASELINE
                notes.append("the coach's grant has lapsed")
            if route == ROUTE_REAL:
                live, skip, markets = self._runner_rule()
                forbidden = policy_mod.real_forbidden(
                    Key.parse(ekey) if ekey else key, env, self._active_pins(v), runner_live=live, runner_skip=skip,
                    runner_markets=markets, symbol=symbol)
                if not pol.grants_allowed:
                    route = ROUTE_BASELINE
                    notes.append("grants are paused (the coach's data checks)")
                elif key.condition == CONDITION_UNCLEAR:
                    route = ROUTE_BASELINE
                    notes.append("the market is UNCLEAR: never granted")
                elif guard:
                    route = ROUTE_BASELINE
                    notes.append("the coach may only restrict (GUARD)")
                elif forbidden:
                    route = ROUTE_BASELINE
                    notes.append(forbidden)
            if (route in (ROUTE_BASELINE,) and entry.money_gbp is not None and entry.route != ROUTE_REAL
                    and key.bot != BOT_TNB):
                # a cap that only lowers, for the bots that apply one (the Runner, Ian). Trend & Breakout
                # sizes its own settings' orders exactly as before, so a cap there is never claimed, and B5
                # below weighs the size it will really send.
                cap = max(0.0, float(entry.money_gbp))
        why_restrict = ""
        restrict_rule = ""
        if route in _RESTRICT and entry is not None:
            why_restrict = entry.why or f"the coach's instruction for {ekey}"
            restrict_rule = rule
        would_real = route == ROUTE_REAL or (route == ROUTE_BASELINE and static_real)
        if would_real:
            fl = None
            for k in key.lookup_chain():
                fl = pol.floors.get(str(k))
                if fl is not None:
                    break
            if fl is not None:
                low_score = fl.min_score is not None and score is not None and float(score) < fl.min_score
                low_rr = fl.min_rr is not None and rr is not None and float(rr) < fl.min_rr
                if low_score or low_rr:
                    route, restrict_rule = self._restrict_route(ctx), "floor"
                    why_restrict = (f"below the floor for this key (score {score} under {fl.min_score})" if low_score
                                    else f"below the floor for this key (reward:risk {rr} under {fl.min_rr})")
        if route == ROUTE_REAL or (route == ROUTE_BASELINE and static_real):
            for b in pol.blocks:
                until = parse_utc(b.until_utc) if b.until_utc else None
                if until is not None and now >= until:
                    continue
                if key.bot in b.bots and symbol.upper() in b.symbols:
                    route = self._restrict_route(ctx)
                    restrict_rule = b.rule or "block"
                    why_restrict = b.why or f"{symbol} is practice-only for the coach's keys ({restrict_rule})"
                    break
        if route == ROUTE_REAL and full:
            gate = self._market_hours(pol.gates, key.kind, now)
            if gate:
                route = ROUTE_BASELINE
                notes.append(gate)
        demo = False
        exposure = None
        ceiling_pct = None
        money: Optional[float] = None
        risk_pct: Optional[float] = None
        over_trials = False
        if route == ROUTE_REAL:
            eq = float(ctx.equity or 0.0)
            if not (math.isfinite(eq) and eq > 0):
                route = ROUTE_BASELINE
                notes.append("the account's equity is not known: no grant")
            else:
                demo_ok = False
                if pol.demo is not None and full and v.mode == MODE_PILOT:
                    demo_ok, _ = self.demo_test(now, pol)
                v1_ceiling = float(r.max_risk_pct)
                ceiling = v1_ceiling
                env_cap = float(env.max_step_pct)
                if demo_ok and pol.demo is not None and pol.demo.max_risk_pct is not None:
                    lifted = min(float(pol.demo.max_risk_pct), float(env.demo_max_step_pct), DEMO_MAX_STEP_PCT)
                    if lifted > ceiling:
                        ceiling = lifted
                        env_cap = max(env_cap, lifted)
                        demo = True
                cap_pct = min(ceiling, env_cap)
                risk_pct = min(float(entry.risk_pct or 0.0), cap_pct)
                m = min(float(entry.money_gbp or 0.0), float(entry.risk_pct or 0.0) * eq / 100.0,
                        ceiling * eq / 100.0, env_cap * eq / 100.0)
                if entry.state == STATE_LIVE_TRIAL and not demo_ok:
                    m = min(m, float(env.trial_max_gbp), float(env.trial_risk_pct) * eq / 100.0)
                    # the v1 number of trials too (section 21: every lifted limit reverts when the demo test
                    # fails): only the first env.max_trials real trials, in key order, keep their grant
                    trials = sorted(k for k, e in pol.entries.items()
                                    if e.route == ROUTE_REAL and e.state == STATE_LIVE_TRIAL)
                    if ekey not in trials[:max(0, int(env.max_trials))]:
                        m, over_trials = 0.0, True
                        notes.append(f"more trials than the real-money limit of {int(env.max_trials)}")
                h = self._headroom(pol, ctx)
                if h is not None:
                    m = min(m, h)
                money = _money(max(0.0, m))
                if demo:
                    ceiling_pct = max(v1_ceiling, risk_pct)
                    d = pol.demo
                    corr = max(float(r.max_correlated_risk_pct),
                               min(float(d.max_correlated_risk_pct or 0.0), DEMO_TOTAL_MAX_PCT))
                    tot = max(float(r.max_total_risk_pct), min(float(d.max_total_risk_pct or 0.0), DEMO_TOTAL_MAX_PCT))
                    if corr > float(r.max_correlated_risk_pct) or tot > float(r.max_total_risk_pct):
                        exposure = (corr, tot)
                if money <= 0 and over_trials:
                    route, money, risk_pct = ROUTE_BASELINE, None, None
                    demo, exposure, ceiling_pct = False, None, None
                elif money <= 0:
                    route, restrict_rule = self._restrict_route(ctx), "B5"
                    why_restrict = "no headroom left under today's loss limit"
                    money = None
                    risk_pct = None
        # B5 for an entry that is real on Aaron's own settings (governed, not pinned)
        if full and route == ROUTE_BASELINE and static_real:
            h = self._headroom(pol, ctx)
            need = ctx.planned_gbp
            if cap is not None and need is not None:
                need = min(need, cap)
            if h is not None and need is not None and float(need) > h + 0.005:
                route, restrict_rule = self._restrict_route(ctx), "B5"
                why_restrict = f"it would risk {_gbp(need)} with {_gbp(h)} of headroom left today"
        # B6: one idea, one real ticket
        if full and (route == ROUTE_REAL or (route == ROUTE_BASELINE and static_real)):
            why6 = self._one_idea(ctx, key.bot, symbol, side, now)
            if why6:
                route, restrict_rule, why_restrict = self._restrict_route(ctx), "B6", why6
                money = risk_pct = None
                demo, exposure, ceiling_pct = False, None, None
        seq = int(pol.seq)
        if route == ROUTE_SKIP and ctx.has_paper:
            route = ROUTE_PRACTICE           # the coach never stops practice: with a paper path, SKIP is practice
        if route in _RESTRICT:
            where = "practice" if route == ROUTE_PRACTICE else "no new entry"
            return Decision(route, key=ekey or str(key), source=source, seq=seq, rule=restrict_rule or rule,
                            state=state, why=f"The coach: {where} - {why_restrict or 'its instruction'}.")
        if route == ROUTE_REAL:
            words = (f"The coach's grant: real money at {_gbp(money)} ({(state or 'live').replace('_', ' ').lower()}"
                     f"{', rule ' + rule if rule else ''}, instruction {seq})"
                     + (" - demo limits in use" if demo else "") + ".")
            return Decision(ROUTE_REAL, risk_pct=risk_pct, money_gbp=money, key=ekey, source=source, seq=seq,
                            rule=rule, state=state, ceiling_pct=ceiling_pct, exposure=exposure, demo=demo, why=words)
        extra = f" ({'; '.join(notes)})" if notes else ""
        if cap is not None:
            return Decision(ROUTE_BASELINE, money_gbp=cap, key=ekey or str(key), source=source, seq=seq, rule=rule,
                            state=state, why=f"Your own settings, at most {_gbp(cap)} a trade (the coach's "
                                             f"cap{', rule ' + rule if rule else ''}){extra}.")
        return Decision(ROUTE_BASELINE, key=ekey or str(key), source=source, seq=seq, rule=rule, state=state,
                        why=f"Your own settings{extra}.")

    # ----------------------------------------------------------- other bots --
    def bot_may_enter(self, name: str, now: Optional[dt.datetime] = None, static_live: Optional[bool] = None) -> bool:
        """May this bot (running inside the trader) open a new entry? Always True when its own mode is not
        LIVE: practice is never gated. LIVE: False under HALT, or when the coach's instruction for the whole
        bot (``name|*|*|*``) is PRACTICE or SKIP (a valid policy, or the last good restrictions)."""
        try:
            live = self._static_live(name) if static_live is None else bool(static_live)
            if not live:
                return True
            if self.halted():
                return False
            now = as_utc(now or self.clock())
            v = self._view(now)
            if v.off_why or v.env is None or not v.env.governs(name) or v.mode == MODE_WATCH:
                return True
            pol = v.policy or v.fallback
            if pol is None:
                return True
            e = pol.entries.get(str(Key(name, WILD, WILD, WILD)))
            if e is None:
                return True
            until = parse_utc(e.until_utc) if e.until_utc else None
            if until is not None and now >= until:
                return True
            return e.route not in _RESTRICT
        except Exception as exc:
            self.errors += 1
            log.error("the coach's gate failed for %s (%s): your own settings", name, exc)
            return True

    def confirm_order(self, req: Any, kind: Optional[str] = None) -> bool:
        """PaperBroker.coach_confirm: an independent re-check of a coach-REAL order against the envelope
        alone (key governed, kind allowed, not pinned, not C2, not the live Runner's idea), with a valid
        PILOT lease. False on any doubt: the order then stays on paper."""
        try:
            if self.halted():
                return False
            now = as_utc(self.clock())
            v = self._view(now)
            if v.off_why or v.env is None or v.policy is None or v.mode != MODE_PILOT or not v.policy.grants_allowed:
                return False
            approach = _approach(getattr(req, "tactic", ""))
            if not approach:
                return False
            symbol = str(getattr(req, "symbol", "") or "")
            if not kind:
                from ..contracts import infer_group
                kind = infer_group(symbol)
            key = Key(BOT_TNB, approach, str(kind).upper(), WILD)
            live, skip, markets = self._runner_rule()
            why = policy_mod.real_forbidden(key, v.env, self._active_pins(v), runner_live=live, runner_skip=skip,
                                            runner_markets=markets, symbol=symbol)
            if why:
                log.warning("the coach's real order on %s was not confirmed (%s): it stays on paper", symbol, why)
            return not why
        except Exception as exc:
            self.errors += 1
            log.error("the coach's order check failed (%s): the order stays on paper", exc)
            return False

    # ------------------------------------------------------------------ log --
    def _log(self, d: Decision, bot: str, symbol: str, side: Any, static_real: bool,
             now: Optional[dt.datetime], would: Optional[Decision] = None) -> None:
        """Step 10: every decision for a real or would-be-real entry, one JSON line."""
        if not self._logging():
            return
        try:
            real = static_real or d.route == ROUTE_REAL or (would is not None and would.route == ROUTE_REAL)
            if not real:
                return
            ts = as_utc(now or self.clock())
            same = (bot, d.key, symbol, _side_text(side), d.route, d.money_gbp, d.source, d.seq, bool(static_real),
                    would.route if would is not None else None, would.money_gbp if would is not None else None)
            with self._lock:
                last = self._logged.get(same)
                if last is not None and 0 <= (ts - last).total_seconds() < 60.0:
                    return
                if len(self._logged) > 1000:
                    self._logged.clear()
                self._logged[same] = ts
            row = {"ts": ts.isoformat(), "bot": bot, "key": d.key, "symbol": symbol, "side": _side_text(side),
                   "route": d.route, "money": d.money_gbp, "seq": d.seq, "source": d.source, "why": d.why,
                   "static_real": bool(static_real)}
            if would is not None:
                row["would"] = {"route": would.route, "money": would.money_gbp, "key": would.key, "why": would.why}
            self._append(bot, ts, row)
        except Exception as exc:
            log.warning("the coach's gate log could not be written: %s", exc)

    def _logging(self) -> bool:
        """Log only once the coach is installed (Aaron's envelope exists): before that the bots' data folder
        stays exactly as it was without the coach."""
        return self.log_decisions and os.path.exists(self.paths.envelope)

    def _append(self, bot: str, ts: dt.datetime, row: dict) -> None:
        path = self.paths.gate_log(bot, uk_day(ts))
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, sort_keys=True, default=str, allow_nan=False)
        with self._lock, open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def note_fill(self, ticket: int, decision: Decision, *, bot: Optional[str] = None, symbol: str = "",
                  real: bool = True, now: Optional[dt.datetime] = None, static_real: Optional[bool] = None,
                  equity: Optional[float] = None) -> None:
        """After a fill: the ticket joined to its key, route and money in the gate log (never raises).
        ``static_real``: would the entry have been real on Aaron's own settings (the scorecard counts a
        practice trade as money saved only when it says so)."""
        if not self._logging():
            return
        try:
            ts = as_utc(now or self.clock())
            row = {"ts": ts.isoformat(), "event": "fill", "ticket": int(ticket), "bot": bot or self.bot,
                   "symbol": symbol, "key": decision.key, "route": decision.route, "money": decision.money_gbp,
                   "seq": decision.seq, "source": decision.source, "real": bool(real), "why": decision.why}
            if static_real is not None:
                row["static_real"] = bool(static_real)
            if equity is not None and math.isfinite(float(equity)) and float(equity) > 0:
                row["equity"] = round(float(equity), 2)          # section 21's shadow figure sizes v1 at this
            self._append(bot or self.bot, ts, row)
        except Exception as exc:
            log.warning("the coach's gate log could not be written: %s", exc)


    def note_refused(self, decision: Decision, message: str, *, bot: Optional[str] = None, symbol: str = "",
                     now: Optional[dt.datetime] = None) -> None:
        """A real order the PAPER wrapper refused at its ceiling check (spec 10.4): in the gate's log, so the
        coach sees it (its audit puts it in GUARD). Never raises."""
        try:
            ts = as_utc(now or self.clock())
            row = {"ts": ts.isoformat(), "event": "refused_at_ceiling", "bot": bot or self.bot, "symbol": symbol,
                   "key": decision.key, "route": decision.route, "money": decision.money_gbp, "seq": decision.seq,
                   "source": decision.source, "why": str(message)[:300]}
            self._append(bot or self.bot, ts, row)
        except Exception as exc:
            log.warning("the coach's gate log could not be written: %s", exc)


class BaselineGate:
    """The gate when the real one cannot be built (or in a dry run): Aaron's own settings, always."""
    bot = BOT_TNB

    def __init__(self, cfg: Any = None, bot: str = BOT_TNB, halt_path: Optional[Path] = None):
        self.cfg = cfg
        self.bot = bot
        self.halt_path = halt_path
        self.paths = Paths.of(Path(halt_path).parent) if halt_path is not None else None

    def halted(self) -> bool:
        return bool(self.halt_path is not None and os.path.exists(self.halt_path))

    def decide(self, approach: str = WILD, kind: str = WILD, condition: str = WILD, symbol: str = "",
               side: Any = None, score: Optional[float] = None, rr: Optional[float] = None,
               now: Optional[dt.datetime] = None, static_real: bool = False,
               risk_ctx: Optional[RiskCtx] = None, *, bot: Optional[str] = None) -> Decision:
        if static_real and self.halted():
            has_paper = True if risk_ctx is None else bool(risk_ctx.has_paper)
            return Decision(ROUTE_PRACTICE if has_paper else ROUTE_SKIP, source=SOURCE_HALT, rule="HALT",
                            why="HALT is on (Aaron's switch): no new real-money entries; practice carries on.")
        return Decision(ROUTE_BASELINE, source=SOURCE_OFF, why="")

    def bot_may_enter(self, name: str, now=None, static_live=None) -> bool:
        if self.halted():
            block = getattr(self.cfg, name, None) if self.cfg is not None else None
            live = static_live if static_live is not None else (
                block is not None and str(getattr(block, "mode", "") or "").strip().upper() == "LIVE")
            return not live
        return True

    def effective_limits(self, now=None) -> Optional[Limits]:
        return Limits.of(self.cfg.risk) if self.cfg is not None else None

    def confirm_order(self, req: Any, kind: Optional[str] = None) -> bool:
        return False

    def note_fill(self, *args, **kwargs) -> None:
        return None

    def note_refused(self, *args, **kwargs) -> None:
        return None


def build(bot: str, data_dir: "str | Path", cfg: Any, **kw) -> "CoachGate | BaselineGate":
    """A CoachGate, or a BaselineGate (Aaron's settings, HALT still honoured) if it cannot be built."""
    try:
        return CoachGate(bot, data_dir, cfg, **kw)
    except Exception as exc:
        log.error("the coach's gate could not be built (%s): your own settings", exc)
        return BaselineGate(cfg, bot, Paths.of(data_dir).halt)
