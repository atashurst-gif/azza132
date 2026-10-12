"""Aaron's envelope, ``data/coach.json``: reading and checking it (spec section 10.1).

Written only by the installer's one-time step and by Aaron's own commands
(``python -m mintel.coach off | on | watch | guard | pilot | pin ...``). The
coach process and the gate only READ it; nothing here writes it.

The envelope is invalid if it has an unknown key; ``max_step_pct`` is above
``cfg.risk.max_risk_pct``; ``day_pause_pct`` is at or above
``cfg.risk.max_daily_loss_pct``; ``day_line_pct`` is at or above
``day_pause_pct``; ``rider`` or ``lab`` is in ``govern``; or any number is
negative or not finite. Also refused: a wrong schema or mode, a bot or kind of
market this code does not know, a malformed pin, and (section 21) a demo cap
above the demo ladder's top (5% a trade, a 10% daily stop). **An invalid
envelope means OFF**: every bot on Aaron's own settings, with a red line on
the page.

WATCH day: before ``watch_until_utc``, PILOT behaves as WATCH, and PILOT
starts only at the next 06:30 UK release after it (the coach itself stays in
WATCH, through the policy's ``mode``, if its WATCH-day self-check failed).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from . import (DEMO_MAX_DAILY_STOP_PCT, DEMO_MAX_STEP_PCT, ENVELOPE_KEYS, ENVELOPE_SCHEMA, F1_JUDGE_AFTER, F1_KEY,
               GOVERNABLE_BOTS, KINDS,
               MODE_OFF, MODE_PILOT, MODE_RANK, MODE_WATCH, MODES, NEVER_GOVERNED, PIN_KEYS, Envelope, Key, Pin,
               as_utc, next_release_utc, parse_utc)
from .policy import canonical, loads

MAX_ENVELOPE_BYTES = 64 * 1024


class EnvelopeError(ValueError):
    pass


@dataclass(frozen=True)
class EnvelopeState:
    """What was read: the envelope (None when missing or invalid), its sha256 (of the canonical JSON of
    the file's object; "" when unreadable) and why it is not usable ("" when it is)."""
    envelope: Optional[Envelope]
    sha256: str
    error: str = ""
    exists: bool = True

    @property
    def ok(self) -> bool:
        return self.envelope is not None and not self.error

    @property
    def mode(self) -> str:
        return self.envelope.mode if self.ok else MODE_OFF


def envelope_sha(obj: Any) -> str:
    """sha256 of the envelope's canonical JSON (what a policy names in ``envelope_sha``)."""
    return hashlib.sha256(canonical(obj).encode("ascii")).hexdigest()


def _num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(float(x))


def parse(obj: Any, max_risk_pct: float, max_daily_loss_pct: float) -> Envelope:
    """The envelope from the file's object, checked against Aaron's config.json (raises EnvelopeError)."""
    if not isinstance(obj, dict):
        raise EnvelopeError("the envelope is not a JSON object")
    extra = sorted(set(obj) - set(ENVELOPE_KEYS))
    if extra:
        raise EnvelopeError(f"unknown key(s) {extra}")
    for need in ("schema", "mode"):
        if need not in obj:
            raise EnvelopeError(f"missing {need}")
    if obj["schema"] != ENVELOPE_SCHEMA or isinstance(obj["schema"], bool):
        raise EnvelopeError(f"schema {obj['schema']!r}, not {ENVELOPE_SCHEMA}")
    if obj["mode"] not in MODES:
        raise EnvelopeError(f"mode {obj['mode']!r} is not one of {MODES}")
    d = Envelope()
    kw: dict = {"schema": ENVELOPE_SCHEMA, "mode": obj["mode"]}
    w = obj.get("watch_until_utc", d.watch_until_utc)
    if w is None:
        w = ""
    if not isinstance(w, str) or (w.strip() and parse_utc(w) is None):
        raise EnvelopeError("watch_until_utc is not a time")
    kw["watch_until_utc"] = w
    govern = obj.get("govern", list(d.govern))
    if not isinstance(govern, list) or not all(isinstance(b, str) for b in govern):
        raise EnvelopeError("govern is not a list of bots")
    for b in govern:
        if b in NEVER_GOVERNED:
            raise EnvelopeError(f"{b} may never be governed by the coach")
        if b not in GOVERNABLE_BOTS:
            raise EnvelopeError(f"{b!r} is not a bot the coach knows")
    kw["govern"] = tuple(dict.fromkeys(govern))
    kinds = obj.get("tnb_real_kinds", list(d.tnb_real_kinds))
    if not isinstance(kinds, list) or not all(isinstance(k, str) and k in KINDS for k in kinds):
        raise EnvelopeError("tnb_real_kinds is not a list of kinds of market")
    kw["tnb_real_kinds"] = tuple(dict.fromkeys(kinds))
    for name in ("trial_risk_pct", "trial_max_gbp", "trial_loss_budget_gbp_week", "max_step_pct", "day_line_pct",
                 "day_pause_pct", "week_line_pct", "drawdown_line_pct", "drawdown_pause_pct", "demo_max_step_pct",
                 "demo_max_daily_stop_pct"):
        v = obj.get(name, getattr(d, name))
        if not _num(v) or float(v) < 0:
            raise EnvelopeError(f"{name} {v!r} is not a finite number of zero or more")
        kw[name] = float(v)
    mt = obj.get("max_trials", d.max_trials)
    if not isinstance(mt, int) or isinstance(mt, bool) or mt < 0:
        raise EnvelopeError(f"max_trials {mt!r} is not a whole number of zero or more")
    kw["max_trials"] = mt
    ann = obj.get("announce_steps", d.announce_steps)
    if not isinstance(ann, bool):
        raise EnvelopeError("announce_steps is not true or false")
    kw["announce_steps"] = ann
    for name in ("set_by", "set_utc"):
        v = obj.get(name, "")
        if v is None:
            v = ""
        if not isinstance(v, str):
            raise EnvelopeError(f"{name} is not text")
        kw[name] = v
    pins_raw = obj.get("pins", [p.to_dict() for p in d.pins])
    if not isinstance(pins_raw, list):
        raise EnvelopeError("pins is not a list")
    pins = []
    for p in pins_raw:
        if not isinstance(p, dict) or "key" not in p:
            raise EnvelopeError("a pin is not a table with a key")
        extra = sorted(set(p) - set(PIN_KEYS))
        if extra:
            raise EnvelopeError(f"unknown pin field(s) {extra}")
        try:
            Key.parse(p["key"])
        except ValueError as exc:
            raise EnvelopeError(f"pin: {exc}") from None
        why = p.get("why", "") or ""
        # Formula 1's pin (the spec's example names no judge_after) is released by its 20-live-trade judgement
        ja = p.get("judge_after", F1_JUDGE_AFTER if p["key"] == F1_KEY else 0) or 0
        since = p.get("set_utc", "") or ""
        if not isinstance(why, str) or not isinstance(since, str) or (since and parse_utc(since) is None):
            raise EnvelopeError(f"pin {p['key']}: why or set_utc is malformed")
        if not isinstance(ja, int) or isinstance(ja, bool) or ja < 0:
            raise EnvelopeError(f"pin {p['key']}: judge_after is not a whole number")
        pins.append(Pin(p["key"], why, ja, since))
    kw["pins"] = tuple(pins)
    env = Envelope(**kw)
    # Aaron's own settings bound the envelope (config.json is read only)
    if env.max_step_pct > float(max_risk_pct) + 1e-9:
        raise EnvelopeError(f"max_step_pct {env.max_step_pct:g} is above config.json's max_risk_pct "
                            f"{float(max_risk_pct):g}")
    if env.day_pause_pct >= float(max_daily_loss_pct) - 1e-12:
        raise EnvelopeError(f"day_pause_pct {env.day_pause_pct:g} is not below config.json's daily stop "
                            f"{float(max_daily_loss_pct):g}")
    if env.day_line_pct >= env.day_pause_pct - 1e-12:
        raise EnvelopeError(f"day_line_pct {env.day_line_pct:g} is not below day_pause_pct {env.day_pause_pct:g}")
    if env.trial_risk_pct > env.max_step_pct + 1e-9:
        raise EnvelopeError("trial_risk_pct is above max_step_pct")
    if env.demo_max_step_pct > DEMO_MAX_STEP_PCT + 1e-9:
        raise EnvelopeError(f"demo_max_step_pct {env.demo_max_step_pct:g} is above the demo ladder's "
                            f"{DEMO_MAX_STEP_PCT:g}%")
    if env.demo_max_daily_stop_pct > DEMO_MAX_DAILY_STOP_PCT + 1e-9:
        raise EnvelopeError(f"demo_max_daily_stop_pct {env.demo_max_daily_stop_pct:g} is above "
                            f"{DEMO_MAX_DAILY_STOP_PCT:g}%")
    return env


def read(path: "str | Path", max_risk_pct: float, max_daily_loss_pct: float) -> EnvelopeState:
    """Read ``data/coach.json``. Missing: OFF (``exists`` False). Anything wrong: OFF with the reason."""
    p = Path(path)
    try:
        with open(p, "rb") as fh:
            raw = fh.read(MAX_ENVELOPE_BYTES + 1)
    except FileNotFoundError:
        return EnvelopeState(None, "", "no envelope (data/coach.json): the coach is off", exists=False)
    except OSError as exc:
        return EnvelopeState(None, "", f"the envelope could not be read ({exc})")
    if len(raw) > MAX_ENVELOPE_BYTES:
        return EnvelopeState(None, "", "the envelope is too large")
    try:
        obj = loads(raw)
    except Exception as exc:
        return EnvelopeState(None, "", f"the envelope is not JSON ({exc})")
    try:
        sha = envelope_sha(obj)
    except Exception as exc:
        return EnvelopeState(None, "", f"the envelope is not canonical JSON ({exc})")
    try:
        env = parse(obj, max_risk_pct, max_daily_loss_pct)
    except EnvelopeError as exc:
        return EnvelopeState(None, sha, f"the envelope is invalid: {exc}")
    return EnvelopeState(env, sha, "")


def effective_mode(env: Optional[Envelope], now: dt.datetime) -> tuple[str, str]:
    """(mode, why) from the envelope and the clock: PILOT behaves as WATCH before ``watch_until_utc`` and
    until the next 06:30 UK release after it."""
    if env is None:
        return MODE_OFF, "no valid envelope"
    if env.mode != MODE_PILOT:
        return env.mode, ""
    until = parse_utc(env.watch_until_utc) if env.watch_until_utc else None
    if until is None:
        return MODE_PILOT, ""
    now = as_utc(now)
    if now < until:
        return MODE_WATCH, f"watching only until {until.isoformat()} (the WATCH day)"
    starts = next_release_utc(until)
    if now < starts:
        return MODE_WATCH, f"watching only: in charge from the 06:30 UK release ({starts.isoformat()})"
    return MODE_PILOT, ""


def stricter(a: str, b: str) -> str:
    """The mode that lets the coach do less."""
    ra, rb = MODE_RANK.get(a, 0), MODE_RANK.get(b, 0)
    return a if ra <= rb else b

