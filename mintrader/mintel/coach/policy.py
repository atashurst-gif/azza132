"""The policy file: canonical JSON, the one validator, atomic write and read.

``data/coach/policy.json`` is the coach's only instruction to the bots
(spec section 10.2). The writer (the coach's run loop) and the reader (the
gate inside each bot) use the SAME validator here, so a policy the writer
would not accept can never be acted on, and the other way round.

Canonical JSON::

    json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)

``sha256`` is the hash of the canonical JSON of the policy WITHOUT that
field. NaN and infinities are refused on the way in and on the way out.

The checks (section 10.3, step 3); any failure makes the whole file invalid,
there is no partial use:

* V1  JSON, no larger than 256 KB (duplicate keys and NaN refused too);
* V2  schema 1;
* V3  sha256 matches;
* V4  account_login matches (when both are known);
* V5  envelope_sha equals the current envelope's;
* V6  written no more than 120 s in the future and no more than valid_s old
      (valid_s itself no more than the 180-second lease);
* V7  seq not below the last one seen;
* V8  every route (and state and mode) in the allowed set;
* V9  every risk_pct and money_gbp finite, not negative, within the ceiling,
      the envelope's step cap and the equity (and the demo block within
      section 21's ladder);
* V10 every REAL key in a governed bot (Trend & Breakout in v1), an allowed
      kind, not pinned, not a minor-pair key while Formula 1 is pinned (C2),
      not a key the live Momentum Runner rides, never UNCLEAR;
* V11 trials within the envelope (count and size);
* V12 brakes and headroom finite and not negative;
* V13 no unknown key or field anywhere, and every field of the right type.

Stdlib only; imports nothing from the trading code.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from . import (ACCOUNT_KEYS, BLOCK_KEYS, BRAKE_DAY, BRAKE_DRAWDOWN, BRAKE_WEEK, BRAKES_KEYS,
               BROKER_CHECK_KEYS, CONDITION_UNCLEAR, DATA_KEYS, DEMO_DAILY_STOP_LADDER,
               DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT, DEMO_DRAWDOWN_BREAKER_PCT,
               DEMO_KEYS, DEMO_MAX_DAILY_STOP_PCT, DEMO_MAX_STEP_PCT, DEMO_MAX_TRIALS, DEMO_TOTAL_MAX_PCT,
               ENTRY_KEYS, FLOOR_KEYS, FUTURE_SKEW_SECONDS, GATES_KEYS, JUDGED_KEYS, JUDGEMENTS, KEY_BOTS,
               LEASE_SECONDS, MODES, POLICY_KEYS, POLICY_MAX_BYTES, POLICY_REQUIRED, REAL_ROUTE_BOTS,
               RESTRICTING_ROUTES, ROUTE_REAL, ROUTES, SCHEMA_VERSION, STATE_LIVE_TRIAL, STATES, STEP_ORDER, WILD,
               WHY_MAX_CHARS, Block, DemoLimits, Entry, Envelope, Floor, Gates, Judgement, Key, Paths, Pin, Policy,
               as_utc, parse_utc)

_HHMM = re.compile(r"^([01][0-9]|2[0-3]):[0-5][0-9]$")


class PolicyError(ValueError):
    """A policy that fails a check. ``code`` is "V1".."V13"."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


# ------------------------------------------------------------- canonical JSON --
def canonical(obj: Any) -> str:
    """The one serialisation the hash is taken over (raises ValueError on NaN or infinity)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def sha_of(obj: dict) -> str:
    """sha256 of the canonical JSON of ``obj`` without its "sha256" field."""
    body = {k: v for k, v in obj.items() if k != "sha256"}
    return hashlib.sha256(canonical(body).encode("ascii")).hexdigest()


def _no_constants(name: str):
    raise ValueError(f"{name} is not a number JSON allows")


def _no_duplicates(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate key {k!r}")
        out[k] = v
    return out


def loads(raw: "bytes | str") -> Any:
    """Strict JSON: no NaN or infinities, no duplicate keys (raises ValueError)."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw, parse_constant=_no_constants, object_pairs_hook=_no_duplicates)


def parse_bytes(raw: bytes) -> dict:
    """V1: the file's bytes as a JSON object (raises PolicyError V1)."""
    if raw is None:
        raise PolicyError("V1", "no policy")
    if len(raw) > POLICY_MAX_BYTES:
        raise PolicyError("V1", f"the policy is {len(raw)} bytes, over the {POLICY_MAX_BYTES} limit")
    try:
        obj = loads(raw)
    except Exception as exc:
        raise PolicyError("V1", f"not JSON ({exc})") from None
    if not isinstance(obj, dict):
        raise PolicyError("V1", "the policy is not a JSON object")
    return obj


# ------------------------------------------------------------- the context --
@dataclass(frozen=True)
class CheckContext:
    """What a policy is checked against: Aaron's envelope and settings, the account, the clock and the last
    sequence number seen. The same for the writer and every reader."""
    envelope: Envelope
    envelope_sha: str
    max_risk_pct: float                    # cfg.risk.max_risk_pct (config.json; read only)
    max_daily_loss_pct: float = 3.0        # cfg.risk.max_daily_loss_pct
    base_pct: float = 0.70                 # Trend & Breakout's base percent (mintel.engine.risk.base_risk)
    base_money: float = 13.0               # and its money cap a trade
    now: Optional[dt.datetime] = None      # None: the lease (V6) is not checked
    last_seq: int = -1
    account_login: int = 0                 # 0: unknown
    runner_live: bool = False              # the Momentum Runner is LIVE (placed)
    runner_skip: tuple[str, ...] = ()      # approaches it does not ride (runner.skip_tactics)
    runner_markets: tuple[str, ...] = ()   # runner.markets ("" = every index market)
    max_drawdown_pct: float = 12.0         # cfg.risk.max_drawdown_pct (the breaker; read only)

    @classmethod
    def from_config(cls, cfg: Any, envelope: Envelope, envelope_sha: str, *, now: Optional[dt.datetime] = None,
                    last_seq: int = -1, account_login: int = 0) -> "CheckContext":
        r = cfg.risk
        from ..engine.risk import base_risk               # the one reader of Trend & Breakout's base size
        base_pct, base_money = base_risk(r)
        rc = getattr(cfg, "runner", None)
        runner_live = bool(rc is not None and getattr(rc, "enabled", True)
                           and str(getattr(rc, "mode", "") or "").strip().upper() == "LIVE")
        skip = tuple(_approach(t) for t in (getattr(rc, "skip_tactics", ()) or ())) if rc is not None else ()
        markets = tuple(str(m).upper() for m in (getattr(rc, "markets", ()) or ())) if rc is not None else ()
        login = int(account_login or getattr(cfg, "account_login", 0) or 0)
        return cls(envelope, envelope_sha, float(r.max_risk_pct), float(r.max_daily_loss_pct),
                   float(base_pct), float(base_money), now, int(last_seq), login, runner_live, skip, markets,
                   float(getattr(r, "max_drawdown_pct", 12.0)))


def _approach(name: Any) -> str:
    """engine.tactics.approach_of, without importing the trading code: upper case, "_2X" twins stripped."""
    text = str(name or "").strip().upper()
    return text[:-3] if text.endswith("_2X") else text


# ------------------------------------------------------------- small checks --
def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _finite(x: Any) -> bool:
    return _is_num(x) and math.isfinite(float(x))


def _is_int(x: Any) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _unknown(obj: dict, allowed: tuple, where: str) -> Optional[str]:
    extra = sorted(set(obj) - set(allowed))
    return f"unknown field(s) {extra} in {where}" if extra else None


def _opt_text(x: Any) -> bool:
    return x is None or isinstance(x, str)


def _opt_time(x: Any) -> bool:
    return x is None or (isinstance(x, str) and parse_utc(x) is not None)


# ------------------------------------------------------------- pins --
def active_pins(envelope: Envelope, judged: Optional[dict] = None) -> tuple[Pin, ...]:
    """Aaron's pins still in force. A pin with ``judge_after`` is released only by a recorded judgement for
    its key made after the pin was set and after at least that many live trades."""
    out = []
    for pin in envelope.pins:
        j = (judged or {}).get(pin.key)
        if j is not None and pin.judge_after > 0:
            when = parse_utc(j.judged_utc) if isinstance(j, Judgement) else None
            since = parse_utc(pin.set_utc) if pin.set_utc else None
            ok = (isinstance(j, Judgement) and when is not None and j.live_trades >= pin.judge_after
                  and (since is None or when >= since))
            if ok:
                continue
        out.append(pin)
    return tuple(out)


def pinned(key: Key, pins: tuple[Pin, ...]) -> Optional[Pin]:
    """The pin that covers ``key`` (a pin's own key, or any key it includes), else None."""
    for pin in pins:
        try:
            pk = Key.parse(pin.key)
        except ValueError:
            continue
        if pk.covers(key) or key.covers(pk):
            return pin
    return None


def real_forbidden(key: Key, envelope: Envelope, pins: tuple[Pin, ...], *, runner_live: bool = False,
                   runner_skip: tuple[str, ...] = (), runner_markets: tuple[str, ...] = (),
                   symbol: str = "") -> str:
    """Why ``key`` may not be granted REAL ("" when it may): V10, used by the validator and by the gate."""
    if key.bot not in envelope.govern:
        return f"{key.bot} is not governed by the envelope"
    if key.bot not in REAL_ROUTE_BOTS:
        return f"{key.bot} has no real-money grant in v1 (its money can only go down)"
    if key.approach == WILD or key.kind == WILD:
        return "a grant must name its approach and kind of market"
    if key.kind not in envelope.tnb_real_kinds:
        return f"{key.kind} is not a kind of market the envelope allows for real money"
    if key.condition == CONDITION_UNCLEAR:
        return "UNCLEAR is never granted"
    pin = pinned(key, pins)
    if pin is not None:
        return f"pinned by Aaron ({pin.key})"
    for pin in pins:
        try:
            pk = Key.parse(pin.key)
        except ValueError:
            continue
        if pk.bot == key.bot and pk.kind not in (WILD, "") and pk.kind == key.kind:
            # C2: verdicts.formula1_live counts EVERY real Trend & Breakout position of that kind
            return f"{key.kind} keys stay on paper while {pin.key} is pinned (its live count would include them)"
    if runner_live:
        if runner_markets:
            rides = bool(symbol) and symbol.upper() in runner_markets
            rides = rides or (not symbol and key.kind == "INDEX")
        else:
            rides = key.kind == "INDEX"
        if rides and _approach(key.approach) not in runner_skip:
            return "the live Momentum Runner rides this idea: one idea, one real ticket"
    return ""


# ------------------------------------------------------------- the validator --
def problems(obj: Any, ctx: CheckContext, *, raw_size: int = 0) -> list[tuple[str, str]]:
    """Every check that fails, as (code, why), V1 first. An empty list means valid. Never raises."""
    try:
        return _problems(obj, ctx, raw_size)
    except Exception as exc:                                    # a check that crashed is a failed check
        return [("V13", f"the policy could not be checked ({type(exc).__name__}: {exc})")]


def _problems(obj: Any, ctx: CheckContext, raw_size: int) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if not isinstance(obj, dict):
        return [("V1", "the policy is not a JSON object")]
    if raw_size > POLICY_MAX_BYTES:
        out.append(("V1", f"the policy is {raw_size} bytes, over the {POLICY_MAX_BYTES} limit"))
    try:
        canonical(obj)
    except Exception as exc:
        return out + [("V1", f"not canonical JSON ({exc})")]
    if len(canonical(obj)) > POLICY_MAX_BYTES:
        out.append(("V1", "the policy is over the size limit"))
    # V13 first in practice: the other checks need the right shapes
    shape = _shape(obj)
    if shape:
        return out + [("V13", m) for m in shape]
    # V2
    if obj["schema"] != SCHEMA_VERSION:
        out.append(("V2", f"schema {obj['schema']!r}, not {SCHEMA_VERSION}"))
    # V3
    if not isinstance(obj["sha256"], str) or obj["sha256"] != sha_of(obj):
        out.append(("V3", "the sha256 does not match the contents"))
    # V4
    login = int(obj.get("account_login") or 0)
    if ctx.account_login and login and login != int(ctx.account_login):
        out.append(("V4", f"written for account {login}, this is {ctx.account_login}"))
    # V5
    if obj["envelope_sha"] != ctx.envelope_sha:
        out.append(("V5", "written under another envelope"))
    # V6
    lease = lease_problem(obj, ctx.now)
    if lease:
        out.append(("V6", lease))
    # V7
    if int(obj["seq"]) < int(ctx.last_seq):
        out.append(("V7", f"seq {obj['seq']} is below the last seen {ctx.last_seq} (rolled back)"))
    # V8
    out += [("V8", m) for m in _routes(obj)]
    # V9
    out += [("V9", m) for m in _money(obj, ctx)]
    # V10
    out += [("V10", m) for m in _grants(obj, ctx)]
    # V11
    out += [("V11", m) for m in _trials(obj, ctx)]
    # V12
    out += [("V12", m) for m in _brakes(obj)]
    return out


def lease_problem(obj: dict, now: Optional[dt.datetime]) -> str:
    """V6 alone ("" when the lease holds): valid_s within the 180-second lease and, with a clock, written no
    more than 120 s in the future and no more than valid_s ago. ``obj`` must have passed V13."""
    valid_s = float(obj["valid_s"])
    if not (0 < valid_s <= LEASE_SECONDS):
        return f"valid_s {valid_s:g} is outside the {LEASE_SECONDS:g}-second lease"
    if now is None:
        return ""
    age = (as_utc(now) - parse_utc(obj["written_utc"])).total_seconds()
    if age < -FUTURE_SKEW_SECONDS:
        return f"written {-age:.0f} s in the future"
    if age > valid_s:
        return f"stale: written {age:.0f} s ago, valid for {valid_s:g} s"
    return ""


def _shape(obj: dict) -> list[str]:
    """V13: no unknown key or field, the required ones present, every field of the right type."""
    msgs: list[str] = []
    bad = _unknown(obj, POLICY_KEYS, "the policy")
    if bad:
        msgs.append(bad)
    missing = [k for k in POLICY_REQUIRED if k not in obj]
    if missing:
        msgs.append(f"missing field(s) {missing}")
        return msgs
    if not _is_int(obj["schema"]):
        msgs.append("schema is not a whole number")
    if not _is_int(obj["seq"]) or obj["seq"] < 0:
        msgs.append("seq is not a whole number of zero or more")
    if not isinstance(obj["written_utc"], str) or parse_utc(obj["written_utc"]) is None:
        msgs.append("written_utc is not a time")
    if not _finite(obj["valid_s"]):
        msgs.append("valid_s is not a number")
    if "account_login" in obj and not _is_int(obj["account_login"]):
        msgs.append("account_login is not a whole number")
    if not isinstance(obj["envelope_sha"], str):
        msgs.append("envelope_sha is not text")
    if "build" in obj and not isinstance(obj["build"], str):
        msgs.append("build is not text")
    if not isinstance(obj["mode"], str):
        msgs.append("mode is not text")
    if not isinstance(obj["grants_allowed"], bool):
        msgs.append("grants_allowed is not true or false")
    if not isinstance(obj["sha256"], str):
        msgs.append("sha256 is not text")
    if "headroom_gbp" in obj and not (obj["headroom_gbp"] is None or _is_num(obj["headroom_gbp"])):
        msgs.append("headroom_gbp is not a number")
    # data
    data = obj.get("data", {})
    if not isinstance(data, dict):
        msgs.append("data is not a table")
    else:
        b = _unknown(data, DATA_KEYS, "data")
        if b:
            msgs.append(b)
        if "ok" in data and not isinstance(data["ok"], bool):
            msgs.append("data.ok is not true or false")
        if "failed" in data and not (isinstance(data["failed"], list)
                                     and all(isinstance(x, str) for x in data["failed"])):
            msgs.append("data.failed is not a list of text")
        bc = data.get("broker_check", {})
        if not isinstance(bc, dict):
            msgs.append("data.broker_check is not a table")
        else:
            b = _unknown(bc, BROKER_CHECK_KEYS, "data.broker_check")
            if b:
                msgs.append(b)
            for k, v in bc.items():
                if not (v is None or _is_num(v)):
                    msgs.append(f"data.broker_check.{k} is not a number")
    for name, keys in (("account", ACCOUNT_KEYS), ("brakes", BRAKES_KEYS)):
        block = obj.get(name, {})
        if not isinstance(block, dict):
            msgs.append(f"{name} is not a table")
            continue
        b = _unknown(block, keys, name)
        if b:
            msgs.append(b)
    for k, v in (obj.get("account") or {}).items() if isinstance(obj.get("account"), dict) else ():
        if not (v is None or _is_num(v)):
            msgs.append(f"account.{k} is not a number")
    brakes = obj.get("brakes") if isinstance(obj.get("brakes"), dict) else {}
    for k in ("day", "week", "drawdown"):
        if k in brakes and not isinstance(brakes[k], str):
            msgs.append(f"brakes.{k} is not text")
    if "trial_budget_left_gbp" in brakes and not (brakes["trial_budget_left_gbp"] is None
                                                  or _is_num(brakes["trial_budget_left_gbp"])):
        msgs.append("brakes.trial_budget_left_gbp is not a number")
    # entries
    entries = obj["entries"]
    if not isinstance(entries, dict):
        msgs.append("entries is not a table")
    else:
        for k, e in entries.items():
            try:
                Key.parse(k)
            except ValueError as exc:
                msgs.append(str(exc))
            if not isinstance(e, dict):
                msgs.append(f"entry {k} is not a table")
                continue
            b = _unknown(e, ENTRY_KEYS, f"entry {k}")
            if b:
                msgs.append(b)
            if "route" not in e or "state" not in e:
                msgs.append(f"entry {k} needs a route and a state")
            for f in ("route", "state"):
                if f in e and not isinstance(e[f], str):
                    msgs.append(f"entry {k}: {f} is not text")
            for f in ("risk_pct", "money_gbp"):
                if f in e and not (e[f] is None or _is_num(e[f])):
                    msgs.append(f"entry {k}: {f} is not a number")
            if "step" in e and not (e["step"] is None or e["step"] in STEP_ORDER):
                msgs.append(f"entry {k}: step {e['step']!r} is not a step")
            if "rule" in e and not _opt_text(e["rule"]):
                msgs.append(f"entry {k}: rule is not text")
            if "why_id" in e and not (e["why_id"] is None or _is_int(e["why_id"])):
                msgs.append(f"entry {k}: why_id is not a whole number")
            if "why" in e and not (e["why"] is None or (isinstance(e["why"], str)
                                                         and len(e["why"]) <= WHY_MAX_CHARS)):
                msgs.append(f"entry {k}: why is not short text")
            if "until_utc" in e and not _opt_time(e["until_utc"]):
                msgs.append(f"entry {k}: until_utc is not a time")
    # floors
    floors = obj.get("floors", {})
    if not isinstance(floors, dict):
        msgs.append("floors is not a table")
    else:
        for k, f in floors.items():
            try:
                Key.parse(k)
            except ValueError as exc:
                msgs.append(str(exc))
            if not isinstance(f, dict):
                msgs.append(f"floor {k} is not a table")
                continue
            b = _unknown(f, FLOOR_KEYS, f"floor {k}")
            if b:
                msgs.append(b)
            for name, v in f.items():
                if not (v is None or _is_num(v)):
                    msgs.append(f"floor {k}: {name} is not a number")
    # blocks
    blocks = obj.get("blocks", [])
    if not isinstance(blocks, list):
        msgs.append("blocks is not a list")
    else:
        for i, bl in enumerate(blocks):
            if not isinstance(bl, dict):
                msgs.append(f"block {i} is not a table")
                continue
            b = _unknown(bl, BLOCK_KEYS, f"block {i}")
            if b:
                msgs.append(b)
            if not (isinstance(bl.get("bots"), list) and bl["bots"]
                    and all(isinstance(x, str) and x in KEY_BOTS for x in bl["bots"])):
                msgs.append(f"block {i}: bots is not a list of bots")
            if not (isinstance(bl.get("symbols"), list) and bl["symbols"]
                    and all(isinstance(x, str) and x.strip() for x in bl["symbols"])):
                msgs.append(f"block {i}: symbols is not a list of symbols")
            if not isinstance(bl.get("route"), str):
                msgs.append(f"block {i}: route is not text")
            if not _opt_text(bl.get("rule")):
                msgs.append(f"block {i}: rule is not text")
            if not _opt_time(bl.get("until_utc")):
                msgs.append(f"block {i}: until_utc is not a time")
            if not (bl.get("why_id") is None or _is_int(bl.get("why_id"))):
                msgs.append(f"block {i}: why_id is not a whole number")
            if not (bl.get("why") is None or (isinstance(bl["why"], str) and len(bl["why"]) <= WHY_MAX_CHARS)):
                msgs.append(f"block {i}: why is not short text")
    # gates
    if "gates" in obj and obj["gates"] is not None:
        g = obj["gates"]
        if not isinstance(g, dict):
            msgs.append("gates is not a table")
        else:
            b = _unknown(g, GATES_KEYS, "gates")
            if b:
                msgs.append(b)
            ro = g.get("rollover_ny", ["16:45", "17:15"])
            if not (isinstance(ro, list) and len(ro) == 2 and all(isinstance(x, str) and _HHMM.match(x) for x in ro)):
                msgs.append("gates.rollover_ny is not two HH:MM times")
            fc = g.get("friday_cutoff_ny", "16:00")
            if not (isinstance(fc, str) and _HHMM.match(fc)):
                msgs.append("gates.friday_cutoff_ny is not an HH:MM time")
    # demo
    if "demo" in obj and obj["demo"] is not None:
        d = obj["demo"]
        if not isinstance(d, dict):
            msgs.append("demo is not a table")
        else:
            b = _unknown(d, DEMO_KEYS, "demo")
            if b:
                msgs.append(b)
            for k, v in d.items():
                if not (v is None or _is_num(v)):
                    msgs.append(f"demo.{k} is not a number")
    # judged
    if "judged" in obj and obj["judged"] is not None:
        j = obj["judged"]
        if not isinstance(j, dict):
            msgs.append("judged is not a table")
        else:
            for k, v in j.items():
                try:
                    Key.parse(k)
                except ValueError as exc:
                    msgs.append(str(exc))
                if not isinstance(v, dict):
                    msgs.append(f"judged {k} is not a table")
                    continue
                b = _unknown(v, JUDGED_KEYS, f"judged {k}")
                if b:
                    msgs.append(b)
                if v.get("verdict") not in JUDGEMENTS:
                    msgs.append(f"judged {k}: verdict is not one of {JUDGEMENTS}")
                if not (_is_int(v.get("live_trades")) and v["live_trades"] >= 0):
                    msgs.append(f"judged {k}: live_trades is not a whole number")
                if not (isinstance(v.get("judged_utc"), str) and parse_utc(v["judged_utc"]) is not None):
                    msgs.append(f"judged {k}: judged_utc is not a time")
    return msgs


def _routes(obj: dict) -> list[str]:
    msgs = []
    if obj["mode"] not in MODES:
        msgs.append(f"mode {obj['mode']!r} is not one of {MODES}")
    for k, e in obj["entries"].items():
        if e["route"] not in ROUTES:
            msgs.append(f"entry {k}: route {e['route']!r} is not one of {ROUTES}")
        if e["state"] not in STATES:
            msgs.append(f"entry {k}: state {e['state']!r} is not one of {STATES}")
        if e["route"] == ROUTE_REAL and (e.get("risk_pct") is None or e.get("money_gbp") is None):
            msgs.append(f"entry {k}: a REAL route needs risk_pct and money_gbp")
    for i, bl in enumerate(obj.get("blocks", [])):
        if bl["route"] not in RESTRICTING_ROUTES:
            msgs.append(f"block {i}: route {bl['route']!r} is not PRACTICE or SKIP")
    b = obj.get("brakes", {})
    for name, allowed in (("day", BRAKE_DAY), ("week", BRAKE_WEEK), ("drawdown", BRAKE_DRAWDOWN)):
        if name in b and b[name] not in allowed:
            msgs.append(f"brakes.{name} {b[name]!r} is not one of {allowed}")
    return msgs


def _demo_ok_shape(obj: dict) -> bool:
    return isinstance(obj.get("demo"), dict) and bool(obj["demo"])


def caps_pct(obj: dict, ctx: CheckContext) -> float:
    """The most a REAL entry may risk, in percent of equity: the ceiling and the envelope's step cap; with
    a demo block, the demo ladder's (the gate falls back to v1 whenever the demo test fails)."""
    v1 = min(float(ctx.max_risk_pct), float(ctx.envelope.max_step_pct))
    if _demo_ok_shape(obj):
        d = obj["demo"].get("max_risk_pct")
        if _finite(d):
            demo_cap = min(float(ctx.envelope.demo_max_step_pct), DEMO_MAX_STEP_PCT, float(d))
            return max(v1, demo_cap)
    return v1


def _money(obj: dict, ctx: CheckContext) -> list[str]:
    msgs = []
    cap = caps_pct(obj, ctx)
    eq = (obj.get("account") or {}).get("equity")
    for k, e in obj["entries"].items():
        for f in ("risk_pct", "money_gbp"):
            v = e.get(f)
            if v is None:
                continue
            if not _finite(v) or float(v) < 0:
                msgs.append(f"entry {k}: {f} {v!r} is not a finite number of zero or more")
        if e["route"] != ROUTE_REAL:
            continue
        rp, money = e.get("risk_pct"), e.get("money_gbp")
        if _finite(rp) and float(rp) > cap + 1e-9:
            msgs.append(f"entry {k}: risk_pct {rp:g} is above the cap of {cap:g}%")
        if not (_finite(eq) and float(eq) > 0):
            msgs.append(f"entry {k}: a REAL route needs the account's equity in the policy")
        elif _finite(money) and float(money) > float(eq) * cap / 100.0 + 0.01:
            msgs.append(f"entry {k}: money_gbp {money:g} is above {cap:g}% of the equity {float(eq):.2f}")
    for k, f in (obj.get("floors") or {}).items():
        for name, v in f.items():
            if v is not None and (not _finite(v) or float(v) < 0):
                msgs.append(f"floor {k}: {name} {v!r} is not a finite number of zero or more")
    d = obj.get("demo")
    if isinstance(d, dict):
        env = ctx.envelope
        for k, v in d.items():
            if v is not None and (not _finite(v) or float(v) < 0):
                msgs.append(f"demo.{k} {v!r} is not a finite number of zero or more")
        stop = d.get("max_daily_loss_pct")
        if _finite(stop):
            top = min(float(env.demo_max_daily_stop_pct), DEMO_MAX_DAILY_STOP_PCT)
            if float(stop) > top + 1e-9:
                msgs.append(f"demo.max_daily_loss_pct {stop:g} is above {top:g}")
            if float(stop) > ctx.max_daily_loss_pct + 1e-9 and not any(
                    abs(float(stop) - lvl) < 1e-9 for lvl in DEMO_DAILY_STOP_LADDER):
                msgs.append(f"demo.max_daily_loss_pct {stop:g} is not a level of the demo ladder "
                            f"{DEMO_DAILY_STOP_LADDER}")
        mr = d.get("max_risk_pct")
        if _finite(mr) and float(mr) > min(float(env.demo_max_step_pct), DEMO_MAX_STEP_PCT) + 1e-9:
            msgs.append(f"demo.max_risk_pct {mr:g} is above the envelope's demo cap")
        dd = d.get("max_drawdown_pct")
        if _finite(dd) and float(dd) > DEMO_DRAWDOWN_BREAKER_PCT + 1e-9:
            msgs.append(f"demo.max_drawdown_pct {dd:g} is above {DEMO_DRAWDOWN_BREAKER_PCT:g}")
        if _finite(dd) and float(dd) > ctx.max_drawdown_pct + 1e-9 and not (
                _finite(stop) and float(stop) >= DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT - 1e-9):
            msgs.append(f"demo.max_drawdown_pct {dd:g} lifts the drawdown breaker before the daily stop is "
                        f"{DEMO_DRAWDOWN_BREAKER_FROM_STOP_PCT:g}% or more")
        tot, cor = d.get("max_total_risk_pct"), d.get("max_correlated_risk_pct")
        if _finite(tot) and float(tot) > DEMO_TOTAL_MAX_PCT + 1e-9:
            msgs.append(f"demo.max_total_risk_pct {tot:g} is above {DEMO_TOTAL_MAX_PCT:g}")
        if _finite(cor) and float(cor) > DEMO_TOTAL_MAX_PCT + 1e-9:
            msgs.append(f"demo.max_correlated_risk_pct {cor:g} is above {DEMO_TOTAL_MAX_PCT:g}")
    return msgs


def _judged(obj: dict) -> dict:
    out = {}
    for k, v in (obj.get("judged") or {}).items():
        out[k] = Judgement(str(v["verdict"]), int(v["live_trades"]), str(v["judged_utc"]))
    return out


def _grants(obj: dict, ctx: CheckContext) -> list[str]:
    msgs = []
    env = ctx.envelope
    pins_now = active_pins(env, _judged(obj))
    pin_keys = {p.key for p in env.pins}
    for k in (obj.get("judged") or {}):
        if k not in pin_keys:
            msgs.append(f"judged {k}: not one of Aaron's pins")
    for k, e in obj["entries"].items():
        if e["route"] != ROUTE_REAL:
            continue
        why = real_forbidden(Key.parse(k), env, pins_now, runner_live=ctx.runner_live,
                             runner_skip=ctx.runner_skip, runner_markets=ctx.runner_markets)
        if why:
            msgs.append(f"entry {k}: {why}")
    return msgs


def _trials(obj: dict, ctx: CheckContext) -> list[str]:
    msgs = []
    env = ctx.envelope
    demo = _demo_ok_shape(obj)
    most = max(int(env.max_trials), DEMO_MAX_TRIALS) if demo else int(env.max_trials)
    pct = max(float(env.trial_risk_pct), ctx.base_pct) if demo else float(env.trial_risk_pct)
    money = max(float(env.trial_max_gbp), ctx.base_money) if demo else float(env.trial_max_gbp)
    trials = [(k, e) for k, e in obj["entries"].items() if e["state"] == STATE_LIVE_TRIAL]
    if len(trials) > most:
        msgs.append(f"{len(trials)} trials, the envelope allows {most}")
    for k, e in trials:
        rp, m = e.get("risk_pct"), e.get("money_gbp")
        if _finite(rp) and float(rp) > pct + 1e-9:
            msgs.append(f"trial {k}: risk_pct {rp:g} is above the trial size of {pct:g}%")
        if _finite(m) and float(m) > money + 1e-9:
            msgs.append(f"trial {k}: money_gbp {m:g} is above the trial cap of {money:g}")
    return msgs


def _brakes(obj: dict) -> list[str]:
    msgs = []
    b = obj.get("brakes") or {}
    v = b.get("trial_budget_left_gbp")
    if v is not None and (not _finite(v) or float(v) < 0):
        msgs.append(f"brakes.trial_budget_left_gbp {v!r} is not a finite number of zero or more")
    h = obj.get("headroom_gbp")
    if h is not None and (not _finite(h) or float(h) < 0):
        msgs.append(f"headroom_gbp {h!r} is not a finite number of zero or more")
    for k, val in (obj.get("account") or {}).items():
        if val is not None and not _finite(val):
            msgs.append(f"account.{k} {val!r} is not a finite number")
    bc = ((obj.get("data") or {}).get("broker_check") or {})
    for k, val in bc.items():
        if val is not None and not _finite(val):
            msgs.append(f"data.broker_check.{k} {val!r} is not a finite number")
    return msgs


# ------------------------------------------------------------- objects --
def to_policy(obj: dict) -> Policy:
    """The dataclass of a policy that has passed V13 (shape)."""
    entries = {k: Entry(route=e["route"], state=e["state"], risk_pct=_f(e.get("risk_pct")),
                        money_gbp=_f(e.get("money_gbp")), step=e.get("step"), rule=e.get("rule"),
                        why_id=e.get("why_id"), why=e.get("why"), until_utc=e.get("until_utc"))
               for k, e in obj["entries"].items()}
    floors = {k: Floor(_f(f.get("min_score")), _f(f.get("min_rr"))) for k, f in (obj.get("floors") or {}).items()}
    blocks = tuple(Block(tuple(b["bots"]), tuple(str(s).upper() for s in b["symbols"]), b["route"], b.get("rule"),
                         b.get("until_utc"), b.get("why_id"), b.get("why")) for b in (obj.get("blocks") or []))
    g = obj.get("gates")
    gates = (Gates(tuple(g.get("rollover_ny", ["16:45", "17:15"])), g.get("friday_cutoff_ny", "16:00"))
             if isinstance(g, dict) else None)
    d = obj.get("demo")
    demo = (DemoLimits(**{k: _f(d.get(k)) for k in DEMO_KEYS}) if isinstance(d, dict) and d else None)
    return Policy(seq=int(obj["seq"]), written_utc=obj["written_utc"], envelope_sha=obj["envelope_sha"],
                  mode=obj["mode"], grants_allowed=bool(obj["grants_allowed"]), entries=entries,
                  valid_s=float(obj["valid_s"]), account_login=int(obj.get("account_login") or 0),
                  build=str(obj.get("build") or ""), data=dict(obj.get("data") or {}),
                  account=dict(obj.get("account") or {}), brakes=dict(obj.get("brakes") or {}),
                  headroom_gbp=_f(obj.get("headroom_gbp")), floors=floors, blocks=blocks, gates=gates, demo=demo,
                  judged=_judged(obj), schema=int(obj["schema"]), sha256=str(obj["sha256"]))


def _f(x: Any) -> Optional[float]:
    return float(x) if _is_num(x) else None


def validate(obj: Any, ctx: CheckContext, *, raw_size: int = 0) -> Policy:
    """The policy as a dataclass, or PolicyError with the first failing check (the writer and every reader
    call this same function)."""
    found = problems(obj, ctx, raw_size=raw_size)
    if found:
        code, why = found[0]
        raise PolicyError(code, why)
    return to_policy(obj)


def seal(policy: Policy) -> Policy:
    """The policy with its sha256 filled in."""
    body = policy.to_dict(with_sha=False)
    return dataclasses.replace(policy, sha256=sha_of(body))


def encode(policy: Policy) -> bytes:
    """The bytes written to disk: canonical JSON, sha256 included."""
    return canonical(policy.to_dict(with_sha=True)).encode("ascii")


# ------------------------------------------------------------- disk --
def read_bytes(path: "str | Path") -> Optional[bytes]:
    """The file's bytes (at most a little over the limit), or None when it is not there."""
    try:
        with open(path, "rb") as fh:
            return fh.read(POLICY_MAX_BYTES + 1)
    except FileNotFoundError:
        return None


def load(path: "str | Path", ctx: CheckContext) -> Policy:
    """Read and validate one policy file (raises PolicyError; V1 when it is missing)."""
    raw = read_bytes(path)
    if raw is None:
        raise PolicyError("V1", "no policy file")
    obj = parse_bytes(raw)
    return validate(obj, ctx, raw_size=len(raw))


def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write(paths: Paths, policy: Policy, ctx: CheckContext) -> Path:
    """Write the policy atomically (the coach's writer; never a bot).

    The policy is sealed, then run through the SAME validator the gate runs (PolicyError aborts the write,
    nothing on disk changes). Then: a temporary file, fsync, the current file copied to policy.prev.json
    (itself atomically), and os.replace. A reader running alongside sees the old file or the new one,
    never a mix. Ledger rows (section 10.2, step 4) are the caller's, before this call."""
    sealed = seal(policy)
    data = encode(sealed)
    obj = parse_bytes(data)
    validate(obj, ctx, raw_size=len(data))
    paths.coach_dir.mkdir(parents=True, exist_ok=True)
    tmp = paths.policy_tmp
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    current = paths.policy
    if current.exists():
        prev_tmp = paths.coach_dir / (paths.policy_prev.name + ".tmp")
        try:
            body = current.read_bytes()
            with open(prev_tmp, "wb") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(prev_tmp, paths.policy_prev)
        except OSError:
            prev_tmp.unlink(missing_ok=True)
            raise
    os.replace(tmp, current)
    _fsync_dir(paths.coach_dir)
    return current
