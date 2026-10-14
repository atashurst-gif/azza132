"""Real-money approval: PREPARED, and DISABLED (Aaron, 14 Oct 2026).

    python -m mintel.ops.realmoney --data data status
    python -m mintel.ops.realmoney --data data set-passphrase
    python -m mintel.ops.realmoney --data data approve --id lab:abc123 --version v1 --login 12345678 --risk 0.5
    python -m mintel.ops.realmoney --data data keep-demo --id lab:abc123 --version v1 [--why "..."]
    python -m mintel.ops.realmoney --data data reject --id lab:abc123 --version v1 [--why "..."]

"DEMO" in this codebase is real orders on the IC Markets DEMO account. "REAL MONEY" here means a future,
separate, non-demo account. Nothing in this module - or anywhere - can send an order to one:

* ``REAL_TRADING_ENABLED = False`` is a hard constant. ``guard_real_order`` raises while it is False, whatever
  has been approved; ``real_trading_allowed`` is False. No order path imports this module to send anything.
* No real broker connection is configured. The future real account has its own, never-yet-used section,
  data/realmoney/real_account.json, kept apart from the demo connection (config.json / secrets.json, which this
  module never opens). ``verify_accounts`` refuses to treat the demo login as real or a real login as demo:
  real_account.json may not hold a known demo login (DEMO_LOGINS, config.json's ``account_login``) or name a
  demo server.
* The existing demo checks (``Config.is_live`` and its marker, the weekly pick, ``modes.set_modes``) are
  untouched.

HOW APPROVAL WORKS - only on the Mac, at the command line; never from the web page (it has no login and runs on
127.0.0.1: its buttons only show the command to run):

* ``set-passphrase`` once: a local passphrase, stored only as a PBKDF2-SHA256 hash with a random salt in
  data/realmoney/passphrase.json (never in source control: data/ is the machine's own).
* ``approve`` needs: the exact strategy id and version (as data/live_readiness.json names it, at the stage
  "qualified"), the real account's login (never the demo's), the risk % (a tier: 0.5, 1 or 1.5), a typed
  confirmation phrase (``confirmation_phrase``) and the passphrase. A tier above 0.5% needs the lower tier
  approved first for that same id, version and real login, and its own approval. The combined planned risk of
  every approval that still stands (each id AND version counts) stays within ``sizing.MAX_COMBINED_PCT``.
* ``keep-demo`` and ``reject`` record the decision (passphrase too). Either one withdraws every earlier approval
  of that id and version: its ladder starts again at 0.5% and it no longer counts toward the combined cap
  (``current_approvals``). An approval ON a different login starts that id and version's ladder again there.
* Every decision is one line of data/realmoney/approvals.jsonl, append-only, each line chained to the one
  before by a SHA-256 hash, and data/realmoney/approvals.head.json (rewritten after every line) holds the line
  count and the last hash. So a line changed, removed, added or cut off by hand - at the end too - or a line
  that does not parse shows (``chain_ok``), and no decision is recorded on a record that does not check out.
  It is a tamper check, not a signature: the hashes are not keyed, so someone able to rewrite every line and
  the head file on the Mac could rebuild a record that checks out.

An approval is a record. It does not switch anything on: while ``REAL_TRADING_ENABLED`` is False an approved
strategy still trades only on DEMO (or PAPER), exactly as before.
"""
from __future__ import annotations

import argparse
import datetime as dt
import getpass
import hashlib
import hmac
import json
import os
import secrets
import sys
from pathlib import Path
from typing import Optional

from ..clock import to_utc, utcnow

# ---------------------------------------------------------------- the switch --
REAL_TRADING_ENABLED = False          # hard constant: real-money trading is impossible while False

DIR = "realmoney"
PASSPHRASE_FILE = "passphrase.json"
APPROVALS_FILE = "approvals.jsonl"
HEAD_FILE = "approvals.head.json"     # {count, last_hash} of approvals.jsonl, rewritten after every append
REAL_ACCOUNT_FILE = "real_account.json"
PBKDF2_ITERATIONS = 310_000
MIN_PASSPHRASE = 10
DEMO_LOGINS = frozenset({53020675})   # IC Markets demo account (docs/REQUIREMENTS_LEDGER.md)
APPROVE, KEEP_DEMO, REJECT = "approve", "keep-demo", "reject"
TIERS = (0.5, 1.0, 1.5)


class RealTradingDisabled(RuntimeError):
    """Raised by every attempt to treat anything as a real-money order."""


class ApprovalRefused(ValueError):
    """An approval (or a decision) that does not meet every condition. Nothing is recorded."""


def real_trading_allowed() -> bool:
    """False while REAL_TRADING_ENABLED is False - approved or not - and while no real account is connected
    (none is: ``real_connected``)."""
    return bool(REAL_TRADING_ENABLED) and real_connected()


def guard_real_order(*_a, **_k) -> None:
    """The one door a real-money order would have to pass: it is shut. Raises whatever is asked."""
    if not REAL_TRADING_ENABLED:
        raise RealTradingDisabled("real-money trading is disabled (REAL_TRADING_ENABLED = False); nothing was sent")
    raise RealTradingDisabled("no real-money broker connection exists in this code; nothing was sent")


def real_connected() -> bool:
    """Is a real-money account connected? No: there is no real broker connection in this codebase."""
    return False


# ---------------------------------------------------------------- files --
def rdir(data_dir) -> Path:
    return Path(data_dir) / DIR


def _now_iso(now: Optional[dt.datetime] = None) -> str:
    return to_utc(now or utcnow()).replace(microsecond=0).isoformat()


def _write_private(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("." + path.name + ".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, indent=1) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# ---------------------------------------------------------------- the passphrase --
def hash_passphrase(phrase: str, salt: Optional[bytes] = None, iterations: int = PBKDF2_ITERATIONS) -> dict:
    """{algo, iterations, salt, hash}: PBKDF2-SHA256 with a random 16-byte salt. The phrase itself is never kept."""
    salt = salt or secrets.token_bytes(16)
    h = hashlib.pbkdf2_hmac("sha256", phrase.encode("utf-8"), salt, int(iterations))
    return {"algo": "pbkdf2_sha256", "iterations": int(iterations), "salt": salt.hex(), "hash": h.hex()}


def set_passphrase(data_dir, phrase: str, now: Optional[dt.datetime] = None, iterations: int = PBKDF2_ITERATIONS,
                   current: Optional[str] = None) -> None:
    """Set the local passphrase (once; changing it needs the current one)."""
    if len(phrase or "") < MIN_PASSPHRASE:
        raise ApprovalRefused(f"the passphrase must be at least {MIN_PASSPHRASE} characters")
    p = rdir(data_dir) / PASSPHRASE_FILE
    if p.exists() and not check_passphrase(data_dir, current or ""):
        raise ApprovalRefused("a passphrase is already set: give the current one to change it")
    _write_private(p, dict(hash_passphrase(phrase, iterations=iterations), set_utc=_now_iso(now)))


def check_passphrase(data_dir, phrase: str) -> bool:
    try:
        rec = json.loads((rdir(data_dir) / PASSPHRASE_FILE).read_text())
        salt = bytes.fromhex(rec["salt"])
        want = bytes.fromhex(rec["hash"])
        got = hashlib.pbkdf2_hmac("sha256", (phrase or "").encode("utf-8"), salt, int(rec["iterations"]))
    except Exception:
        return False
    return hmac.compare_digest(want, got)


# ---------------------------------------------------------------- the real account (never yet used) --
REAL_ACCOUNT_TEMPLATE = {
    "note": "A FUTURE real-money account. Never used by any code while REAL_TRADING_ENABLED is False. Kept apart "
            "from the demo connection (config.json / secrets.json). No password here: none is ever stored.",
    "login": None, "server": "", "broker": "", "currency": "GBP", "connected": False,
}


def real_account(data_dir) -> dict:
    """data/realmoney/real_account.json, or the empty template (nothing is created by reading)."""
    try:
        d = json.loads((rdir(data_dir) / REAL_ACCOUNT_FILE).read_text())
        if isinstance(d, dict):
            return dict(REAL_ACCOUNT_TEMPLATE, **d)
    except Exception:
        pass
    return dict(REAL_ACCOUNT_TEMPLATE)


def demo_logins(data_dir=None) -> set:
    """Every login known to be the DEMO account: the IC Markets demo, and config.json's ``account_login``
    (read as plain JSON; secrets.json is never opened here)."""
    out = set(DEMO_LOGINS)
    if data_dir:
        try:
            raw = json.loads((Path(data_dir) / "config.json").read_text())
            v = int(raw.get("account_login") or 0)
            if v:
                out.add(v)
        except Exception:
            pass
    return out


def verify_accounts(demo_login, real_login, data_dir=None) -> tuple[bool, str]:
    """(ok, why): a real login must be a number, must not be the demo's (or any known demo login), and the demo
    login must not be the real one: real_account.json may not hold any known demo login (DEMO_LOGINS,
    config.json's ``account_login`` or ``demo_login``) or name a demo server. The demo logins are known without
    ``demo_login``, so the command line (which never passes it) gets every check."""
    try:
        r = int(real_login)
    except (TypeError, ValueError, OverflowError):
        return False, "the real account login must be a number"
    if r <= 0:
        return False, "the real account login must be a number"
    demos = demo_logins(data_dir)
    try:
        d = int(demo_login) if demo_login not in (None, "") else None
    except (TypeError, ValueError, OverflowError):
        d = None
    if d is not None:
        demos.add(d)
    if r in demos:
        return False, f"{r} is the DEMO account: it is never treated as real"
    ra = real_account(data_dir) if data_dir else REAL_ACCOUNT_TEMPLATE
    try:
        ral = int(ra.get("login") or 0)
    except (TypeError, ValueError, OverflowError):      # Infinity / 1e400 read as float('inf')
        return False, "real_account.json's login must be a number"
    if ral and ral in demos:
        return False, f"{ral} is set as the real account: it is never treated as demo"
    server = str(ra.get("server") or "")
    if "demo" in server.lower():
        return False, f"real_account.json names a demo server ({server}): a demo account is never treated as real"
    return True, ""


# ---------------------------------------------------------------- the decisions --
def confirmation_phrase(decision: str, sid: str, version: str, risk_pct: Optional[float] = None,
                        login: Optional[int] = None) -> str:
    """The exact words to type: 'APPROVE REAL MONEY lab:abc v1 0.5% ON 12345678', 'KEEP ON DEMO lab:abc v1',
    'REJECT lab:abc v1'."""
    if decision == APPROVE:
        return f"APPROVE REAL MONEY {sid} {version} {float(risk_pct):g}% ON {int(login)}"
    if decision == KEEP_DEMO:
        return f"KEEP ON DEMO {sid} {version}"
    return f"REJECT {sid} {version}"


def command_for(decision: str, sid: str, version: str, risk_pct: float = 0.5) -> str:
    """The command the page shows instead of a button that does anything."""
    base = f"python -m mintel.ops.realmoney --data data {decision} --id {sid} --version \"{version}\""
    if decision == APPROVE:
        return base + f" --login <REAL ACCOUNT LOGIN> --risk {risk_pct:g}"
    return base


def records(data_dir) -> list[dict]:
    out = []
    try:
        lines = (rdir(data_dir) / APPROVALS_FILE).read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if isinstance(d, dict):
            out.append(d)
    return out


def _line_hash(rec: dict) -> str:
    body = {k: v for k, v in rec.items() if k != "hash"}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


def chain_ok(data_dir) -> bool:
    """True when every line of the approvals record parses, is chained to the one before, and the record ends
    where approvals.head.json says it does (its line count and last hash). A line that does not parse, or a
    record cut short at the end, is broken. No head file is fine only while the record is empty. Not a
    signature: see the module docstring."""
    try:
        lines = (rdir(data_dir) / APPROVALS_FILE).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    except (OSError, ValueError):
        return False
    prev = ""
    for line in lines:
        try:
            r = json.loads(line)
        except ValueError:
            return False
        if not isinstance(r, dict) or r.get("prev") != prev or r.get("hash") != _line_hash(r):
            return False
        prev = r["hash"]
    hp = rdir(data_dir) / HEAD_FILE
    if not hp.exists():
        return not lines
    try:
        head = json.loads(hp.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(head, dict) and head.get("count") == len(lines) and head.get("last_hash") == prev


def _append(data_dir, rec: dict) -> dict:
    """Append one decision (the caller has checked ``chain_ok``), then rewrite the head file to match."""
    rs = records(data_dir)
    rec = dict(rec, prev=(rs[-1].get("hash") if rs else ""))
    rec["hash"] = _line_hash(rec)
    p = rdir(data_dir) / APPROVALS_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _write_private(rdir(data_dir) / HEAD_FILE, {"count": len(rs) + 1, "last_hash": rec["hash"],
                                                "updated_utc": rec.get("ts_utc")})
    return rec


def current_approvals(data_dir) -> dict:
    """{(id, version): {"login": int, "tiers": set of risk %}}: the approvals that stand NOW, worked out by
    walking the record in order. A later keep-demo or reject of that id and version withdraws them all; an
    approval ON a different login starts that id and version's ladder again on that login (the earlier login's
    tiers stop counting)."""
    cur: dict = {}
    for r in records(data_dir):
        k = (r.get("id"), r.get("version"))
        if r.get("decision") == APPROVE:
            e = cur.get(k)
            if e is None or e["login"] != r.get("real_login"):
                e = cur[k] = {"login": r.get("real_login"), "tiers": set()}
            try:
                e["tiers"].add(float(r.get("risk_pct") or 0.0))
            except (TypeError, ValueError):
                pass
        elif r.get("decision") in (KEEP_DEMO, REJECT):
            cur.pop(k, None)
    return cur


def approved_tiers(data_dir, sid: str, version: str, login: Optional[int] = None) -> list[float]:
    """The tiers of ``sid`` ``version`` that stand now (``current_approvals``); [] when none do, or when
    ``login`` is given and they stand on another login."""
    e = current_approvals(data_dir).get((sid, version))
    if e is None or (login is not None and e["login"] != int(login)):
        return []
    return sorted(e["tiers"])


def _readiness_bot(data_dir, sid: str, version: str) -> Optional[dict]:
    try:
        rd = json.loads((Path(data_dir) / "live_readiness.json").read_text())
    except Exception:
        return None
    return next((b for b in rd.get("bots") or () if b.get("id") == sid and b.get("version") == version), None)


def decide(data_dir, decision: str, sid: str, version: str, *, typed: str, passphrase: str,
           login: Optional[int] = None, risk_pct: Optional[float] = None, why: str = "",
           demo_login=None, now: Optional[dt.datetime] = None, by: str = "Aaron (command line)") -> dict:
    """Record one decision after every check; raises ApprovalRefused (and records nothing) otherwise. Returns the
    record. An approval never changes a mode, a config or an order path."""
    if decision not in (APPROVE, KEEP_DEMO, REJECT):
        raise ApprovalRefused(f"unknown decision {decision!r}")
    if not sid or not version:
        raise ApprovalRefused("the exact strategy id and version are needed")
    if not (rdir(data_dir) / PASSPHRASE_FILE).exists():
        raise ApprovalRefused("no passphrase set yet: run set-passphrase first")
    if not check_passphrase(data_dir, passphrase):
        raise ApprovalRefused("the passphrase is not right")
    if not chain_ok(data_dir):          # a decision on top would hide it (the head file would match again)
        raise ApprovalRefused(f"the approvals record ({DIR}/{APPROVALS_FILE}) does not check out: a line was "
                              "changed, removed, added or cut off. Nothing was recorded; look at it first")
    bot = _readiness_bot(data_dir, sid, version)
    rec = {"ts_utc": _now_iso(now), "decision": decision, "id": sid, "version": version, "by": by,
           "why": str(why or ""), "real_trading_enabled": bool(REAL_TRADING_ENABLED),
           "readiness_stage": (bot or {}).get("stage"), "readiness_fingerprint": (bot or {}).get("fingerprint")}
    if decision == APPROVE:
        from .sizing import MAX_COMBINED_PCT
        try:
            risk = float(risk_pct)
        except (TypeError, ValueError):
            raise ApprovalRefused("the risk % is needed")
        if risk not in TIERS:
            raise ApprovalRefused(f"the risk must be one of the tiers {', '.join(f'{t:g}%' for t in TIERS)}")
        ok, msg = verify_accounts(demo_login, login, data_dir)
        if not ok:
            raise ApprovalRefused(msg)
        if bot is None or bot.get("stage") != "qualified":
            raise ApprovalRefused(f"{sid} {version} is not qualified in the live-readiness review")
        have = approved_tiers(data_dir, sid, version, int(login))       # standing now, on this same login
        if risk in have:
            raise ApprovalRefused(f"{sid} {version} at {risk:g}% is already approved")
        lower = [t for t in TIERS if t < risk]
        if lower and lower[-1] not in have:
            raise ApprovalRefused(f"{risk:g}% is a higher tier: approve {lower[-1]:g}% first, separately")
        others = sum(max(e["tiers"], default=0.0) for k, e in current_approvals(data_dir).items()
                     if k != (sid, version))                            # every other id AND version counts
        if others + risk > MAX_COMBINED_PCT + 1e-9:
            raise ApprovalRefused(f"the combined planned risk would pass {MAX_COMBINED_PCT:g}%")
        want = confirmation_phrase(APPROVE, sid, version, risk, int(login))
        rec.update(real_login=int(login), risk_pct=risk, tier=TIERS.index(risk) + 1)
    else:
        want = confirmation_phrase(decision, sid, version)
    if (typed or "").strip() != want:
        raise ApprovalRefused(f"the confirmation did not match; type exactly: {want}")
    rec["confirmation"] = want
    return _append(data_dir, rec)


def status(data_dir) -> dict:
    return {"real_trading_enabled": bool(REAL_TRADING_ENABLED), "real_connected": real_connected(),
            "passphrase_set": (rdir(data_dir) / PASSPHRASE_FILE).exists(), "decisions": records(data_dir),
            "chain_ok": chain_ok(data_dir), "real_account": real_account(data_dir)}


def latest_decision(data_dir, sid: str, version: str) -> Optional[dict]:
    rs = [r for r in records(data_dir) if r.get("id") == sid and r.get("version") == version]
    return rs[-1] if rs else None


# ---------------------------------------------------------------- the command line --
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="real-money approval: prepared, disabled")
    ap.add_argument("--data", default="data")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("status")
    sub.add_parser("set-passphrase")
    for name in (APPROVE, KEEP_DEMO, REJECT):
        p = sub.add_parser(name)
        p.add_argument("--id", required=True)
        p.add_argument("--version", required=True)
        p.add_argument("--why", default="")
        if name == APPROVE:
            p.add_argument("--login", required=True)
            p.add_argument("--risk", required=True, type=float)
    a = ap.parse_args(argv)
    if a.cmd in (None, "status"):
        print(json.dumps(status(a.data), indent=1, default=str))
        return 0
    if a.cmd == "set-passphrase":
        cur = getpass.getpass("Current passphrase (blank if none): ") if (rdir(a.data) / PASSPHRASE_FILE).exists() \
            else ""
        p1 = getpass.getpass("New passphrase: ")
        if getpass.getpass("Again: ") != p1:
            print("They did not match; nothing changed.")
            return 1
        try:
            set_passphrase(a.data, p1, current=cur)
        except ApprovalRefused as exc:
            print(f"Refused: {exc}")
            return 1
        print("Passphrase set (stored only as a hash).")
        return 0
    login = getattr(a, "login", None)
    risk = getattr(a, "risk", None)
    try:
        want = confirmation_phrase(a.cmd, a.id, a.version, risk, int(login) if login else None)
    except (TypeError, ValueError):
        print("Refused: the real account login must be a number")
        return 1
    print("Real-money trading stays DISABLED in the code whatever is decided here; this only records the decision.")
    print(f"Type exactly: {want}")
    typed = input("> ")
    phrase = getpass.getpass("Passphrase: ")
    try:
        rec = decide(a.data, a.cmd, a.id, a.version, typed=typed, passphrase=phrase, login=login, risk_pct=risk,
                     why=a.why)
    except ApprovalRefused as exc:
        print(f"Refused: {exc}")
        return 1
    print(f"Recorded: {rec['decision']} {rec['id']} {rec['version']} at {rec['ts_utc']}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
