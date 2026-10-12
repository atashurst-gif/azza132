"""The coach's commands (spec section 14).

    python -m mintel.coach run                     the coach itself (the watchdog keeps it alive)
    python -m mintel.coach show [--once]           its status in plain words (--once: one line)
    python -m mintel.coach explain <key>           one key: its state, its numbers, its decisions
    python -m mintel.coach off | on                COACH-OFF: every bot on your own settings at once / back
    python -m mintel.coach watch | guard | pilot   the envelope's mode (only that field is rewritten)
    python -m mintel.coach halt | resume           data/HALT: no new real-money entries anywhere (typed YES)
    python -m mintel.coach approve <id>            approve a "Needs your yes" item (f1-jump, guard)
    python -m mintel.coach veto <id>               veto an item of tonight's announced plan
    python -m mintel.coach pin formula1 | unpin formula1
    python -m mintel.coach replay --from 2026-09-17 [--to DATE] [--csv FILE]
    python -m mintel.coach envelope show

Every command takes ``--config data/config.json`` (the data folder is the
settings file's folder) or ``--data DIR``.

These functions are the ONLY writers of the envelope (``data/coach.json``),
``data/HALT``, ``data/COACH-OFF`` and ``data/coach/approvals.json``; the run
loop never writes any of them. Nothing here writes config.json, and nothing
here opens the secrets file.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Optional, Sequence

from . import (F1_JUDGE_AFTER, F1_KEY, F1_PIN_WHY, MODE_GUARD, MODE_OFF, MODE_PILOT, MODE_WATCH, UTC, Paths,
               as_utc)
from . import status as S
from .words import key_words


def _now() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def _atomic_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, indent=1, sort_keys=True))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def data_dir_of(args) -> Path:
    if getattr(args, "data", None):
        return Path(args.data)
    return Path(args.config).resolve().parent


# ---------------------------------------------------- Aaron's writers --
def set_mode(paths: Paths, mode: str) -> str:
    """Rewrite ONLY the envelope's ``mode``, atomically. Aaron's command."""
    if mode not in (MODE_OFF, MODE_WATCH, MODE_GUARD, MODE_PILOT):
        raise ValueError(f"{mode} is not a mode")
    try:
        raw = json.loads(paths.envelope.read_text())
    except FileNotFoundError:
        return "There is no envelope yet (the installer writes it): nothing changed."
    except Exception as exc:
        return f"The envelope could not be read ({exc}): nothing changed."
    if not isinstance(raw, dict):
        return "The envelope is not a table: nothing changed."
    before = raw.get("mode")
    raw["mode"] = mode
    _atomic_json(paths.envelope, raw)
    return f"The coach's mode is now {mode} (was {before})."


def set_pin_formula1(paths: Paths, pin: bool, now: Optional[dt.datetime] = None) -> str:
    """``pin formula1`` puts Formula 1 back on Aaron's pin (set now, so an earlier judgement no longer releases
    it); ``unpin formula1`` removes the pin. Aaron's command."""
    now = as_utc(now or _now())
    try:
        raw = json.loads(paths.envelope.read_text())
    except Exception as exc:
        return f"The envelope could not be read ({exc}): nothing changed."
    pins = [p for p in (raw.get("pins") or []) if isinstance(p, dict) and p.get("key") != F1_KEY]
    if pin:
        pins.append({"key": F1_KEY, "why": F1_PIN_WHY, "judge_after": F1_JUDGE_AFTER, "set_utc": now.isoformat()})
    raw["pins"] = pins
    _atomic_json(paths.envelope, raw)
    return "Formula 1 is pinned: the coach leaves it alone." if pin else "Formula 1 is no longer pinned."


def switch_off(paths: Paths, off: bool) -> str:
    if off:
        paths.coach_off.parent.mkdir(parents=True, exist_ok=True)
        paths.coach_off.write_text(f"COACH-OFF set {_now().isoformat()} by Aaron\n")
        return "Coach off: every bot is back on your own settings at its next decision."
    try:
        paths.coach_off.unlink()
    except FileNotFoundError:
        pass
    return "Coach on: it works within your envelope again."


def set_halt(paths: Paths, halt: bool, answer: str) -> str:
    if answer.strip() != "YES":
        return "Nothing changed (type YES in capitals to confirm)."
    if halt:
        paths.halt.write_text(f"HALT set {_now().isoformat()} by Aaron\n")
        return "HALT is on: no new real-money entries in any bot. Open trades are still managed; practice goes on."
    try:
        paths.halt.unlink()
    except FileNotFoundError:
        pass
    return "HALT is off: the bots may take new real-money entries again."


def add_approval(paths: Paths, item: str, action: str, plan: str = "", now: Optional[dt.datetime] = None) -> str:
    """Append an approve or veto to ``data/coach/approvals.json`` (by: aaron)."""
    if action not in ("approve", "veto"):
        raise ValueError(action)
    try:
        items = json.loads(paths.approvals.read_text())
        if isinstance(items, dict):
            items = items.get("items") or []
        if not isinstance(items, list):
            items = []
    except Exception:
        items = []
    if not plan and action == "veto":
        doc = S.read(paths) or {}
        plan = str(((doc.get("plan") or {}).get("date")) or "")
    items.append({"id": str(item), "action": action, "plan": plan, "by": "aaron",
                  "ts": as_utc(now or _now()).isoformat()})
    _atomic_json(paths.approvals, items)
    return f"{'Approved' if action == 'approve' else 'Vetoed'}: {item}" + (f" (plan of {plan})" if plan else "") + "."


# ---------------------------------------------------------- readers --
def explain(paths: Paths, key: str) -> str:
    from .ledger import Ledger
    if not paths.ledger.exists():
        return "The coach has no ledger yet."
    led = Ledger(paths.ledger)
    try:
        seg = led.segments().get(key)
        lines = [f"{key_words(key)} ({key})"]
        if seg is None:
            lines.append("The coach has not registered this key.")
        else:
            lines.append(f"State {seg.state}" + (f", step {seg.step}" if seg.step else "")
                         + (f", rule {seg.rule}" if seg.rule else "") + f"; registered {seg.registered_utc[:16]}")
            if seg.cooloff_until:
                lines.append(f"No re-promotion before {seg.cooloff_until}.")
        doc = S.read(paths) or {}
        for k in doc.get("keys") or []:
            if k.get("key") == key:
                lines.append(f"Chance of a real edge {k.get('p_plus')}, lower bound {k.get('lcb80')} R a trade, "
                             f"{k.get('trades')} trades ({k.get('real_trades')} real).")
        for c in led.changes(target=key, limit=20):
            lines.append(f"  {str(c.ts_utc)[:16]} {c.words}")
        return "\n".join(lines)
    finally:
        led.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.coach", description="the coach: decisions within your limits")
    ap.add_argument("command", choices=["run", "show", "explain", "off", "on", "watch", "guard", "pilot", "halt",
                                        "resume", "approve", "veto", "pin", "unpin", "replay", "envelope", "tick"])
    ap.add_argument("arg", nargs="?", default="")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--data", default="")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--from", dest="from_", default="2026-09-17")
    ap.add_argument("--to", default="")
    ap.add_argument("--csv", default="")
    ap.add_argument("--yes", default="", help="for halt/resume without a prompt: YES")
    args = ap.parse_args(argv)
    data = data_dir_of(args)
    paths = Paths.of(data)
    cmd = args.command
    if cmd == "run":
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        from .run import Coach
        Coach(data).run_forever()
        return 0
    if cmd == "tick":
        from .run import Coach
        c = Coach(data)
        out = c.tick()
        c.close()
        print(S.one_line(out.get("status")))
        return 0 if out.get("ok") else 1
    if cmd == "show":
        doc = S.read(paths)
        print(S.one_line(doc) if args.once else S.text(doc))
        return 0
    if cmd == "explain":
        print(explain(paths, args.arg))
        return 0
    if cmd in ("off", "on"):
        print(switch_off(paths, cmd == "off"))
        return 0
    if cmd in ("watch", "guard", "pilot"):
        print(set_mode(paths, cmd.upper()))
        return 0
    if cmd in ("halt", "resume"):
        answer = args.yes
        if not answer:
            prompt = ("Stop every new real-money entry in every bot? Type YES: " if cmd == "halt"
                      else "Allow new real-money entries again? Type YES: ")
            try:
                answer = input(prompt)
            except EOFError:
                answer = ""
        print(set_halt(paths, cmd == "halt", answer))
        return 0
    if cmd in ("approve", "veto"):
        if not args.arg:
            print("Which item? e.g. 'python -m mintel.coach veto 1' (the number in tonight's plan).")
            return 2
        print(add_approval(paths, args.arg, cmd))
        return 0
    if cmd in ("pin", "unpin"):
        if args.arg.lower() != "formula1":
            print("Only 'pin formula1' and 'unpin formula1' exist.")
            return 2
        print(set_pin_formula1(paths, cmd == "pin"))
        return 0
    if cmd == "envelope":
        try:
            print(paths.envelope.read_text())
        except FileNotFoundError:
            print("There is no envelope yet.")
        return 0
    if cmd == "replay":
        from . import envelope as envelope_mod
        from . import records as R
        from .run import load_reports_csv, replay
        cfg = R.baseline_config(data)
        st = envelope_mod.read(paths.envelope, cfg.risk.max_risk_pct, cfg.risk.max_daily_loss_pct)
        from . import Envelope
        env = st.envelope or Envelope(mode=MODE_PILOT)
        start = dt.date.fromisoformat(args.from_)
        if args.csv:
            trades = load_reports_csv(args.csv)
        else:
            trades = R.read_evidence(data, cfg=cfg).trades
        end = dt.date.fromisoformat(args.to) if args.to else max(t.closed_utc for t in trades).date()
        rows = replay(trades, start, end, cfg, env)
        for r in rows:
            print(json.dumps(r, default=str))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
