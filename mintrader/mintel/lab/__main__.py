"""``python -m mintel.lab``: the lab's one command.

    python -m mintel.lab nightly  --config data/config.json [nightly options]
    python -m mintel.lab practice --config data/config.json
    python -m mintel.lab show     --config data/config.json [--one-line]
    python -m mintel.lab mode     off|paper|show --config data/config.json
    python -m mintel.lab reopen   <id> --reason "..." --config data/config.json
    python -m mintel.lab due      --config data/config.json      (exit 0 when tonight's run is owed)
    python -m mintel.lab refresh  --config data/config.json [--force]   (the weekly pool refresh, by hand)

``mode`` is the only writer of ``data/lab.json`` (atomically); the bots' own
modes command is not involved. There is no live mode: the lab has no live
path, and LIVE written by hand reads as PAPER.

``reopen`` is Claude's paper-side call (Aaron, 9 Oct: "anything on paper is your
call"): it puts a binned idea back for a retest on the next stretch and logs it
on the events table, by 'claude', with the reason, for the page to show.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from typing import Optional, Sequence

from . import BINNED, FRESH_TOO_FEW, UTC, LabPaths, LabSettings


def _cfg(path: str):
    from ..config import Config
    return Config.load(path)


def cmd_mode(args) -> int:
    cfg = _cfg(args.config)
    paths = LabPaths(cfg.ops.data_dir)
    current = LabSettings.load(paths.data_dir)
    word = args.word.upper()
    if word == "SHOW":
        print(f"The lab is {current.mode} (data/lab.json)."
              + (" It practises on live prices with no money; it cannot place an order."
                 if current.mode == "PAPER" else " Paused: nothing practises and the nightly does not run."))
        return 0
    if word not in ("OFF", "PAPER"):
        print(f"Nothing changed: the lab's mode is OFF or PAPER (it has no live mode), not {args.word!r}.",
              file=sys.stderr)
        return 2
    raw = current.to_dict()
    raw["mode"] = word
    LabSettings.from_dict(raw).save(paths.data_dir)
    print(f"The lab is now {word}." + (" Paused: practice stops within a minute and the nightly skips."
                                        if word == "OFF" else " Practice starts within a minute."))
    return 0


def cmd_show(args) -> int:
    cfg = _cfg(args.config)
    from .report import board_text
    from .status import one_line, read_json
    if args.one_line:
        print(one_line(cfg.ops.data_dir))
        return 0
    paths = LabPaths(cfg.ops.data_dir)
    summary, why = read_json(paths.summary)
    if summary is None and not paths.root.exists():
        why = ""
    print(one_line(cfg.ops.data_dir))
    print()
    print(board_text(summary, why))
    return 0


def cmd_reopen(args) -> int:
    cfg = _cfg(args.config)
    paths = LabPaths(cfg.ops.data_dir)
    if not args.reason.strip():
        print("Nothing changed: a reason is needed (it is shown on the page).", file=sys.stderr)
        return 2
    from .ledger import LabLedger, LedgerError
    led = LabLedger(paths.sqlite, paths.trials)
    try:
        row = led.candidate(args.id)
        if row is None:
            print(f"No such idea: {args.id}", file=sys.stderr)
            return 2
        if row["stage"] != BINNED:
            print(f"Nothing changed: {args.id} is at {row['stage']}, not binned (only a binned idea "
                  f"can be reopened).", file=sys.stderr)
            return 2
        looks = led.fresh_rows(args.id)
        retry = (len(looks) == 1 and looks[0]["status"] == "done"
                 and FRESH_TOO_FEW in (json.loads(looks[0]["reasons_json"] or "[]") or []))
        if looks and not retry:
            print(f"Nothing changed: {args.id} already had its one fresh look, so reopening it could "
                  f"only run its past test again, for ever, and never give it a second look.",
                  file=sys.stderr)
            return 2
        led.reopen(args.id, args.reason.strip(), by="claude", now=dt.datetime.now(UTC))
    except LedgerError as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 2
    finally:
        led.close()
    print(f"{args.id} ({row['name']}) is back at IDEA, due a retest: {args.reason.strip()}")
    return 0


def cmd_due(args) -> int:
    cfg = _cfg(args.config)
    from .nightly import due
    owed, night = due(cfg.ops.data_dir)
    if LabSettings.load(cfg.ops.data_dir).mode == "OFF":
        owed = False
    print(f"{'due' if owed else 'not due'} {night}")
    return 0 if owed else 1


def cmd_refresh(args) -> int:
    """The weekly refresh of the practice pool by hand (the nightly runs it on Saturdays)."""
    cfg = _cfg(args.config)
    paths = LabPaths(cfg.ops.data_dir, cfg.ops.log_dir)
    from .cut import load_cut
    from .ledger import LabLedger
    from .refresh import refresh_pool
    from .stages import night_of
    now = dt.datetime.now(UTC)
    led = LabLedger(paths.sqlite, paths.trials)
    try:
        cut = load_cut(paths)
        rep = refresh_pool(led, paths, LabSettings.load(paths.data_dir), night=night_of(now), now=now,
                           news_awake=bool(cut.news_awake) if cut else False, force=args.force, log=print)
    finally:
        led.close()
    print(f"{len(rep['retired'])} retired, {len(rep['variants'])} variants, {len(rep['new'])} new; "
          f"pool {rep['pool']} of {rep['target']} (data/lab/runs/{rep['night']}/refresh.md)")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "nightly":
        from .nightly import main as nightly_main
        return nightly_main(argv[1:])
    if argv and argv[0] == "practice":
        from .practice import main as practice_main
        return practice_main(argv[1:])
    ap = argparse.ArgumentParser(prog="python -m mintel.lab", description="the strategy lab")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("nightly", help="the night's run (see python -m mintel.lab.nightly --help)")
    sub.add_parser("practice", help="the practice process (the watchdog runs it)")
    p = sub.add_parser("show", help="the board in plain text")
    p.add_argument("--config", default="data/config.json")
    p.add_argument("--one-line", action="store_true")
    p.set_defaults(fn=cmd_show)
    p = sub.add_parser("mode", help="off | paper | show")
    p.add_argument("word")
    p.add_argument("--config", default="data/config.json")
    p.set_defaults(fn=cmd_mode)
    p = sub.add_parser("reopen", help="put a binned idea back for a retest (Claude's call, logged)")
    p.add_argument("id")
    p.add_argument("--reason", required=True)
    p.add_argument("--config", default="data/config.json")
    p.set_defaults(fn=cmd_reopen)
    p = sub.add_parser("due", help="exit 0 when tonight's run has not finished and none is running")
    p.add_argument("--config", default="data/config.json")
    p.set_defaults(fn=cmd_due)
    p = sub.add_parser("refresh", help="the weekly refresh of the practice pool (the nightly runs it on Saturdays)")
    p.add_argument("--config", default="data/config.json")
    p.add_argument("--force", action="store_true", help="run it again although it ran tonight")
    p.set_defaults(fn=cmd_refresh)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
