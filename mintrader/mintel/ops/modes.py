"""Switch any bot between OFF, PAPER and LIVE from the terminal.

    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --show
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --live all
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper runner crowd
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --off bandbreaker

Bots: runner (Momentum Runner), bandbreaker (Band Breaker), crowd (Crowd
Fader), scalper (Rapid Scalper), or all. The first three keep their mode in
config.json, each in its own block ("runner", "bandbreaker", "crowd" ->
"mode"); the Rapid Scalper keeps its own file, scalper.json, in the data
folder config.json names (ops.data_dir), which is where the scalper, the
watchdog and the reconciliation read it. Everything else in both files is
left exactly as it was, and each file is written whole under a temporary
name and renamed into place, so a crash can never leave half a file behind.
Every file that will be touched is read and checked BEFORE anything is
written: a missing or broken config.json, or a broken scalper.json, means
nothing changes at all. Trend & Breakout (the main bot) is not switched
here: it is the account itself.

On the way to LIVE the bot's magic number is checked: it must be positive
and its own, not Trend & Breakout's, the scalper's or another bot's. The
magic number is the only thing that keeps one bot from seeing or touching
another's positions, so a bot whose number is not its own is refused.

A change takes effect on the next start: Stop Trading Bot, then Start
Trading Bot. The Start window prints every bot's mode at the end.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from ..config import Config
from ..scalper import EXISTING_STRATEGY_LABEL
from ..scalper.config import ScalperConfig

MODES = ("OFF", "PAPER", "LIVE")
CONFIG_BOTS = ("runner", "bandbreaker", "crowd")          # blocks in config.json
BOTS = CONFIG_BOTS + ("scalper",)                           # scalper.json
# the bots' strategy ids and the names the docs use, as spellings of the same bot
ALIASES = {"momentum_runner": "runner", "momentum-runner": "runner",
           "band_breaker": "bandbreaker", "band-breaker": "bandbreaker",
           "crowd_fader": "crowd", "crowd-fader": "crowd",
           "rapid_scalper": "scalper", "rapid-scalper": "scalper"}
TREND = "trend"                                             # Trend & Breakout: never switched, but its magic counts
WHAT = {"LIVE": "real orders on the account, under its own magic number; the broker's figures are the record",
        "PAPER": "simulated; its results are on its own tab only, never in Overall",
        "OFF": "does nothing"}
RESTART = "Restart the bot for this to take effect (Stop Trading Bot, then Start Trading Bot)."


class PartialWrite(OSError):
    """A write failed after another file had already been written: the
    message says exactly which file holds the change and which does not."""


def labels() -> dict[str, str]:
    from ..runner import STRATEGY_LABEL as RUNNER
    from ..bandbreaker import STRATEGY_LABEL as BAND
    from ..crowd import STRATEGY_LABEL as CROWD
    from ..scalper import STRATEGY_LABEL as SCALPER
    return {"runner": RUNNER, "bandbreaker": BAND, "crowd": CROWD, "scalper": SCALPER, TREND: EXISTING_STRATEGY_LABEL}


def _read_json(path: Path) -> dict:
    """The file's object, or an empty one for a file that is not there. A
    file that exists but cannot be read is an error: nothing is written over
    a broken file."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except ValueError as exc:
        raise ValueError(f"{path} is not valid JSON ({exc})") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{path} does not hold a JSON object")
    return raw


def _read_config(cfg_path: Path) -> dict:
    """config.json as an object. Missing or broken: an error, nothing written."""
    if not cfg_path.exists():
        raise FileNotFoundError(f"no config file at {cfg_path}: run the setup (Start Trading Bot) first")
    return _read_json(cfg_path)


def _data_dir(raw: dict, cfg_path: Path) -> Path:
    """The data folder config.json names (ops.data_dir), read the way the
    scalper, the watchdog and reconcile read it; the config's own folder when
    it names none."""
    ops = raw.get("ops")
    d = ops.get("data_dir") if isinstance(ops, dict) else None
    return Path(str(d)) if d else cfg_path.parent


def scalper_path(config_path: str | Path) -> Path:
    """Where the Rapid Scalper's file is: scalper.json in the data folder
    config.json names, exactly where the scalper itself reads it."""
    cfg_path = Path(config_path)
    return ScalperConfig.default_path(_data_dir(_read_json(cfg_path), cfg_path))


def _write_atomic(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n")
    tmp.replace(path)


def read_modes(config_path: str | Path) -> dict[str, str]:
    """Every bot's mode as the files say it, PAPER where a block or the
    scalper's file says nothing (the bots' own default). A missing or broken
    config.json is an error, never four PAPERs."""
    cfg_path = Path(config_path)
    raw = _read_config(cfg_path)
    out = {}
    for bot in CONFIG_BOTS:
        block = raw.get(bot)
        block = block if isinstance(block, dict) else {}
        out[bot] = str(block.get("mode") or "PAPER").upper()
    sp = ScalperConfig.default_path(_data_dir(raw, cfg_path))
    out["scalper"] = str(_read_json(sp).get("mode") or "PAPER").upper()
    return out


def magics(config_path: str | Path, scalper_file: Optional[Path] = None) -> dict[str, int]:
    """Every bot's magic number as the files say it: Trend & Breakout and the
    three config bots from config.json, the Rapid Scalper from scalper.json.
    These are the numbers the orders will carry."""
    cfg_path = Path(config_path)
    cfg = Config.load(cfg_path)
    s = ScalperConfig.load(scalper_file or scalper_path(cfg_path))
    return {TREND: int(cfg.magic), "scalper": int(s.magic), "runner": int(cfg.runner.magic),
            "bandbreaker": int(cfg.bandbreaker.magic), "crowd": int(cfg.crowd.magic)}


def magic_problems(magic: dict[str, int], targets) -> list[str]:
    """Why each bot in `targets` may NOT go LIVE on its magic number: one plain
    sentence per bot whose number is not positive or is another bot's.
    Empty when every one of them carries its own number."""
    names = labels()
    out = []
    for bot in targets:
        m = int(magic.get(bot) or 0)
        if m <= 0:
            out.append(f"{bot}'s magic number is {m}; a LIVE bot must carry its own positive magic number")
            continue
        for other, om in magic.items():
            if other != bot and int(om) == m:
                out.append(f"{bot}'s magic {m} is {names[other]}'s; a LIVE bot must carry its own magic number")
                break
    return out


def plan_changes(live=(), paper=(), off=()) -> dict[str, str]:
    """{bot: mode} from the three lists; 'all' means every bot. Refuses a
    name it does not know and a bot named twice."""
    out: dict[str, str] = {}
    for mode, names in (("LIVE", live), ("PAPER", paper), ("OFF", off)):
        for name in names or ():
            key = str(name).strip().lower()
            key = ALIASES.get(key, key)
            bots = BOTS if key == "all" else (key,)
            for bot in bots:
                if bot not in BOTS:
                    raise ValueError(f"unknown bot {name!r}: choose from {', '.join(BOTS)} or all")
                if bot in out and out[bot] != mode:
                    raise ValueError(f"{bot} is named for both {out[bot]} and {mode}: say one")
                out[bot] = mode
    return out


def set_modes(config_path: str | Path, changes: dict[str, str]) -> dict[str, str]:
    """Write the modes in `changes` ({bot: mode}) and return every bot's
    resulting mode. Only the "mode" keys change; everything else in
    config.json and scalper.json is kept.

    Every file that will be touched is read and checked first, so a refusal
    means nothing was written: config.json must exist and be readable
    whichever bot is named (scalper.json lives in the folder it names);
    scalper.json, when the scalper is named, must be readable; and a bot
    going LIVE must carry its own magic number."""
    cfg_path = Path(config_path)
    unknown = [b for b in changes if b not in BOTS]
    if unknown:
        raise ValueError(f"unknown bot(s): {', '.join(unknown)}; choose from {', '.join(BOTS)} or all")
    modes = {b: str(m).upper() for b, m in changes.items()}
    bad = [m for m in modes.values() if m not in MODES]
    if bad:
        raise ValueError(f"a mode must be one of {', '.join(MODES)}, not {', '.join(bad)}")
    # read and check everything first ...
    raw = _read_config(cfg_path)
    sp = ScalperConfig.default_path(_data_dir(raw, cfg_path))
    sraw = _read_json(sp) if "scalper" in modes else None
    going_live = [b for b, m in modes.items() if m == "LIVE"]
    if going_live:
        problems = magic_problems(magics(cfg_path, sp), going_live)
        if problems:
            raise ValueError("; ".join(problems))
    # ... then write
    in_config = {b: m for b, m in modes.items() if b in CONFIG_BOTS}
    for bot, mode in in_config.items():
        block = raw.get(bot)
        if not isinstance(block, dict):
            block = {}
            raw[bot] = block
        block["mode"] = mode
    written: list[Path] = []
    try:
        if in_config:
            _write_atomic(cfg_path, raw)
            written.append(cfg_path)
        if sraw is not None:
            sraw["mode"] = modes["scalper"]
            _write_atomic(sp, sraw)
            written.append(sp)
    except OSError as exc:
        if written:
            left = sp if sraw is not None and sp not in written else cfg_path
            raise PartialWrite(f"{exc}; {', '.join(str(p) for p in written)} already holds the change, "
                               f"{left} does not: check with --show") from exc
        raise
    return read_modes(cfg_path)


def describe(modes: dict[str, str], magic: Optional[dict[str, int]] = None,
             scalper_file: Optional[Path] = None) -> str:
    """One plain line per bot: its mode, what that means, its magic number
    when known, and where it is set."""
    names = labels()
    where = {bot: f"config.json: {bot}.mode" for bot in CONFIG_BOTS}
    where["scalper"] = f"{scalper_file}: mode" if scalper_file else "scalper.json: mode"
    lines = []
    for bot in BOTS:
        mode = str(modes.get(bot) or "PAPER").upper()
        meaning = WHAT.get(mode, "not a mode the bot knows: it treats this as OFF")
        tag = f"magic {magic[bot]}; " if magic and bot in magic else ""
        lines.append(f"{names[bot] + ':':<17}{mode:<7}{meaning}  ({tag}{where[bot]})")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.ops.modes",
                                 description="switch the Momentum Runner, Band Breaker, Crowd Fader or Rapid Scalper "
                                             "between OFF, PAPER and LIVE; the change applies on the next start")
    ap.add_argument("--config", required=True,
                    help="config.json (scalper.json is read from the data folder it names, ops.data_dir)")
    ap.add_argument("--live", nargs="+", metavar="BOT", default=[], help="real orders: runner bandbreaker crowd scalper, or all")
    ap.add_argument("--paper", nargs="+", metavar="BOT", default=[], help="simulate only (its own tab, never in Overall)")
    ap.add_argument("--off", nargs="+", metavar="BOT", default=[], help="do nothing")
    ap.add_argument("--show", action="store_true", help="print every bot's mode and change nothing")
    a = ap.parse_args(argv)
    try:
        changes = plan_changes(a.live, a.paper, a.off)
    except ValueError as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 2
    if not changes and not a.show:
        ap.print_help()
        return 2
    try:
        modes = set_modes(a.config, changes) if changes else read_modes(a.config)
        sp = scalper_path(a.config)
        mg = magics(a.config, sp)
    except PartialWrite as exc:
        print(f"Stopped part way: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 1
    print(describe(modes, mg, sp))
    if changes:
        print(RESTART)
    else:
        print("Change a mode with --live, --paper or --off (a bot name, or all), then restart the bot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
