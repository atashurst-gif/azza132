"""Switch any bot between OFF, PAPER and LIVE from the terminal.

    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --show
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --live all
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper runner crowd
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --off bandbreaker
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --live rider --gbp-per-pip 1.0
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --ian-feed databento
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --check rider
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper tnb
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --check tnb

Bots: rider (Rapid Momentum Rider), runner (Momentum Runner), bandbreaker
(Band Breaker), crowd (Crowd Fader), ian (Financial Ian), or all - "all" is
these five, the active bots. The Momentum Runner, Band Breaker and Crowd
Fader keep their mode in config.json, each in its own block ("runner",
"bandbreaker", "crowd" -> "mode"); the Rapid Momentum Rider and Financial
Ian keep their own files, rider.json and ian.json, in the data folder
config.json names (ops.data_dir), which is where the bots and the watchdog
read them.

Trend & Breakout (tnb; also trend, trend_breakout, market_intelligence) is
named on its own, never by "all". It keeps its mode in config.json too
("tnb" -> "mode"), LIVE when the block says nothing. It is LIVE or PAPER,
never OFF: it picks the Momentum Runner's entries, so OFF is refused with a
plain message. On PAPER it keeps deciding on real prices and its orders are
simulated (its paper record is data/tnb_paper.sqlite); the Momentum Runner
still rides its index entries.

The Rapid Scalper (scalper.json) is RETIRED: the Rapid Momentum Rider
replaced it on 9 Oct and it is no longer started. It can still be switched
OFF; PAPER and LIVE are refused with a plain message. Its records and its
magic number are kept, so its real trades still count on the page.

Everything else in every file is left exactly as it was, and each file is
written whole under a temporary name and renamed into place, so a crash can
never leave half a file behind. Every file that will be touched is read and
checked BEFORE anything is written: a missing or broken config.json, or a
broken rider.json, ian.json or scalper.json, means nothing changes at all.

On the way to LIVE the bot's magic number is checked: it must be positive
and its own, not Trend & Breakout's, the scalper's or another bot's. The
magic number is the only thing that keeps one bot from seeing or touching
another's positions, so a bot whose number is not its own is refused. The
Rapid Momentum Rider's and Financial Ian's numbers are fixed in their own
code (990811 and 990911); a "magic" written in ian.json is ignored, by Ian
and here alike.

Two settings ride along with the modes: --gbp-per-pip sets the Rapid
Momentum Rider's exposure (rider.json "user_pip_value_gbp", Aaron's
setting; the bot itself never changes it), and --ian-feed names Financial
Ian's institutional data feed (ian.json "feed" -> "vendor": none or
databento; the key itself lives in secrets.json, never here).

--check BOT prints that bot's health in one plain line from its own status
file, for the one-click scripts: exit 0 when it is running and reporting,
2 when its file says OFF, 1 otherwise. For tnb the line names its mode and
what that means for the Momentum Runner, from config.json and the trader's
own heartbeat.

A change takes effect on the next start: Stop Trading Bot, then Start
Trading Bot. The Start window prints every bot's mode at the end. (A bot in
its own file that is switched ON from OFF is started by the watchdog within
a minute.)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Optional

from ..config import Config
from ..scalper import EXISTING_STRATEGY_LABEL
from ..scalper.config import ScalperConfig
from .watchdog import own_file_mode

MODES = ("OFF", "PAPER", "LIVE")
TNB = "tnb"                                                  # Trend & Breakout: config.json "tnb" -> "mode"
TNB_MODES = ("LIVE", "PAPER")                                # never OFF: it picks the Runner's entries
TNB_DEFAULT = "LIVE"                                         # no block, or no mode in it
CONFIG_BOTS = ("runner", "bandbreaker", "crowd")            # blocks in config.json (PAPER when they say nothing)
FILE_BOTS = ("rider", "ian")                                 # their own files in the data folder
ACTIVE = ("rider",) + CONFIG_BOTS + ("ian",)                 # what "all" means, in the page's order
RETIRED = ("scalper",)                                       # scalper.json: OFF only
BOTS = (TNB,) + ACTIVE + RETIRED
FILES = {"rider": "rider.json", "ian": "ian.json", "scalper": "scalper.json"}
# the bots' strategy ids and the names the docs use, as spellings of the same bot
ALIASES = {"momentum_runner": "runner", "momentum-runner": "runner",
           "band_breaker": "bandbreaker", "band-breaker": "bandbreaker",
           "crowd_fader": "crowd", "crowd-fader": "crowd",
           "rapid_scalper": "scalper", "rapid-scalper": "scalper",
           "momentum_rider": "rider", "momentum-rider": "rider",
           "rapid_momentum_rider": "rider", "rapid-momentum-rider": "rider",
           "financial_ian": "ian", "financial-ian": "ian",
           "trend": TNB, "trend_breakout": TNB, "trend-breakout": TNB, "trend_and_breakout": TNB,
           "market_intelligence": TNB, "market-intelligence": TNB}
TREND = TNB                                                 # the key Trend & Breakout's magic is listed under
TNB_OFF_REFUSAL = "Trend & Breakout picks the Momentum Runner's entries; put it on PAPER instead"
TNB_PAPER_DB = "tnb_paper.sqlite"
WHAT = {"LIVE": "real orders on the account, under its own magic number; the broker's figures are the record",
        "PAPER": "simulated; its results are on its own tab only, never in Overall",
        "OFF": "does nothing"}
TNB_WHAT = {"LIVE": "real orders on the account, under its own magic number; the Momentum Runner rides its index "
                    "entries",
            "PAPER": "real prices, simulated orders (its paper record: tnb_paper.sqlite); practice only, never in "
                     "Overall; the Momentum Runner still rides its index entries"}
RETIRED_WHAT = "retired 9 Oct (replaced by the Rapid Momentum Rider): never started; its records are kept"
RETIRED_REFUSAL = ("the Rapid Scalper is retired (the Rapid Momentum Rider replaced it on 9 Oct) and is no "
                   "longer started, so it can only be switched OFF; for fast FX trading use: --live rider")
RESTART = "Restart the bot for this to take effect (Stop Trading Bot, then Start Trading Bot)."
MAX_GBP_PER_PIP = 10.0                                      # a typo guard; the file can still be edited by hand
IAN_FEEDS = ("none", "databento")
STATUS_STALE_SECONDS = 120.0


class PartialWrite(OSError):
    """A write failed after another file had already been written: the
    message says exactly which file holds the change and which does not."""


def labels() -> dict[str, str]:
    from ..runner import STRATEGY_LABEL as RUNNER
    from ..bandbreaker import STRATEGY_LABEL as BAND
    from ..crowd import STRATEGY_LABEL as CROWD
    from ..scalper import STRATEGY_LABEL as SCALPER
    from ..rider import STRATEGY_LABEL as RIDER
    from ..ian import STRATEGY_LABEL as IAN
    return {"runner": RUNNER, "bandbreaker": BAND, "crowd": CROWD, "scalper": SCALPER, "rider": RIDER,
            "ian": IAN, TNB: EXISTING_STRATEGY_LABEL}


def tnb_mode_of(raw: dict) -> str:
    """Trend & Breakout's mode from config.json's object: PAPER only when its
    block says PAPER; LIVE otherwise (no block, no mode, anything else)."""
    block = raw.get(TNB) if isinstance(raw, dict) else None
    mode = str((block.get("mode") if isinstance(block, dict) else "") or TNB_DEFAULT).strip().upper()
    return "PAPER" if mode == "PAPER" else TNB_DEFAULT


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
    bots, the watchdog and reconcile read it; the config's own folder when
    it names none."""
    ops = raw.get("ops")
    d = ops.get("data_dir") if isinstance(ops, dict) else None
    return Path(str(d)) if d else cfg_path.parent


def bot_file(config_path: str | Path, bot: str) -> Path:
    """Where a bot with its own file keeps it: rider.json, ian.json or
    scalper.json in the data folder config.json names, exactly where the
    bot itself reads it."""
    cfg_path = Path(config_path)
    return _data_dir(_read_json(cfg_path), cfg_path) / FILES[bot]


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
    """Every bot's mode as the files say it, PAPER where a block or a bot's
    own file says nothing (the bots' own default) - except Trend & Breakout,
    which is LIVE unless its block says PAPER. A missing or broken
    config.json is an error, never a column of PAPERs."""
    cfg_path = Path(config_path)
    raw = _read_config(cfg_path)
    data = _data_dir(raw, cfg_path)
    out = {}
    for bot in BOTS:
        if bot == TNB:
            out[bot] = tnb_mode_of(raw)
        elif bot in CONFIG_BOTS:
            block = raw.get(bot)
            block = block if isinstance(block, dict) else {}
            out[bot] = str(block.get("mode") or "PAPER").upper()
        elif bot == "scalper":
            out[bot] = str(_read_json(data / FILES[bot]).get("mode") or "PAPER").upper()
        else:
            out[bot] = own_file_mode(data / FILES[bot])
    return out


def _ian_magic(data: Optional[Path] = None) -> int:
    """Financial Ian's magic number: always ``mintel.ian.MAGIC`` (990911).
    Ian's engine is hard-wired to it and ignores any "magic" in ian.json
    (it only logs a warning), so the file is not read here either: a number
    written there is never the one Ian's orders carry. ``data`` is kept for
    the callers' sake and not used."""
    from ..ian import MAGIC as IAN_MAGIC
    return int(IAN_MAGIC)


def magics(config_path: str | Path, scalper_file: Optional[Path] = None) -> dict[str, int]:
    """Every bot's magic number as the bots really use it: Trend & Breakout
    and the three config bots from config.json, the Rapid Scalper from
    scalper.json, the Rapid Momentum Rider and Financial Ian their own fixed
    numbers (``mintel.rider.MAGIC``, ``mintel.ian.MAGIC``; Ian ignores any
    "magic" in ian.json). These are the numbers the orders will carry."""
    from ..rider import MAGIC as RIDER_MAGIC
    cfg_path = Path(config_path)
    cfg = Config.load(cfg_path)
    s = ScalperConfig.load(scalper_file or scalper_path(cfg_path))
    return {TNB: int(cfg.magic), "scalper": int(s.magic), "runner": int(cfg.runner.magic),
            "bandbreaker": int(cfg.bandbreaker.magic), "crowd": int(cfg.crowd.magic),
            "rider": int(RIDER_MAGIC), "ian": _ian_magic()}


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
    """{bot: mode} from the three lists; 'all' means every active bot (not
    the retired scalper, and not Trend & Breakout, which is named on its
    own). Refuses a name it does not know, a bot named twice, the retired
    scalper for anything but OFF, and Trend & Breakout for OFF."""
    out: dict[str, str] = {}
    for mode, names in (("LIVE", live), ("PAPER", paper), ("OFF", off)):
        for name in names or ():
            key = str(name).strip().lower()
            key = ALIASES.get(key, key)
            bots = ACTIVE if key == "all" else (key,)
            for bot in bots:
                if bot not in BOTS:
                    raise ValueError(f"unknown bot {name!r}: choose from {TNB}, {', '.join(ACTIVE)} or all")
                if bot in RETIRED and mode != "OFF":
                    raise ValueError(RETIRED_REFUSAL)
                if bot == TNB and mode not in TNB_MODES:
                    raise ValueError(TNB_OFF_REFUSAL)
                if bot in out and out[bot] != mode:
                    raise ValueError(f"{bot} is named for both {out[bot]} and {mode}: say one")
                out[bot] = mode
    return out


def _check_gbp_per_pip(value) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"the GBP per pip must be a number, not {value!r}") from None
    if not (0 < v <= MAX_GBP_PER_PIP):
        raise ValueError(f"the GBP per pip must be more than 0 and at most {MAX_GBP_PER_PIP:g} (it was {v:g}); "
                         f"a bigger figure can only be typed into rider.json by hand")
    return v


def set_modes(config_path: str | Path, changes: dict[str, str], *, gbp_per_pip: Optional[float] = None,
              ian_feed: Optional[str] = None) -> dict[str, str]:
    """Write the modes in `changes` ({bot: mode}), and the two settings when
    given, and return every bot's resulting mode. Only those keys change;
    everything else in config.json, rider.json, ian.json and scalper.json
    is kept.

    Every file that will be touched is read and checked first, so a refusal
    means nothing was written: config.json must exist and be readable
    whichever bot is named (the bots' own files live in the folder it
    names); a bot's own file, when it is named, must be readable; the
    retired scalper can only go OFF; Trend & Breakout can only be LIVE or
    PAPER; and a bot going LIVE must carry its own magic number."""
    cfg_path = Path(config_path)
    unknown = [b for b in changes if b not in BOTS]
    if unknown:
        raise ValueError(f"unknown bot(s): {', '.join(unknown)}; choose from {TNB}, {', '.join(ACTIVE)} or all")
    modes = {b: str(m).upper() for b, m in changes.items()}
    bad = [m for m in modes.values() if m not in MODES]
    if bad:
        raise ValueError(f"a mode must be one of {', '.join(MODES)}, not {', '.join(bad)}")
    if any(b in RETIRED and m != "OFF" for b, m in modes.items()):
        raise ValueError(RETIRED_REFUSAL)
    if TNB in modes and modes[TNB] not in TNB_MODES:
        raise ValueError(TNB_OFF_REFUSAL)
    pip = _check_gbp_per_pip(gbp_per_pip) if gbp_per_pip is not None else None
    feed = None
    if ian_feed is not None:
        feed = str(ian_feed).strip().lower()
        if feed not in IAN_FEEDS:
            raise ValueError(f"Financial Ian's feed must be one of {', '.join(IAN_FEEDS)}, not {ian_feed!r}")
    # read and check everything first ...
    raw = _read_config(cfg_path)
    data = _data_dir(raw, cfg_path)
    own: dict[str, dict] = {}
    for bot in FILES:
        if bot in modes or (bot == "rider" and pip is not None) or (bot == "ian" and feed is not None):
            own[bot] = _read_json(data / FILES[bot])
    going_live = [b for b, m in modes.items() if m == "LIVE"]
    if going_live:
        problems = magic_problems(magics(cfg_path, data / FILES["scalper"]), going_live)
        if problems:
            raise ValueError("; ".join(problems))
    # ... then write
    in_config = {b: m for b, m in modes.items() if b in CONFIG_BOTS or b == TNB}
    for bot, mode in in_config.items():
        block = raw.get(bot)
        if not isinstance(block, dict):
            block = {}
            raw[bot] = block
        block["mode"] = mode
    for bot, obj in own.items():
        if bot in modes:
            obj["mode"] = modes[bot]
    if pip is not None:
        own["rider"]["user_pip_value_gbp"] = pip
    if feed is not None:
        f = own["ian"].get("feed")
        f = dict(f) if isinstance(f, dict) else {}
        f["vendor"] = feed
        own["ian"]["feed"] = f
    todo: list[tuple[Path, dict]] = []
    if in_config:
        todo.append((cfg_path, raw))
    todo += [(data / FILES[bot], obj) for bot, obj in own.items()]
    written: list[Path] = []
    try:
        for path, obj in todo:
            _write_atomic(path, obj)
            written.append(path)
    except OSError as exc:
        if written:
            left = [str(p) for p, _ in todo if p not in written]
            raise PartialWrite(f"{exc}; {', '.join(str(p) for p in written)} already holds the change, "
                               f"{', '.join(left)} does not: check with --show") from exc
        raise
    return read_modes(cfg_path)


def describe(modes: dict[str, str], magic: Optional[dict[str, int]] = None,
             scalper_file: Optional[Path] = None, files: Optional[dict[str, Path]] = None) -> str:
    """One plain line per bot: its mode, what that means, its magic number
    when known, and where it is set."""
    names = labels()
    where = {bot: f"config.json: {bot}.mode" for bot in CONFIG_BOTS + (TNB,)}
    for bot, name in FILES.items():
        path = (files or {}).get(bot) or (scalper_file if bot == "scalper" else None)
        where[bot] = f"{path}: mode" if path else f"{name}: mode"
    lines = []
    for bot in BOTS:
        if bot == TNB and bot not in modes:
            continue                                # a caller that never read it: no line rather than a guess
        mode = str(modes.get(bot) or "PAPER").upper()
        meaning = WHAT.get(mode, "not a mode the bot knows: it treats this as OFF")
        name = names[bot]
        if bot == TNB:
            meaning = TNB_WHAT.get(mode, TNB_WHAT[TNB_DEFAULT])
        if bot in RETIRED:
            name, meaning = f"{name} (retired)", RETIRED_WHAT
        tag = f"magic {magic[bot]}; " if magic and bot in magic else ""
        lines.append(f"{name + ':':<27}{mode:<7}{meaning}  ({tag}{where[bot]})")
    return "\n".join(lines)


# ----------------------------------------------------------- health line --
def _age_seconds(stamp, now: dt.datetime) -> Optional[float]:
    try:
        t = dt.datetime.fromisoformat(str(stamp))
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return (now - t).total_seconds()
    except Exception:
        return None


def _heartbeat(data: Path, name: str) -> dict:
    try:
        return json.loads((data / "heartbeats" / f"{name}.heartbeat.json").read_text())
    except Exception:
        return {}


def bot_health(config_path: str | Path, bot: str, now: Optional[dt.datetime] = None) -> tuple[int, str]:
    """(code, one plain line) on how the Rapid Momentum Rider or Financial
    Ian is doing, from its own status file: 0 running and reporting, 2 its
    file says OFF, 1 anything else (not started yet, stopped, stale)."""
    key = ALIASES.get(str(bot).strip().lower(), str(bot).strip().lower())
    if key not in FILE_BOTS and key != TNB:
        raise ValueError(f"--check works for {TNB}, {' and '.join(FILE_BOTS)}, not {bot!r}")
    cfg_path = Path(config_path)
    raw = _read_config(cfg_path)
    if key == TNB:
        return tnb_line(_data_dir(raw, cfg_path), raw, now)
    return health_line(_data_dir(raw, cfg_path), key, now)


def tnb_line(data_dir: str | Path, raw: dict, now: Optional[dt.datetime] = None) -> tuple[int, str]:
    """(code, one plain line) on Trend & Breakout: its mode as config.json
    says it, what that means for the Momentum Runner, and whether the trader
    is reporting (its own heartbeat). 0 reporting, 1 anything else; it is
    never OFF, so never 2."""
    now = now or dt.datetime.now(dt.timezone.utc)
    mode = tnb_mode_of(raw)
    runner = raw.get("runner") if isinstance(raw.get("runner"), dict) else {}
    runner_mode = str(runner.get("mode") or "PAPER").upper()
    if runner.get("enabled") is False:
        runner_mode = "OFF"
    what = "real prices, simulated orders" if mode == "PAPER" else "real orders on the account"
    if runner_mode == "OFF":
        rides = "the Momentum Runner is OFF"
    else:
        rides = "the Momentum Runner rides its index entries" + ("" if runner_mode == "LIVE" else f" ({runner_mode})")
    line = f"{labels()[TNB].upper()} - {mode} ({what}), {rides}"
    beat = _heartbeat(Path(data_dir), "strategy")
    age = _age_seconds(beat.get("ts_utc"), now) if beat else None
    waiting = (beat.get("detail") or {}).get("waiting") if beat else None
    if age is None:
        return 1, f"{line} - NOT STARTED YET (no report from the trader yet)"
    if age > STATUS_STALE_SECONDS:
        return 1, f"{line} - NOT RUNNING (last report {age / 60:.0f} min ago)"
    if waiting:
        return 1, f"{line} - STARTING: {waiting} (attempt {(beat.get('detail') or {}).get('attempt', 1)})"
    return 0, line


def health_line(data_dir: str | Path, key: str, now: Optional[dt.datetime] = None) -> tuple[int, str]:
    """bot_health from the data folder itself (the status page uses this)."""
    now = now or dt.datetime.now(dt.timezone.utc)
    data = Path(data_dir)
    title = labels()[key].upper()
    mode = own_file_mode(data / FILES[key])
    if mode == "OFF":
        return 2, f"{title} - OFF ({FILES[key]} says OFF, so it is not started)"
    status_name = "rider-status.json" if key == "rider" else "ian-status.json"
    if key == "rider":
        try:
            sn = json.loads((data / FILES[key]).read_text()).get("status_file")
            status_name = str(sn) if isinstance(sn, str) and sn else status_name
        except Exception:
            pass
    try:
        st = json.loads((data / status_name).read_text())
        if not isinstance(st, dict):
            st = None
    except Exception:
        st = None
    beat = _heartbeat(data, key)
    if st is None:
        waiting = (beat.get("detail") or {}).get("waiting") if beat else None
        age = _age_seconds(beat.get("ts_utc"), now) if beat else None
        if waiting and age is not None and age <= STATUS_STALE_SECONDS:
            return 1, f"{title} - STARTING, {mode}: {waiting} (attempt {(beat.get('detail') or {}).get('attempt', 1)})"
        return 1, f"{title} - NOT STARTED YET, set to {mode} (no report from it yet)"
    age = _age_seconds(st.get("updated_utc") or st.get("updated"), now)
    run_mode = str(st.get("mode") or mode).upper()
    if age is None or age > STATUS_STALE_SECONDS:
        when = "never" if age is None else f"{age / 60:.0f} min ago"
        return 1, f"{title} - NOT RUNNING, set to {mode} (last report {when})"
    restart = ""
    if run_mode != mode:
        try:
            file_pid = int((data / f"{key}.pid").read_text().strip())
        except Exception:
            file_pid = None
        status_pid = st.get("pid")
        if isinstance(status_pid, int) and file_pid is not None and status_pid != file_pid:
            # this report is from the copy that was stopped; a new one is starting
            return 1, f"{title} - STARTING, set to {mode} (a new copy has started; its first report is not in yet)"
        restart = f" (running {run_mode}: restart it for {mode})"
    if key == "rider":
        h = st.get("health") or {}
        n = len(st.get("scanner") or [])
        pip = st.get("user_pip_value_gbp")
        pip_txt = f", GBP {float(pip):.2f} per pip" if isinstance(pip, (int, float)) else ""
        if h.get("entries_allowed"):
            return 0, f"{title} - HEALTHY, {run_mode}, scanning {n} markets{pip_txt}{restart}"
        return 0, (f"{title} - RUNNING, {run_mode}, scanning {n} markets{pip_txt}{restart} - "
                   f"{h.get('summary') or 'new entries paused'}")
    feed = st.get("feed") or {}
    state = str(feed.get("state") or "UNKNOWN")
    bits = [f"{title} - HEALTHY, {run_mode}, feed {state}"]
    if state == "NOT CONFIGURED":
        bits.append(" (no signals and no trades until a data-feed key is saved)")
    elif state != "LIVE" and feed.get("reason"):
        bits.append(f" ({feed.get('reason')})")
    if not (st.get("mt5") or {}).get("connected"):
        bits.append(", MetaTrader not connected")
    return 0, "".join(bits) + restart


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.ops.modes",
                                 description="switch the Rapid Momentum Rider, Momentum Runner, Band Breaker, Crowd "
                                             "Fader or Financial Ian between OFF, PAPER and LIVE, and Trend & "
                                             "Breakout (tnb) between LIVE and PAPER; the change applies on the next "
                                             "start")
    ap.add_argument("--config", required=True,
                    help="config.json (rider.json, ian.json and scalper.json are read from the data folder it names, "
                         "ops.data_dir)")
    ap.add_argument("--live", nargs="+", metavar="BOT", default=[],
                    help="real orders: tnb rider runner bandbreaker crowd ian, or all (all = every bot but tnb)")
    ap.add_argument("--paper", nargs="+", metavar="BOT", default=[], help="simulate only (its own tab, never in Overall)")
    ap.add_argument("--off", nargs="+", metavar="BOT", default=[],
                    help="do nothing (the retired scalper: OFF only; tnb is never OFF)")
    ap.add_argument("--gbp-per-pip", type=float, default=None, metavar="GBP",
                    help="the Rapid Momentum Rider's exposure in GBP per pip (rider.json user_pip_value_gbp)")
    ap.add_argument("--ian-feed", default=None, choices=IAN_FEEDS,
                    help="Financial Ian's institutional data feed (the key itself goes in secrets.json)")
    ap.add_argument("--show", action="store_true", help="print every bot's mode and change nothing")
    ap.add_argument("--check", default="", metavar="BOT",
                    help="one line on how tnb, rider or ian is doing; changes nothing")
    a = ap.parse_args(argv)
    if a.check:
        try:
            code, line = bot_health(a.config, a.check)
        except (OSError, ValueError) as exc:
            print(f"Cannot check: {exc}", file=sys.stderr)
            return 1
        print(line)
        return code
    try:
        changes = plan_changes(a.live, a.paper, a.off)
    except ValueError as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 2
    settings = a.gbp_per_pip is not None or a.ian_feed is not None
    if not changes and not settings and not a.show:
        ap.print_help()
        return 2
    try:
        if changes or settings:
            modes = set_modes(a.config, changes, gbp_per_pip=a.gbp_per_pip, ian_feed=a.ian_feed)
        else:
            modes = read_modes(a.config)
        files = {bot: bot_file(a.config, bot) for bot in FILES}
        mg = magics(a.config, files["scalper"])
    except PartialWrite as exc:
        print(f"Stopped part way: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 1
    print(describe(modes, mg, files=files))
    if a.gbp_per_pip is not None:
        print(f"Rapid Momentum Rider exposure: GBP {a.gbp_per_pip:.2f} per pip ({files['rider']}: user_pip_value_gbp)")
    if a.ian_feed is not None:
        print(f"Financial Ian data feed: {a.ian_feed} ({files['ian']}: feed.vendor)")
    if changes or settings:
        print(RESTART)
    else:
        print("Change a mode with --live, --paper or --off (a bot name, or all), then restart the bot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
