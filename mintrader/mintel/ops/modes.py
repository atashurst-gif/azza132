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
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --tnb-live-groups FX_MINOR
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --tnb-live-groups none
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --risk-pct 0.7 --risk-money 13
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --tnb-live-tactics MOMENTUM_CONTINUATION
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --tnb-live-tactics all
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --top-size off
    python -m mintel.ops.modes --config ~/MarketBot/data/config.json --news-before-minutes 15

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

Trend & Breakout on PAPER can keep some kinds of market LIVE:
--tnb-live-groups FX_MINOR (9 Oct, Aaron: "Just Formula 1") sends its orders
on minor currency pairs (EURGBP, AUDJPY and the like) to the real broker
under its own magic number, while everything else stays on paper and still
feeds the Momentum Runner. It writes config.json "tnb" -> "live_groups" and,
when the set of kinds changes, "live_since_utc" (now, UTC); the same set
again keeps its time. The kinds are the ones the trader sorts markets into
(FX_MINOR, FX_MAJOR, FX_EXOTIC, INDEX, GOLD, SILVER, METAL, ENERGY, EQUITY,
CRYPTO; FX, METALS and INDICES name several). It puts Trend & Breakout on
PAPER when given alone, and is refused with --live tnb (LIVE already sends
everything). --tnb-live-groups none clears both keys: every order on paper.
The trader reads them at its start, so a restart applies them.

--risk-pct and --risk-money set what a Trend & Breakout trade risks:
config.json "risk" -> "base_risk_pct" (percent of the account at normal
confidence; it must sit between risk.min_risk_pct and risk.max_risk_pct) and
"max_risk_money" (the most any one trade may lose at its stop, more than 0).
9 Oct, Aaron: "About £13 a trade" - 0.7 and 13. Each can be given alone; no
other risk setting is ever touched. Only Trend & Breakout sizes from these
two: the Momentum Runner copies the size Trend & Breakout would have had
at its old 0.5% (runner.copy_risk_pct, 10 Oct), held to its own
runner.max_risk_money, and the other bots size from their own settings.

Three switches for the questions put to Aaron on 10 Oct (each default keeps
what runs today; the answer is applied by one of these alone):

--tnb-live-tactics MOMENTUM_CONTINUATION [more] chooses which approaches go
real on the markets Trend & Breakout keeps LIVE (config.json "tnb" ->
"live_tactics"): any other approach there stays on paper (practice), and so
does an order that does not say its approach. "all" clears it: every
approach on a live market goes real, as set on 9 Oct. It needs Trend &
Breakout on PAPER with markets kept LIVE (in the file, or --tnb-live-groups
in the same call); an approach it does not know is refused. When the set of
live trades changes, tnb.live_since_utc starts again from now: a narrower
(or wider) scope is a new trial, and the page counts its live trades from
then.

--top-size on|off sets risk.top_size_enabled: off, every Trend & Breakout
trade - Formula 1's included - is sized by --risk-pct / --risk-money (about
13 a trade); on, momentum continuation on minor pairs may take the
top-opportunity size when its own record passes. Nothing else in risk.top_*
changes, nor the Momentum Runner's own top size.

--news-before-minutes N (at least 0.5, at most 120) sets
news.blackout_before_seconds to N x 60: no new Trend & Breakout entry,
practice or real, in the N minutes before a high-importance release
(news.min_importance_for_blackout) on either currency of the pair. 90
seconds until 10 Oct. The other bots have their own news rules.

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
import math
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
# The kinds of market the trader sorts every market into (contracts.infer_group,
# as broker.paper.market_kind and RiskManager.market_kind read it) that can be
# kept LIVE while Trend & Breakout is on PAPER; plain names for the output.
LIVE_KINDS = {"FX_MINOR": "minor currency pairs", "FX_MAJOR": "major currency pairs",
              "FX_EXOTIC": "exotic currency pairs", "INDEX": "indices", "GOLD": "gold", "SILVER": "silver",
              "METAL": "other metals", "ENERGY": "oil and energy", "EQUITY": "shares", "CRYPTO": "crypto"}
NO_LIVE_GROUPS = "none"
# 9 Oct, Aaron: "Just Formula 1" - T&B's momentum continuation on FX minor
# pairs; every T&B trade on a minor pair goes live, whatever its approach.
FORMULA_1 = frozenset({"FX_MINOR"})
TNB_LIVE_WITH_LIVE_REFUSAL = ("--tnb-live-groups keeps some markets LIVE while Trend & Breakout is on PAPER; "
                              "with --live tnb every order is real already, so say one or the other")
# 10 Oct: the three switches for Aaron's pending decisions (asked 10 Oct)
ALL_TACTICS = "all"                                         # --tnb-live-tactics all: every approach (9 Oct)
F1_TACTIC = "MOMENTUM_CONTINUATION"                         # Formula 1's own approach
TNB_TACTICS_WITH_LIVE_REFUSAL = ("--tnb-live-tactics chooses which approaches go real on the markets Trend & "
                                 "Breakout keeps LIVE while it is on PAPER; with --live tnb every order is real "
                                 "already, so say one or the other")
TNB_TACTICS_NOTHING_LIVE = ("--tnb-live-tactics chooses which approaches go real on the markets Trend & Breakout "
                            "keeps LIVE while it is on PAPER, and none is kept LIVE: set --tnb-live-groups FX_MINOR "
                            "(with --paper tnb if it is LIVE) first or in the same call")
NEWS_MAX_MINUTES = 120.0                                    # a typo guard on --news-before-minutes
# 10 Oct review: "more than 0" let 1e-9 through, stored as 0 seconds - no
# blackout at all. Half a minute is the least it takes.
NEWS_MIN_MINUTES = 0.5


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


# -------------------------------------------- Trend & Breakout's live kinds --
def check_live_kinds(names) -> tuple[str, ...]:
    """The kinds of market named for --tnb-live-groups, each checked and
    spelled as the trader spells it (FX, METALS and INDICES name several, as
    the paper broker reads them), in the order given, once each; () for
    "none". A kind the trader never uses is refused, so nothing is written."""
    from ..broker.paper import GROUP_ALIASES
    words = [str(n).strip().upper().replace("-", "_").replace(" ", "_") for n in (names or ())]
    words = [w for w in words if w]
    if not words:
        raise ValueError("name at least one kind of market for --tnb-live-groups (FX_MINOR for Formula 1), or none")
    if NO_LIVE_GROUPS.upper() in words:
        if len(words) > 1:
            raise ValueError("--tnb-live-groups none puts every market back on paper; name it on its own")
        return ()
    out: list[str] = []
    bad: list[str] = []
    for w in words:
        kinds = GROUP_ALIASES.get(w, (w,))
        if any(k not in LIVE_KINDS for k in kinds):
            bad.append(w)
            continue
        out += [k for k in kinds if k not in out]
    if bad:
        raise ValueError(f"unknown kind of market {', '.join(bad)}: choose from {', '.join(LIVE_KINDS)} "
                         f"(FX, METALS or INDICES for several), or none")
    return tuple(out)


def tnb_live_of(raw: dict) -> tuple[tuple[str, ...], str]:
    """(kinds, since) from config.json's object: the kinds of market
    Trend & Breakout keeps LIVE while on PAPER, spelled as the trader reads
    them (an alias expanded, a kind it does not know kept as written), and
    when they were switched on (ISO UTC, or "" when not recorded)."""
    from ..broker.paper import GROUP_ALIASES
    from ..config import utc_iso_or_blank
    block = raw.get(TNB) if isinstance(raw, dict) else None
    block = block if isinstance(block, dict) else {}
    groups = block.get("live_groups")
    if isinstance(groups, str):
        groups = [groups]
    out: list[str] = []
    for g in groups if isinstance(groups, (list, tuple)) else ():
        key = str(g or "").strip().upper()
        out += [k for k in GROUP_ALIASES.get(key, (key,) if key else ()) if k not in out]
    since = utc_iso_or_blank(block.get("live_since_utc")) if out else ""
    return tuple(out), since or ""


def kinds_in_words(kinds) -> str:
    """"minor currency pairs", "indices and gold", ... for the output."""
    words = [LIVE_KINDS.get(k, k.lower().replace("_", " ")) for k in kinds]
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " and " + words[-1]


def to_utc_seconds(now: Optional[dt.datetime] = None) -> str:
    """``now`` (or the clock) as ISO UTC to the second, as tnb.live_since_utc holds it."""
    t = now or dt.datetime.now(dt.timezone.utc)
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def _since_words(since: str) -> str:
    try:
        return "since " + dt.datetime.fromisoformat(since).astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError):
        return "since a time not recorded"


def tactics_in_words(tactics) -> str:
    """"momentum continuation", "momentum continuation and trend pullback"."""
    words = [str(t).lower().replace("_", " ") for t in tactics]
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " and " + words[-1]


def tnb_live_phrase(kinds, since: str = "", tactics=()) -> str:
    """"minor currency pairs LIVE (Formula 1) since 2026-10-09 18:45 UTC";
    with tnb.live_tactics set (10 Oct) "minor currency pairs LIVE for
    momentum continuation only (Formula 1) since ..."."""
    tactics = tuple(tactics or ())
    f1 = (" (Formula 1)" if frozenset(kinds) == FORMULA_1 and (not tactics or set(tactics) == {F1_TACTIC})
          else "")
    only = f" for {tactics_in_words(tactics)} only" if tactics else ""
    return f"{kinds_in_words(kinds)} LIVE{only}{f1} {_since_words(since)}"


def tnb_paper_live_meaning(kinds, since: str = "", tactics=()) -> str:
    """What PAPER with live kinds means, for --show."""
    runner = ("its index orders are real, so the Momentum Runner gets no runner feed" if "INDEX" in kinds else
              "the Momentum Runner still rides its index entries")
    rest = ("its other approaches there and every other market simulated" if tactics else
            "every other market simulated")
    return (f"{tnb_live_phrase(kinds, since, tactics)}: real orders under its own magic number, the broker's "
            f"figures are the record; {rest} (tnb_paper.sqlite), practice only, never in Overall; {runner}")


# --------------------------------------- Trend & Breakout's live approaches --
def check_live_tactics(names) -> tuple[str, ...]:
    """The approaches named for --tnb-live-tactics, each checked and spelled
    as the trader's journal spells them, in the order given, once each; ()
    for "all". An approach the trader does not trade is refused, so nothing
    is written (never real money on a name it cannot match)."""
    from ..engine.tactics import APPROACH_NAMES, approach_of
    words = [str(n).strip().upper().replace("-", "_").replace(" ", "_") for n in (names or ())]
    words = [w for w in words if w]
    if not words:
        raise ValueError(f"name at least one approach for --tnb-live-tactics ({F1_TACTIC} for Formula 1), or all")
    if ALL_TACTICS.upper() in words:
        if len(words) > 1:
            raise ValueError("--tnb-live-tactics all lets every approach on the live markets go real; name it on "
                             "its own")
        return ()
    out: list[str] = []
    bad: list[str] = []
    for w in words:
        a = approach_of(w)
        if a not in APPROACH_NAMES:
            bad.append(w)
        elif a not in out:
            out.append(a)
    if bad:
        raise ValueError(f"unknown approach {', '.join(bad)}: choose from {', '.join(APPROACH_NAMES)}, or all")
    return tuple(out)


def tnb_tactics_of(raw: dict) -> tuple[str, ...]:
    """The approaches config.json's tnb.live_tactics lets go real, spelled as
    the trader reads them (a name it does not know is left out, as it is
    there); () when every approach does."""
    from ..engine.tactics import APPROACH_NAMES, approach_of
    block = raw.get(TNB) if isinstance(raw, dict) else None
    block = block if isinstance(block, dict) else {}
    names = block.get("live_tactics")
    if isinstance(names, str):
        names = [names]
    out: list[str] = []
    for n in names if isinstance(names, (list, tuple)) else ():
        a = approach_of(n)
        if a in APPROACH_NAMES and a not in out:
            out.append(a)
    return tuple(out)


def tnb_tactics_name_nothing(raw: dict) -> bool:
    """Does config.json's tnb.live_tactics name no approach the trader knows
    (a list of unknown names, or not a list at all)? The trader then keeps
    every market on paper (TnbConfig.normalise, PaperBroker), so --show,
    --check and the restart of tnb.live_since_utc must read it the same way
    (10 Oct review). Missing, null, "" or [] mean every approach: False."""
    block = raw.get(TNB) if isinstance(raw, dict) else None
    block = block if isinstance(block, dict) else {}
    names = block.get("live_tactics")
    if names is None:
        return False
    if isinstance(names, str):
        names = [names] if names else []
    if not isinstance(names, (list, tuple)):
        return True
    return bool(names) and not tnb_tactics_of(raw)


def tnb_live_in_force(raw: dict) -> tuple[tuple[str, ...], str]:
    """(kinds, since) as the trader acts on them: ``tnb_live_of``, but no
    kind at all when tnb.live_tactics names no approach it knows."""
    kinds, since = tnb_live_of(raw)
    return ((), "") if kinds and tnb_tactics_name_nothing(raw) else (kinds, since)


NAMES_NO_APPROACH = ("config.json's tnb.live_tactics names no approach Trend & Breakout trades, so every market "
                     "stays on paper and nothing goes to the real broker (set it with --tnb-live-tactics "
                     "MOMENTUM_CONTINUATION, or all)")


# ---------------------------------------------------- the top-opportunity size --
def top_size_lines(raw: dict) -> list[str]:
    """What a Formula 1 trade (momentum continuation on a minor pair) risks
    with risk.top_size_enabled as config.json says it, in plain words."""
    from ..config import RiskConfig, _build
    rc = _build(RiskConfig, raw.get("risk") if isinstance(raw.get("risk"), dict) else {})
    normal = (f"{rc.base_risk_pct:g}% of the account and never more than {rc.max_risk_money:.2f} at its stop "
              f"(risk.base_risk_pct, risk.max_risk_money)")
    if not rc.top_size_enabled or float(rc.top_risk_money or 0.0) <= 0:
        return [f"Top-opportunity size: OFF (config.json: risk.top_size_enabled false). Every Trend & Breakout "
                f"trade, Formula 1's included, risks {normal}; a stronger setup may take a higher percent, still "
                f"never more than {rc.max_risk_money:.2f}. The Momentum Runner's own top size "
                f"(runner.top_size_enabled) is not changed."]
    segs = ", ".join(f"{str(t).lower().replace('_', ' ')} on "
                     f"{LIVE_KINDS.get(str(g).upper(), str(g).lower().replace('_', ' '))}"
                     for t, g in (rc.top_segments or ())) or "no segment listed (risk.top_segments)"
    return [f"Top-opportunity size: ON (config.json: risk.top_size_enabled true). A trade in {segs} - Formula "
            f"1's - may risk up to {rc.top_risk_money:.2f} (risk.top_risk_money), never more than "
            f"{rc.max_risk_pct:g}% of the account and at most {rc.top_max_lots:g} lots, but only while its own "
            f"record passes (at least {rc.top_min_trades} trades over {rc.top_min_days} days, positive after costs, "
            f"more days up than down); otherwise it risks {normal}, like every other trade. The Momentum Runner's "
            f"own top size (runner.top_size_enabled) is not changed."]


# ------------------------------------------------------------- the news window --
def check_news_minutes(value) -> float:
    """--news-before-minutes: at least 0.5 (30 seconds) and at most 120."""
    v = _a_number(value, "--news-before-minutes")
    if not (NEWS_MIN_MINUTES <= v <= NEWS_MAX_MINUTES):
        raise ValueError(f"--news-before-minutes must be at least {NEWS_MIN_MINUTES:g} (30 seconds) and at most "
                         f"{NEWS_MAX_MINUTES:g}, not {v:g}")
    return v


def _minutes_words(seconds: float) -> str:
    m = float(seconds) / 60.0
    if seconds < 60 or abs(m - round(m)) > 1e-9:
        return f"{float(seconds):g} seconds"
    return f"{m:g} minute{'' if m == 1 else 's'}"


def news_lines(raw: dict) -> list[str]:
    """What news.blackout_before_seconds means, as config.json says it. What
    it touches (checked 10 Oct): Trend & Breakout's scanner (NewsIntelligence
    verdict -> a blocker on the setup) for every new entry, paper and live
    alike, on both currencies of an FX pair (an index or metal: its own
    currencies, else USD), releases of at least
    news.min_importance_for_blackout. The Momentum Runner, Band Breaker,
    Crowd Fader, Rapid Momentum Rider and Financial Ian never read it (the
    Rider and Ian have their own news rules); the Runner only rides Trend &
    Breakout's entries, so an index entry not taken is not ridden either."""
    from ..config import NewsConfig, _build
    nc = _build(NewsConfig, raw.get("news") if isinstance(raw.get("news"), dict) else {})
    window = _minutes_words(float(nc.blackout_before_seconds))
    return [f"News: Trend & Breakout takes no new trade - practice or real - in the {window} before a "
            f"high-importance release (importance {int(nc.min_importance_for_blackout)} or more, "
            f"news.min_importance_for_blackout) on either currency of the pair (an index or a metal: its own "
            f"currencies, else USD). Trades already open are still managed, and the wait after a release is "
            f"unchanged. The Momentum Runner, Band Breaker, Crowd Fader, Rapid Momentum Rider and Financial Ian do "
            f"not read this setting; the Runner only rides Trend & Breakout's index entries, so one not taken "
            f"in the window is not ridden either (config.json: news.blackout_before_seconds "
            f"{float(nc.blackout_before_seconds):g})."]


# ------------------------------------------- Trend & Breakout's risk per trade --
def _a_number(value, what: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{what} must be a number, not {value!r}")
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{what} must be a number, not {value!r}") from None
    if not math.isfinite(v):
        raise ValueError(f"{what} must be a number, not {value!r}")
    return v


def check_risk(raw: dict, risk_pct=None, risk_money=None) -> tuple[Optional[float], Optional[float]]:
    """(base_risk_pct, max_risk_money) as they will be written, each None
    when not given, checked by RiskConfig's own rules against what
    config.json already says: the percent between risk.min_risk_pct and
    risk.max_risk_pct, the money more than 0. A refusal writes nothing."""
    from ..config import RiskConfig, _build
    block = raw.get("risk") if isinstance(raw.get("risk"), dict) else {}
    rc = _build(RiskConfig, block)
    pct = money = None
    if risk_pct is not None:
        pct = _a_number(risk_pct, "--risk-pct")
        lo, hi = float(rc.min_risk_pct), float(rc.max_risk_pct)
        if not (pct > 0 and lo <= pct <= hi):
            raise ValueError(f"--risk-pct must be between {lo:g} (risk.min_risk_pct) and {hi:g} (risk.max_risk_pct), "
                             f"not {pct:g}")
    if risk_money is not None:
        money = _a_number(risk_money, "--risk-money")
        if money <= 0:
            raise ValueError(f"--risk-money must be more than 0, not {money:g}")
    return pct, money


def risk_lines(raw: dict) -> list[str]:
    """What a Trend & Breakout trade risks now, from config.json, and who
    else these two numbers touch (nobody: checked 9 Oct, every reader of
    risk.base_risk_pct and risk.max_risk_money is Trend & Breakout's own -
    mintel/engine/risk.py, mintel/verify.py's check, mintel/research)."""
    from ..config import RiskConfig, RunnerConfig, _build
    rc = _build(RiskConfig, raw.get("risk") if isinstance(raw.get("risk"), dict) else {})
    run = _build(RunnerConfig, raw.get("runner") if isinstance(raw.get("runner"), dict) else {})
    cap = float(getattr(run, "max_risk_money", 0.0) or 0.0)
    copy = float(getattr(run, "copy_risk_pct", 0.0) or 0.0)
    # 10 Oct: the Runner copies the size Trend & Breakout would have had at its old percent (runner.copy_risk_pct)
    sized = (f"the size it would have had at {copy:g}% of the account (runner.copy_risk_pct, as before 9 Oct)"
             if copy > 0 else "its size")
    runner = (f"the Momentum Runner copies {sized} and each ride is held to at most {cap:.2f} at its own stop "
              f"(runner.max_risk_money)" if cap > 0 else
              f"the Momentum Runner copies {sized} (runner.max_risk_money is 0: no cap of its own)")
    # 10 Oct review: the top-opportunity sentence only while that size is on (risk.top_size_enabled)
    top = (f"A top-opportunity trade keeps its own limit (risk.top_risk_money {rc.top_risk_money:.2f}, at most "
           f"{rc.max_risk_pct:g}% of the account)." if rc.top_size_enabled and float(rc.top_risk_money or 0.0) > 0
           else "The top-opportunity size is OFF (risk.top_size_enabled), so no trade risks more.")
    return [f"Trend & Breakout's risk per trade: {rc.base_risk_pct:g}% of the account and never more than "
            f"{rc.max_risk_money:.2f} at its stop (config.json: risk.base_risk_pct, risk.max_risk_money). A stronger "
            f"setup may take a higher percent, never past the {rc.max_risk_pct:g}% ceiling, and still never past "
            f"{rc.max_risk_money:.2f}. {top}",
            f"Only Trend & Breakout sizes from these two numbers: {runner}; the Band Breaker, Crowd Fader, Rapid "
            f"Momentum Rider and Financial Ian size from their own settings."]


def set_modes(config_path: str | Path, changes: dict[str, str], *, gbp_per_pip: Optional[float] = None,
              ian_feed: Optional[str] = None, tnb_live_groups=None, risk_pct=None, risk_money=None,
              tnb_live_tactics=None, top_size: Optional[bool] = None, news_before_minutes=None,
              now: Optional[dt.datetime] = None) -> dict[str, str]:
    """Write the modes in `changes` ({bot: mode}), and the settings when
    given, and return every bot's resulting mode. Only those keys change;
    everything else in config.json, rider.json, ian.json and scalper.json
    is kept.

    Every file that will be touched is read and checked first, so a refusal
    means nothing was written: config.json must exist and be readable
    whichever bot is named (the bots' own files live in the folder it
    names); a bot's own file, when it is named, must be readable; the
    retired scalper can only go OFF; Trend & Breakout can only be LIVE or
    PAPER; and a bot going LIVE must carry its own magic number.

    ``tnb_live_groups`` (a list of kinds, or ["none"]) sets tnb.live_groups
    and, when the live set changes, tnb.live_since_utc (``now``); live kinds
    need Trend & Breakout on PAPER, so it is put there (refused with LIVE).
    ``risk_pct`` and ``risk_money`` set risk.base_risk_pct and
    risk.max_risk_money and nothing else in the risk block.

    10 Oct: ``tnb_live_tactics`` (approach names, or ["all"]) sets
    tnb.live_tactics - it needs Trend & Breakout on PAPER with markets kept
    LIVE after this call, and starts tnb.live_since_utc again (``now``) when
    the set of live trades changes; ``top_size`` (True/False) sets
    risk.top_size_enabled alone; ``news_before_minutes`` sets
    news.blackout_before_seconds to that many minutes in seconds."""
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
    live_kinds = check_live_kinds(tnb_live_groups) if tnb_live_groups is not None else None
    if live_kinds:
        if modes.get(TNB) == "LIVE":
            raise ValueError(TNB_LIVE_WITH_LIVE_REFUSAL)
        modes[TNB] = "PAPER"                                 # live kinds only mean something on PAPER
    live_tactics = check_live_tactics(tnb_live_tactics) if tnb_live_tactics is not None else None
    if live_tactics and modes.get(TNB) == "LIVE":
        raise ValueError(TNB_TACTICS_WITH_LIVE_REFUSAL)
    if top_size is not None and not isinstance(top_size, bool):
        raise ValueError(f"--top-size must be on or off, not {top_size!r}")
    news_minutes = check_news_minutes(news_before_minutes) if news_before_minutes is not None else None
    pip = _check_gbp_per_pip(gbp_per_pip) if gbp_per_pip is not None else None
    feed = None
    if ian_feed is not None:
        feed = str(ian_feed).strip().lower()
        if feed not in IAN_FEEDS:
            raise ValueError(f"Financial Ian's feed must be one of {', '.join(IAN_FEEDS)}, not {ian_feed!r}")
    # read and check everything first ...
    raw = _read_config(cfg_path)
    data = _data_dir(raw, cfg_path)
    pct, money = check_risk(raw, risk_pct, risk_money)
    old_kinds, _old_since = tnb_live_of(raw)
    old_tactics = tnb_tactics_of(raw)
    old_nothing = bool(old_kinds) and tnb_tactics_name_nothing(raw)    # nothing was live: the trader read it so
    kinds_after = live_kinds if live_kinds is not None else old_kinds
    paper_after = (modes.get(TNB) or tnb_mode_of(raw)) == "PAPER"
    if live_tactics and not (kinds_after and paper_after):
        raise ValueError(TNB_TACTICS_NOTHING_LIVE)          # an approach list with nothing live means nothing
    own: dict[str, dict] = {}
    for bot in FILES:
        if bot in modes or (bot == "rider" and pip is not None) or (bot == "ian" and feed is not None):
            own[bot] = _read_json(data / FILES[bot])
    going_live = [b for b, m in modes.items() if m == "LIVE"]
    if live_kinds and TNB not in going_live:
        going_live.append(TNB)                               # its orders there are real: its own magic, checked
    if going_live:
        problems = magic_problems(magics(cfg_path, data / FILES["scalper"]), going_live)
        if problems:
            raise ValueError("; ".join(problems))
    # ... then write
    was_paper = tnb_mode_of(raw) == "PAPER"
    in_config = {b: m for b, m in modes.items() if b in CONFIG_BOTS or b == TNB}
    for bot, mode in in_config.items():
        block = raw.get(bot)
        if not isinstance(block, dict):
            block = {}
            raw[bot] = block
        block["mode"] = mode
    stamp = to_utc_seconds(now)
    if live_kinds is not None:
        block = raw.get(TNB)
        if not isinstance(block, dict):
            block = {}
            raw[TNB] = block
        if not live_kinds:
            block["live_groups"], block["live_since_utc"] = [], ""
        elif set(live_kinds) != set(old_kinds) or not was_paper:
            # a new set (or one that only now takes effect, from LIVE): live from now
            block["live_groups"], block["live_since_utc"] = list(live_kinds), stamp
        # the same set while already on PAPER: both keys exactly as they were
    elif old_kinds and not was_paper and modes.get(TNB) == "PAPER":
        raw[TNB]["live_since_utc"] = stamp                   # LIVE -> PAPER: the kept live kinds take effect now
    if live_tactics is not None:
        block = raw.get(TNB)
        if not isinstance(block, dict):
            block = {}
            raw[TNB] = block
        block["live_tactics"] = list(live_tactics)
        if (old_nothing or set(live_tactics) != set(old_tactics)) and kinds_after and paper_after:
            # 10 Oct: a different set of live trades (narrower or wider) is a new trial: its live count starts now
            block["live_since_utc"] = stamp
    if top_size is not None:
        risk = raw.get("risk")
        if not isinstance(risk, dict):
            risk = {}
            raw["risk"] = risk
        risk["top_size_enabled"] = bool(top_size)
    if news_minutes is not None:
        news = raw.get("news")
        if not isinstance(news, dict):
            news = {}
            raw["news"] = news
        news["blackout_before_seconds"] = round(news_minutes * 60.0, 3)
    if pct is not None or money is not None:
        risk = raw.get("risk")
        if not isinstance(risk, dict):
            risk = {}
            raw["risk"] = risk
        if pct is not None:
            risk["base_risk_pct"] = pct
        if money is not None:
            risk["max_risk_money"] = money
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
    if (in_config or live_kinds is not None or pct is not None or money is not None or live_tactics is not None
            or top_size is not None or news_minutes is not None):
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
             scalper_file: Optional[Path] = None, files: Optional[dict[str, Path]] = None,
             tnb_live: Optional[tuple] = None) -> str:
    """One plain line per bot: its mode, what that means, its magic number
    when known, and where it is set. ``tnb_live`` is (kinds, since) from
    ``tnb_live_of``, or (kinds, since, approaches) with ``tnb_tactics_of``
    (10 Oct): Trend & Breakout's markets kept LIVE while on PAPER."""
    names = labels()
    where = {bot: f"config.json: {bot}.mode" for bot in CONFIG_BOTS + (TNB,)}
    kinds, since = (tuple(tnb_live[0]), str(tnb_live[1] or "")) if tnb_live else ((), "")
    tactics = tuple(tnb_live[2]) if tnb_live and len(tnb_live) > 2 else ()
    if str(modes.get(TNB) or "").upper() != "PAPER":
        kinds, since = (), ""                                # on LIVE every order is real: the kinds mean nothing
    if not kinds:
        tactics = ()
    if kinds:
        where[TNB] += ", tnb.live_groups" + (", tnb.live_tactics" if tactics else "")
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
            if mode == "PAPER" and kinds:
                meaning = tnb_paper_live_meaning(kinds, since, tactics)
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
    kinds, since = tnb_live_of(raw) if mode == "PAPER" else ((), "")
    if kinds and tnb_tactics_name_nothing(raw):
        # 10 Oct review: as the trader reads it - every market on paper
        kinds, since = (), ""
        what += "; tnb.live_tactics names no approach it trades, so every market is on paper"
    if kinds:
        # e.g. minor currency pairs LIVE (Formula 1) since ...; 10 Oct: "... LIVE for momentum continuation only ..."
        what += f"; {tnb_live_phrase(kinds, since, tnb_tactics_of(raw))}"
    if runner_mode == "OFF":
        rides = "the Momentum Runner is OFF"
    else:
        rides = "the Momentum Runner rides its index entries" + ("" if runner_mode == "LIVE" else f" ({runner_mode})")
        if "INDEX" in kinds:
            rides += " (no runner feed: its index orders are real)"
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


def tnb_live_lines(mode: str, live: tuple, tactics=(), *, tactics_given: bool = False,
                   restarted: bool = False) -> list[str]:
    """What --tnb-live-groups / --tnb-live-tactics leave in force, in plain
    words: which of Trend & Breakout's orders now go to the real broker, and
    (10 Oct) when the live trial starts again because the set of live
    trades changed."""
    kinds, since = live
    tactics = tuple(tactics or ())
    if str(mode).upper() != "PAPER" or not kinds:
        if tactics_given:
            where = ("it is LIVE, so every order is real" if str(mode).upper() == "LIVE" else
                     "no market is kept LIVE just now, so every order is simulated")
            return [f"Trend & Breakout: every approach may go real on the markets it keeps LIVE while on PAPER "
                    f"(config.json: tnb.live_tactics cleared); {where}."]
        # 10 Oct review: say so when the approach list stays in the file (it applies again later)
        kept = (f"; tnb.live_tactics - {tactics_in_words(tactics)} only - is kept, and applies again if "
                f"markets are kept LIVE later" if tactics else "")
        return [f"Trend & Breakout: no market kept LIVE - on PAPER every order is simulated "
                f"(config.json: tnb.live_groups and tnb.live_since_utc cleared{kept})."]
    if tactics:
        rest = (f"its other approaches there, and every other market, stay on paper (practice)"
                + ("; its index orders are real only for those approaches, so the Momentum Runner gets no runner "
                   "feed" if "INDEX" in kinds else "; the Momentum Runner still rides its index entries"))
        what = f"Its {tactics_in_words(tactics)} orders on {kinds_in_words(kinds)} go to the real broker; {rest}"
        keys = "tnb.live_groups, tnb.live_tactics, tnb.live_since_utc"
    else:
        rest = ("every other market stays on paper; its index orders are real, so the Momentum Runner rides "
                "those and gets no runner feed" if "INDEX" in kinds else
                "every other market stays on paper and still feeds the Momentum Runner")
        what = f"Its orders on {kinds_in_words(kinds)} go to the real broker; {rest}"
        keys = "tnb.live_groups, tnb.live_since_utc"
        if tactics_given:
            what = f"Every approach's orders on {kinds_in_words(kinds)} go to the real broker; {rest}"
            keys = "tnb.live_groups, tnb.live_tactics cleared, tnb.live_since_utc"
    out = [f"Trend & Breakout: {mode}, {tnb_live_phrase(kinds, since, tactics)}. {what} (config.json: {keys})."]
    if restarted:
        out.append(f"The set of live trades changed, so the live trial starts again: the page counts Trend & "
                   f"Breakout's live trades on {kinds_in_words(kinds)} from {_since_words(since)[6:]} "
                   f"(tnb.live_since_utc); the earlier ones stay in the account's record.")
    return out


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
    ap.add_argument("--tnb-live-groups", nargs="+", default=None, metavar="KIND",
                    help="kinds of market whose Trend & Breakout orders stay REAL while it is on PAPER: FX_MINOR "
                         "(Formula 1), FX_MAJOR, INDEX, GOLD, ...; none = every order on paper (tnb.live_groups)")
    ap.add_argument("--risk-pct", type=float, default=None, metavar="PCT",
                    help="Trend & Breakout's risk per trade, percent of the account (risk.base_risk_pct)")
    ap.add_argument("--risk-money", type=float, default=None, metavar="MONEY",
                    help="the most one Trend & Breakout trade may lose at its stop (risk.max_risk_money)")
    # 10 Oct: the switches for Aaron's pending decisions; each default keeps what runs today
    ap.add_argument("--tnb-live-tactics", nargs="+", default=None, metavar="APPROACH",
                    help="approaches whose Trend & Breakout orders go REAL on the markets it keeps LIVE: "
                         "MOMENTUM_CONTINUATION (Formula 1), ...; all = every approach, as on 9 Oct "
                         "(tnb.live_tactics; restarts tnb.live_since_utc when the live set changes)")
    ap.add_argument("--top-size", default=None, choices=("on", "off"),
                    help="the top-opportunity size for Trend & Breakout (risk.top_size_enabled); off = every "
                         "trade sized by --risk-pct / --risk-money")
    ap.add_argument("--news-before-minutes", type=float, default=None, metavar="N",
                    help="no new Trend & Breakout entry in the N minutes (at least 0.5, at most 120) before a "
                         "high-importance release on either currency of the pair (news.blackout_before_seconds)")
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
    risk_given = a.risk_pct is not None or a.risk_money is not None
    settings = (a.gbp_per_pip is not None or a.ian_feed is not None or a.tnb_live_groups is not None
                or risk_given or a.tnb_live_tactics is not None or a.top_size is not None
                or a.news_before_minutes is not None)
    if not changes and not settings and not a.show:
        ap.print_help()
        return 2
    try:
        tactics_before: Optional[tuple] = None
        nothing_before = False
        if a.tnb_live_tactics is not None:
            try:
                before = _read_config(Path(a.config))
                tactics_before = tnb_tactics_of(before)
                nothing_before = bool(tnb_live_of(before)[0]) and tnb_tactics_name_nothing(before)
            except (OSError, ValueError):
                tactics_before = None                        # set_modes says why, and writes nothing
        if changes or settings:
            modes = set_modes(a.config, changes, gbp_per_pip=a.gbp_per_pip, ian_feed=a.ian_feed,
                              tnb_live_groups=a.tnb_live_groups, risk_pct=a.risk_pct, risk_money=a.risk_money,
                              tnb_live_tactics=a.tnb_live_tactics,
                              top_size=None if a.top_size is None else a.top_size == "on",
                              news_before_minutes=a.news_before_minutes)
        else:
            modes = read_modes(a.config)
        raw = _read_config(Path(a.config))
        files = {bot: bot_file(a.config, bot) for bot in FILES}
        mg = magics(a.config, files["scalper"])
    except PartialWrite as exc:
        print(f"Stopped part way: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"Nothing changed: {exc}", file=sys.stderr)
        return 1
    live = tnb_live_of(raw)
    tactics = tnb_tactics_of(raw)
    # 10 Oct review: a tnb.live_tactics naming no approach keeps every market on paper, as the trader reads it
    names_nothing = bool(live[0]) and tnb_tactics_name_nothing(raw)
    print(describe(modes, mg, files=files, tnb_live=((), "", ()) if names_nothing else live + (tactics,)))
    if names_nothing and modes.get(TNB) == "PAPER":
        print(f"Trend & Breakout: {NAMES_NO_APPROACH}.")
    if a.gbp_per_pip is not None:
        print(f"Rapid Momentum Rider exposure: GBP {a.gbp_per_pip:.2f} per pip ({files['rider']}: user_pip_value_gbp)")
    if a.ian_feed is not None:
        print(f"Financial Ian data feed: {a.ian_feed} ({files['ian']}: feed.vendor)")
    if (a.tnb_live_groups is not None or a.tnb_live_tactics is not None) and not names_nothing:
        for line in tnb_live_lines(modes.get(TNB, TNB_DEFAULT), live, tactics,
                                   tactics_given=a.tnb_live_tactics is not None,
                                   restarted=tactics_before is not None and (nothing_before
                                                                             or set(tactics_before) != set(tactics))):
            print(line)
    if risk_given:
        for line in risk_lines(raw):
            print(line)
    if a.top_size is not None:
        for line in top_size_lines(raw):
            print(line)
    if a.news_before_minutes is not None:
        for line in news_lines(raw):
            print(line)
    if changes or settings:
        print(RESTART)
    else:
        print("Change a mode with --live, --paper or --off (a bot name, or all), then restart the bot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
