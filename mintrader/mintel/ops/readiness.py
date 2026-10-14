"""Live readiness: which DEMO strategies are getting close to real money (Aaron, 14 Oct 2026).

    python -m mintel.ops.readiness --config data/config.json             # work it out and write it
    python -m mintel.ops.readiness --config data/config.json show        # print the file
    python -m mintel.ops.readiness --config data/config.json mark-notified   # after the message has gone

THE BAR (proposed 14 Oct; every number from the broker's demo results after all costs):

QUALIFIED - enters the readiness review:
  1. QUALIFY_TRADES (40)+ demo trades over QUALIFY_DAYS (10)+ UK trading days;
  2. the broker's net after costs is positive;
  3. the bootstrap's lower bound (the 10th percentile of 2,000 resampled averages, ``weekly.bootstrap``) of the
     per-trade net is above zero;
  4. its worst drawdown, replayed on a £1,500 account at 0.5% a trade (``sizing.simulate``), is within 10%;
  5. paper and demo agree: both make money a trade, and (where both record their risk) the demo's average R is
     at least half the paper's.

LIVE PREPARATION STARTED - everything in the bar except the count: 30+ demo trades over 7+ days, and 2 to 5 hold.
APPROACHING QUALIFICATION - half the trades (20+), in profit after costs, and its last 10 trades in profit too
(a positive trend). A streak is never enough on its own.

THE FILE, data/live_readiness.json (the pulse publishes it as reports/live/readiness.json; Claude's nightly
routine sends the phone/email message from it): the stage (none / approaching / LIVE PREPARATION STARTED /
qualified), the bots (id, exact version, name, stage, paper and demo results, why they are promising, the
evidence still needed, whether £1,500 is justified, the earliest defensible date or "no estimate yet"), a
headline and a short message ready to send, ``fingerprint`` (a hash of the stage and of each bot's id, version
and stage - not of the day-to-day numbers), ``notify`` (true only when that fingerprint differs from the last
one notified) and ``notified`` (the last fingerprint notified: kept across writes, set by ``mark-notified``).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Optional, Sequence

from ..clock import to_utc, utcnow
from .standing import uk_date

FILE = "live_readiness.json"
SCHEMA = 1
NONE, APPROACHING, PREPARATION, QUALIFIED = "none", "approaching", "LIVE PREPARATION STARTED", "qualified"
STAGES = (NONE, APPROACHING, PREPARATION, QUALIFIED)
STAGE_WORDS = {NONE: "No action required yet", APPROACHING: "Approaching qualification",
               PREPARATION: "LIVE PREPARATION STARTED", QUALIFIED: "Qualified for the readiness review"}
QUALIFY_TRADES, QUALIFY_DAYS = 40, 10
PREP_TRADES, PREP_DAYS = 30, 7
APPROACH_TRADES, TREND_TRADES = 20, 10
MAX_DD_PCT = 10.0
ESTIMATE_MIN_DAYS = 5            # an earliest date only once its pace is known from this many trading days
BAR_TEXT = (f"{QUALIFY_TRADES}+ demo trades over {QUALIFY_DAYS}+ trading days; the broker's net after costs "
            f"positive; the cautious (bootstrap) per-trade result above zero; worst drawdown within "
            f"{MAX_DD_PCT:g}% of a £1,500 account at 0.5% a trade; paper and demo agree")


def _mean(xs: Sequence[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _rs(trades, fallback) -> list[float]:
    from .sizing import r_of
    out = []
    for t in trades:
        r, _how = r_of(t, fallback)
        if r is not None and math.isfinite(r):
            out.append(r)
    return out


def add_trading_days(d: dt.date, n: int) -> dt.date:
    while n > 0:
        d += dt.timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def assess(cid: str, demo: Sequence[dict], paper: Sequence[dict], stated_risk: Optional[float], today: dt.date
           ) -> dict:
    """One demo strategy against the bar: {stage, checks, needed, promising, demo, paper, lcb, sim, justified,
    earliest}."""
    from . import weekly as W
    from .leagues import quality, conditions
    from .sizing import simulate
    demo = sorted((t for t in demo if t.get("closed") is not None), key=lambda t: t["closed"])
    dq, pq = quality(demo), quality(paper)
    n, days, net = dq["trades"], dq["days"], dq["net"]
    _p, lcb = W.bootstrap([float(t["net"]) for t in demo], cid) if n >= 2 else (1.0, None)
    sim = simulate({cid: demo}, fallback_risk={cid: stated_risk}, check_min_lot=False) if demo else None
    dd = sim["max_dd_pct"] if sim and sim["taken"] else None
    d_r, p_r = _rs(demo, stated_risk), _rs(paper, None)
    d_mean, p_mean = _mean([float(t["net"]) for t in demo]), _mean([float(t["net"]) for t in paper])
    agree = bool(d_mean and d_mean > 0 and p_mean and p_mean > 0)
    if agree and d_r and p_r and _mean(p_r) > 0:
        agree = _mean(d_r) >= 0.5 * _mean(p_r)
    checks = {"count": n >= QUALIFY_TRADES and days >= QUALIFY_DAYS,
              "prep_count": n >= PREP_TRADES and days >= PREP_DAYS,
              "net": net > 0, "lcb": lcb is not None and lcb > 0,
              "drawdown": dd is not None and dd <= MAX_DD_PCT, "agree": agree,
              "approach": n >= APPROACH_TRADES and net > 0 and sum(float(t["net"]) for t in demo[-TREND_TRADES:]) > 0}
    quality_ok = checks["net"] and checks["lcb"] and checks["drawdown"] and checks["agree"]
    if checks["count"] and quality_ok:
        stage = QUALIFIED
    elif checks["prep_count"] and quality_ok:
        stage = PREPARATION
    elif checks["approach"]:
        stage = APPROACHING
    else:
        stage = NONE
    needed = []
    if not checks["count"]:
        bits = []
        if n < QUALIFY_TRADES:
            bits.append(f"{QUALIFY_TRADES - n} more demo trades")
        if days < QUALIFY_DAYS:
            bits.append(f"{QUALIFY_DAYS - days} more trading days")
        needed.append(" and ".join(bits))
    if not checks["net"]:
        needed.append("the broker's net after costs above zero")
    if not checks["lcb"]:
        needed.append("the cautious per-trade result above zero (more trades, or a steadier edge)")
    if not checks["drawdown"]:
        needed.append("its risk a trade on record" if dd is None else
                      f"a smaller worst drawdown (now {dd:.1f}% of £1,500; at most {MAX_DD_PCT:g}%)")
    if not checks["agree"]:
        needed.append("paper and demo to agree (both making money a trade)")
    promising = []
    if net > 0:
        promising.append(f"Up £{net:,.2f} on demo after all costs over {n} trade{'' if n == 1 else 's'}")
    if days:
        promising.append(f"Up on {dq['up_days']} of its {days} demo trading days")
    best = conditions(demo)["best"][:1]
    if best:
        promising.append(f"Best so far: {best[0]['label']} (£{best[0]['net']:,.2f} from {best[0]['trades']} trades)")
    if pq["trades"] and pq["net"] > 0:
        promising.append(f"Paper agrees: £{pq['net']:,.2f} after costs over {pq['trades']} trades")
    if stage == QUALIFIED:
        justified = (sim is not None and sim["net"] > 0 and dd is not None and dd <= MAX_DD_PCT,
                     "Yes, at 0.5% a trade (£7.50 risk on £1,500)")
        earliest = "now, once you approve"
    else:
        justified = (False, "Not yet: " + (needed[0] if needed else "the bar is not met"))
        earliest = "no estimate yet"
        if stage != NONE and days >= ESTIMATE_MIN_DAYS and n:
            # its pace over the UK trading days since its first demo trade - not just the days it traded - so
            # the days and the trades still needed become calendar trading days at the rate it really trades
            tdays = W.trading_days_between(uk_date(demo[0]["closed"]), today)
            more = max(math.ceil(max(0, QUALIFY_DAYS - days) * tdays / days),
                       math.ceil(max(0, QUALIFY_TRADES - n) * tdays / n))
            earliest = add_trading_days(today, max(1, more)).isoformat()
    figs = lambda q: {"net": q["net"], "trades": q["trades"], "days": q["days"], "up_days": q["up_days"],   # noqa
                      "max_dd": q["max_dd"], "mean": q["mean"]}
    return {"stage": stage, "checks": checks, "needed": needed, "promising": promising, "demo": figs(dq),
            "paper": figs(pq), "lcb": lcb, "sim_1500": ({k: sim[k] for k in ("net", "end", "max_dd_pct", "taken",
                                                                                "stated_risk")} if sim else None),
            "justified": justified[0], "justified_words": justified[1], "earliest": earliest}


def fingerprint(stage: str, bots: Sequence[dict]) -> str:
    """A hash of what is worth a message: the stage and each bot's id, version and stage. The day-to-day
    numbers are left out, so they never cause a message on their own."""
    body = {"stage": stage, "bots": sorted([str(b.get("id")), str(b.get("version")), str(b.get("stage"))]
                                           for b in bots)}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()[:16]


BASELINE = fingerprint(NONE, [])


def _carried(previous: Optional[dict], cid: str) -> Optional[dict]:
    """The last file's entry for ``cid`` when it is a usable one (a known stage and its demo figures)."""
    for b in (previous or {}).get("bots") or ():
        if (isinstance(b, dict) and b.get("id") == cid and b.get("stage") in STAGES and b["stage"] != NONE
                and all(k in b for k in ("version", "nick", "needed", "earliest", "paper"))
                and isinstance(b.get("demo"), dict) and isinstance(b["demo"].get("net"), (int, float))
                and "trades" in b["demo"]):
            return b
    return None


def review(world: dict, built: dict, data_dir, now: Optional[dt.datetime] = None,
           previous: Optional[dict] = None) -> dict:
    """Every strategy that has been on DEMO against the bar; the file's content (without ``notified`` and
    ``notify``). A strategy whose demo record cannot be read now (MetaTrader unread) is UNKNOWN, not "none":
    its entry in ``previous`` (the last file) is kept as it was, marked ``stale``, so an outage never changes
    the stage, the fingerprint or sends a message."""
    from .leagues import version_of
    from . import weekly as W
    now = to_utc(now or utcnow())
    today = uk_date(now)
    tests = world.get("tests") or {}
    stints = built.get("stints") or []
    stated = {}
    for s in stints:
        if s.get("risk_gbp"):
            stated[s["id"]] = s["risk_gbp"]
    lab_versions = {}
    if data_dir:
        for r in W._rows(Path(data_dir) / "lab" / "lab.sqlite", "SELECT id, version FROM candidates"):
            lab_versions[f"lab:{r['id']}"] = r
    bots = []
    for cid, demo in (built.get("demo_trades") or {}).items():
        if not (built.get("demo_known") or {}).get(cid, True):
            old = _carried(previous, cid)
            if old is not None:
                bots.append(dict(old, stale=True))
            continue
        t = tests.get(cid) or {"id": cid, "kind": cid.split(":", 1)[0], "nick": cid}
        a = assess(cid, demo, t.get("trades") or [], stated.get(cid), today)
        if a["stage"] == NONE:
            continue
        on = any(s["id"] == cid and s.get("until") is None for s in stints)
        bots.append(dict(a, id=cid, version=version_of(t, lab_versions.get(cid)), nick=t.get("nick") or cid,
                         desc=t.get("desc") or "", on_demo=on))
    bots.sort(key=lambda b: (-STAGES.index(b["stage"]), -b["demo"]["net"], b["id"]))
    stage = bots[0]["stage"] if bots else NONE
    for b in bots:
        b["fingerprint"] = fingerprint(b["stage"], [b])
    fp = fingerprint(stage, bots)
    return {"schema": SCHEMA, "written_utc": now.replace(microsecond=0).isoformat(), "stage": stage,
            "stage_words": STAGE_WORDS[stage], "bar": BAR_TEXT, "bots": bots, "fingerprint": fp,
            "headline": headline(stage, bots), "message": message(stage, bots),
            "real_trading_enabled": False, "approval": "only at the Mac's command line: python -m "
                                                       "mintel.ops.realmoney (the web page cannot approve)"}


def headline(stage: str, bots: Sequence[dict]) -> str:
    if stage == NONE or not bots:
        return "Live readiness: no action required yet"
    b = bots[0]
    return f"Live readiness: {b['nick']} — {STAGE_WORDS[stage]}"


def message(stage: str, bots: Sequence[dict]) -> str:
    """A short, honest message ready to send (10 to 20 seconds to read)."""
    if stage == NONE or not bots:
        return "No strategy is near real money yet. Demo keeps gathering evidence; nothing needs you."
    lines = []
    for b in bots[:3]:
        d = b["demo"]
        lines.append(f"{b['nick']} ({b['version']}): {STAGE_WORDS[b['stage']].lower()} — £{d['net']:+,.2f} on demo "
                     f"over {d['trades']} trades after costs; still needed: "
                     f"{'; '.join(b['needed']) if b['needed'] else 'nothing, it meets the bar'}; earliest: "
                     f"{b['earliest']}.")
    tail = (" Approval is yours alone, at the Mac's command line; real money stays switched off in the code until "
            "then." if stage == QUALIFIED else " Nothing needs you yet.")
    return " ".join(lines) + tail


def path_of(data_dir) -> Path:
    return Path(data_dir) / FILE


def read(data_dir) -> Optional[dict]:
    try:
        d = json.loads(path_of(data_dir).read_text())
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def with_notify(content: dict, old: Optional[dict]) -> dict:
    """The content with ``notified`` carried over from the last file and ``notify`` set: true only when the
    stage or the set of bots changed since the last fingerprint notified (none notified yet: the baseline of
    "none, no bots", so a quiet start never messages)."""
    notified = (old or {}).get("notified") if isinstance((old or {}).get("notified"), dict) else None
    last = (notified or {}).get("fingerprint") or BASELINE
    return dict(content, notified=notified, notify=content["fingerprint"] != last)


def write(data_dir, content: dict) -> dict:
    from .weekly import _write_atomic
    out = with_notify(content, read(data_dir))
    _write_atomic(path_of(data_dir), out)
    return out


def mark_notified(data_dir, now: Optional[dt.datetime] = None) -> Optional[dict]:
    """After the message has gone: the current fingerprint becomes the last notified, ``notify`` false."""
    from .weekly import _write_atomic
    cur = read(data_dir)
    if cur is None:
        return None
    cur["notified"] = {"fingerprint": cur.get("fingerprint"), "stage": cur.get("stage"),
                       "bots": [[b.get("id"), b.get("version"), b.get("stage")] for b in cur.get("bots") or ()],
                       "ts_utc": to_utc(now or utcnow()).replace(microsecond=0).isoformat()}
    cur["notify"] = False
    _write_atomic(path_of(data_dir), cur)
    return cur


def compute(data_dir, config_path=None, now: Optional[dt.datetime] = None, positions: Optional[list] = None) -> dict:
    """Work the review out from the records (read-only)."""
    from .leagues import build
    from .squad import load_world, reset_of
    now = to_utc(now or utcnow())
    reset = reset_of(None)
    world = load_world(data_dir, config_path, now, reset, cache_seconds=0)
    built = build(world, data_dir, positions, reset, now + dt.timedelta(seconds=1))
    return review(world, built, data_dir, now, previous=read(data_dir))


def refresh(data_dir, config_path=None, now: Optional[dt.datetime] = None, positions: Optional[list] = None
            ) -> Optional[dict]:
    """compute + write; None (and nothing written) when it could not be worked out. Never raises."""
    try:
        return write(data_dir, compute(data_dir, config_path, now, positions))
    except Exception as exc:
        import logging
        logging.getLogger("mintel.readiness").warning("readiness: %s", exc)
        return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="live readiness: who is near real money")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("cmd", nargs="?", default="write", choices=("write", "show", "mark-notified"))
    a = ap.parse_args(argv)
    data = Path(a.config).parent
    if a.cmd == "show":
        print(json.dumps(read(data), indent=1, default=str))
        return 0
    if a.cmd == "mark-notified":
        got = mark_notified(data)
        print("marked" if got else "no readiness file yet")
        return 0 if got else 1
    from .weekly import fetch_live_positions
    got = refresh(data, a.config, positions=fetch_live_positions(a.config))
    print(json.dumps({k: (got or {}).get(k) for k in ("stage", "fingerprint", "notify", "headline")}, default=str))
    return 0 if got else 1


if __name__ == "__main__":
    sys.exit(main())
