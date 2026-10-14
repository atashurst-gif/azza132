"""ROAD TO PROFIT (Aaron, 14 Oct 2026): what the evidence says about £25, £50 and £100 a day.

The milestones are AMBITIONS, NOT TARGETS: £25, £50 and £100 net average daily profit, and higher as the account
grows (£250 from £5,000, £500 from £10,000). For each one, from ACTUAL EVIDENCE ONLY:

* which current strategies could contribute - DEMO results first (the broker's, after all costs), PAPER second,
  each set projected ON ITS OWN and labelled; the two are never added together;
* their demonstrated per-trade net after costs, their R a trade (net / the money risked) and trades a day;
* the capital and the position size it would take (``sizing``): the equity at 0.5% a trade that makes the
  milestone on that evidence, and the lots and £ a pip on EUR/USD at that equity;
* downside, typical and favourable outcomes: their ACTUAL trade results resampled (bootstrap) over SIM_DAYS (20)
  trading days at the proposed size (£1,500 at 0.5% = £7.50 a trade), giving the 10th, 50th and 90th
  percentiles, and the drawdown along the way;
* how much uncertainty remains: the trade counts and the width of the 80% interval of the average R;
* what would have to improve (more trades a day, a bigger edge a trade, or more capital);
* whether the milestone is realistic with £1,500 at 0.5% - "supported" only when the typical (50th percentile)
  outcome reaches it and the downside (10th) is still above zero.

When the evidence is thin - fewer than MIN_TRADES (30) trades over MIN_DAYS (5) days in the set - there is NO
projection, only the plain reason. Nothing here is a forecast; it is the record, resampled.

THE BARRIERS the system can see, each with what is being done about it: too few trades, costs eating the edge,
strategies that move together (correlation), and drawdown - answered by the conveyor belt (the Saturday
refresh), the daily promotions into the free places on Demo and the new families Claude adds every Saturday.
"""
from __future__ import annotations

import datetime as dt
import math
import random
from statistics import median
from typing import Optional, Sequence

from ..clock import to_utc, utcnow
from .standing import uk_date

BASE_MILESTONES = (25.0, 50.0, 100.0)
GROWTH_MILESTONES = ((5000.0, 250.0), (10000.0, 500.0))
SIM_DAYS = 20
RESAMPLES = 2000
MIN_TRADES = 30                 # a set needs this many trades ...
MIN_DAYS = 5                    # ... over this many trading days before any projection
STRAT_MIN_TRADES = 10           # a strategy contributes with this many trades in the window
WINDOW_DAYS = 28                # the evidence: the last four weeks (the weekly pick's own window)
LAB_GRACE = dt.timedelta(days=7)        # a lab demo stint's last trades may close after its demote: this long at most
PAPER_TOP = 5                   # the paper set: the best five by their cautious per-trade result
COSTS_HEAVY = 0.5               # costs at least this share of what it made before costs: "costs eat the edge"
CORR_HIGH = 0.6
CORR_MIN_DAYS = 8
DD_LIMIT_PCT = 10.0


def milestones(equity: Optional[float] = None) -> list[float]:
    out = list(BASE_MILESTONES)
    for at, m in GROWTH_MILESTONES:
        if equity is not None and equity >= at:
            out.append(m)
    return out


def _q(xs: Sequence[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return 0.0
    k = (len(s) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


# ------------------------------------------------------------------ the evidence --
def strategy_evidence(cid: str, nick: str, league: str, trades: Sequence[dict], stated_risk: Optional[float] = None,
                      now: Optional[dt.datetime] = None) -> dict:
    """One strategy's evidence: {id, nick, league, trades, days (the UK days it traded), per_day, mean_net, rs
    (its R multiples), mean_r, r_low/r_high (80% interval of the average R), costs_share, symbols, day_nets}.

    ``per_day`` is its trades a UK TRADING DAY from its first trade here to ``now`` (its last trade when not
    given), the weekly pick's own measure - not its trades on the days it happened to trade, which would make
    a strategy that trades one day in four look four times as busy."""
    from . import weekly as W
    from .sizing import r_of
    from ..lab.stats import seed_of
    trades = sorted((t for t in trades if t.get("closed") is not None), key=lambda t: t["closed"])
    rs = []
    for t in trades:
        r, _ = r_of(t, stated_risk)
        if r is not None and math.isfinite(r):
            rs.append(r)
    days: dict = {}
    for t in trades:
        d = uk_date(t["closed"])
        days[d] = days.get(d, 0.0) + float(t["net"])
    n = len(trades)
    tdays = (W.trading_days_between(uk_date(trades[0]["closed"]),
                                    uk_date(to_utc(now)) if now is not None else uk_date(trades[-1]["closed"]))
             if trades else 1)
    nets = [float(t["net"]) for t in trades]
    costs = [float(t["costs"]) for t in trades if t.get("costs") is not None]
    gross = sum(nets) + sum(costs) if len(costs) == n and n else None
    lo = hi = None
    if len(rs) >= 2:
        rnd = random.Random(seed_of(cid))
        means = sorted(sum(rs[rnd.randrange(len(rs))] for _ in rs) / len(rs) for _ in range(400))
        lo, hi = _q(means, 0.10), _q(means, 0.90)
    return {"id": cid, "nick": nick, "league": league, "trades": n, "days": len(days),
            "per_day": round(n / tdays, 2) if n else 0.0, "mean_net": round(sum(nets) / n, 2) if n else None,
            "net": round(sum(nets), 2), "rs": rs, "mean_r": (sum(rs) / len(rs)) if rs else None,
            "r_low": lo, "r_high": hi, "r_known": len(rs),
            "costs_share": (sum(costs) / gross) if gross and gross > 0 else None, "costs": round(sum(costs), 2)
            if costs else None, "gross": round(gross, 2) if gross is not None else None,
            "symbols": sorted({str(t.get("symbol") or "") for t in trades if t.get("symbol")}),
            "day_nets": {d.isoformat(): round(v, 2) for d, v in days.items()}, "_trades": trades}


def evidence(world: dict, built: dict, now: dt.datetime, window_days: int = WINDOW_DAYS) -> dict:
    """{DEMO: [evidence], PAPER: [evidence], DEMO_UNKNOWN: [ids]} over the last ``window_days``: every DEMO
    strategy with a known record, and the PAPER_TOP paper tests with the best cautious per-trade result (the
    weekly pick's bootstrap), each with STRAT_MIN_TRADES+ trades. Demo first, paper second; never merged.
    DEMO_UNKNOWN: the demo ids whose record cannot be read yet (MetaTrader unread) - unknown, never "nothing" -
    and only those with a stint on DEMO overlapping the window (the rule ``leagues.snapshot_of`` uses for the
    stints running): a stint that ended before the window holds no trade in it, so it is known to be empty here -
    except a lab idea's, whose demo record is not cut at the stint's end (its last position closes after the
    demote), so it stays unknown while that record cannot be read.
    A retired idea is not a current strategy: it keeps its career but is never paper evidence here."""
    from . import weekly as W
    now = to_utc(now)
    start = now - dt.timedelta(days=window_days)
    tests = world.get("tests") or {}
    stated = {s["id"]: s.get("risk_gbp") for s in built.get("stints") or () if s.get("risk_gbp")}
    stints_of: dict = {}
    for s in built.get("stints") or ():
        stints_of.setdefault(s["id"], []).append(s)

    def in_window(cid: str) -> bool:
        # no stint on record (a lab idea's own demo rows only): it cannot be shown empty, so it stays unknown.
        # A lab idea's record is its own demo sqlite, not cut at the stint's end: its last position closes after
        # the demote (the engine's next step, a broker stop, the market's reopening) - but every lab trade has a
        # broker stop, so it closes within days, not months: LAB_GRACE after the end bounds it (rv14 S3 follow-up)
        ss = stints_of.get(cid)
        grace = LAB_GRACE if cid.startswith("lab:") else dt.timedelta(0)
        return not ss or any((s.get("since") is None or s["since"] <= now)
                             and (s.get("until") is None or s["until"] + grace > start) for s in ss)
    demo, unknown = [], []
    for cid, trades in (built.get("demo_trades") or {}).items():
        if not (built.get("demo_known") or {}).get(cid, True):
            if in_window(cid):
                unknown.append(cid)
            continue
        tr = [t for t in trades if start <= t["closed"] <= now]
        if len(tr) < 1:
            continue
        nick = (tests.get(cid) or {}).get("nick") or cid
        demo.append(strategy_evidence(cid, nick, "DEMO", tr, stated.get(cid), now))
    paper = []
    for cid, t in tests.items():
        if t.get("retired"):                       # benched: it can never trade again or be promoted
            continue
        tr = [x for x in t.get("trades") or () if start <= x["closed"] <= now]
        if len(tr) < STRAT_MIN_TRADES or sum(float(x["net"]) for x in tr) <= 0:
            continue
        _p, lcb = W.bootstrap([float(x["net"]) for x in sorted(tr, key=lambda x: x["closed"])], cid)
        paper.append((lcb if lcb is not None else -1e9, cid, tr))
    paper.sort(key=lambda x: (-x[0], x[1]))
    paper_ev = [strategy_evidence(cid, (tests.get(cid) or {}).get("nick") or cid, "PAPER", tr, None, now)
                for _lcb, cid, tr in paper[:PAPER_TOP]]
    demo.sort(key=lambda e: -((e["mean_r"] or 0.0) * e["per_day"]))
    paper_ev.sort(key=lambda e: -((e["mean_r"] or 0.0) * e["per_day"]))
    return {"DEMO": demo, "PAPER": paper_ev, "DEMO_UNKNOWN": sorted(unknown)}


# ------------------------------------------------------------------ the bootstrap --
def bootstrap_days(contribs: Sequence[dict], risk: float, days: int = SIM_DAYS, resamples: int = RESAMPLES,
                   seed: int = 1410) -> Optional[dict]:
    """Resample each strategy's ACTUAL R results for ``days`` trading days (its own trades a day, rounded) at a
    fixed ``risk`` a trade: {p10, p50, p90 (20-day totals), daily_p10/50/90, dd_p50, dd_p90 (£ drawdown along the
    way), trades}. None when no strategy has an R record."""
    pools = [(list(c["rs"]), max(0, int(round(float(c["per_day"]) * days)))) for c in contribs if c.get("rs")]
    pools = [(rs, k) for rs, k in pools if rs and k]
    if not pools:
        return None
    rnd = random.Random(seed)
    totals, dds = [], []
    for _ in range(resamples):
        seq = []
        for rs, k in pools:
            seq.extend(rs[rnd.randrange(len(rs))] for _ in range(k))
        rnd.shuffle(seq)
        run = peak = dd = 0.0
        for r in seq:
            run += r * risk
            peak = max(peak, run)
            dd = max(dd, peak - run)
        totals.append(run)
        dds.append(dd)
    p10, p50, p90 = (_q(totals, p) for p in (0.10, 0.50, 0.90))
    return {"p10": round(p10, 2), "p50": round(p50, 2), "p90": round(p90, 2),
            "daily_p10": round(p10 / days, 2), "daily_p50": round(p50 / days, 2), "daily_p90": round(p90 / days, 2),
            "dd_p50": round(_q(dds, 0.5), 2), "dd_p90": round(_q(dds, 0.9), 2),
            "trades": sum(k for _rs, k in pools), "days": days, "risk": risk}


# ------------------------------------------------------------------ one evidence set, one milestone --
def project(contribs: Sequence[dict], milestone: float, account: float, risk_pct: float, label: str) -> dict:
    """What one evidence set (DEMO or PAPER, never both) says about one milestone."""
    from .sizing import reference_size, risk_gbp
    useful = [c for c in contribs if c["trades"] >= STRAT_MIN_TRADES and c.get("mean_r") is not None
              and c["mean_r"] > 0 and c["r_known"] >= STRAT_MIN_TRADES]
    total = sum(c["trades"] for c in contribs)
    days = len(set().union(*((c.get("day_nets") or {}) for c in contribs)))      # distinct UK days, not the max
    risk = risk_gbp(account, risk_pct)
    out = {"league": label, "milestone": milestone, "account": account, "risk_pct": risk_pct, "risk": risk,
           "contributors": [{k: c.get(k) for k in ("id", "nick", "league", "trades", "days", "per_day", "mean_net",
                                                   "mean_r", "r_low", "r_high", "net")} for c in useful],
           "trades": total, "days": days, "supported": False, "projection": None}
    if total < MIN_TRADES or days < MIN_DAYS:
        out["words"] = (f"Not supported yet: {total} {label.lower()} trade{'' if total == 1 else 's'} over {days} "
                        f"day{'' if days == 1 else 's'} is too little to project from. A projection needs at least "
                        f"{MIN_TRADES} trades over {MIN_DAYS} days.")
        out["improve"] = [f"{max(0, MIN_TRADES - total)} more trades" if total < MIN_TRADES else
                          f"{MIN_DAYS - days} more trading days"]
        return out
    if not useful:
        out["words"] = (f"Not supported: no {label.lower()} strategy shows a positive result a trade with "
                        f"{STRAT_MIN_TRADES}+ trades and its risk on record.")
        out["improve"] = ["a strategy with a positive edge after costs"]
        return out
    # the bar again, on the strategies the projection is actually built from: never a projection below it
    u_total = sum(c["trades"] for c in useful)
    u_days = len(set().union(*((c.get("day_nets") or {}) for c in useful)))
    out["trades"], out["days"] = u_total, u_days
    if u_total < MIN_TRADES or u_days < MIN_DAYS:
        out["words"] = (f"Not supported yet: the {label.lower()} strategies with an edge have {u_total} "
                        f"trade{'' if u_total == 1 else 's'} over {u_days} day{'' if u_days == 1 else 's'}, too "
                        f"little to project from. A projection needs at least {MIN_TRADES} trades over {MIN_DAYS} "
                        f"days.")
        out["improve"] = [f"{MIN_TRADES - u_total} more trades" if u_total < MIN_TRADES else
                          f"{MIN_DAYS - u_days} more trading days"]
        return out
    r_day = sum(c["mean_r"] * c["per_day"] for c in useful)
    sim = bootstrap_days(useful, risk)
    need_equity = milestone / (r_day * risk_pct / 100.0) if r_day > 0 else None
    size = reference_size("EURUSD", need_equity, risk_pct) if need_equity else None
    widths = [(c["r_high"] - c["r_low"]) for c in useful if c.get("r_low") is not None]
    mean_rs = [c["mean_r"] for c in useful]
    out["projection"] = {"r_per_day": round(r_day, 3), "typical_day": round(r_day * risk, 2), "sim": sim,
                         "equity_needed": round(need_equity, -1) if need_equity else None,
                         "eurusd_lots": size.volume if size and size.ok else None,
                         "eurusd_per_pip": size.per_pip if size and size.ok else None,
                         "dd_pct_p50": round(sim["dd_p50"] / account * 100.0, 1) if sim else None,
                         "dd_pct_p90": round(sim["dd_p90"] / account * 100.0, 1) if sim else None,
                         "interval_width_r": round(median(widths), 3) if widths else None,
                         "uncertain": bool(widths) and median(widths) > median(mean_rs)}
    supported = bool(sim) and sim["daily_p50"] >= milestone and sim["p10"] > 0
    out["supported"] = supported
    names = ", ".join(c["nick"] for c in useful[:3])
    if supported:
        out["words"] = (f"Supported by the {label.lower()} evidence: {names} would typically make about "
                        f"£{sim['daily_p50']:,.2f} a day at £{account:,.0f} and {risk_pct:g}% a trade "
                        f"(range £{sim['daily_p10']:,.2f} to £{sim['daily_p90']:,.2f}).")
    else:
        typical = sim["daily_p50"] if sim else r_day * risk
        out["words"] = (f"Not supported at £{account:,.0f} and {risk_pct:g}% a trade: the {label.lower()} evidence "
                        f"({names}) points to about £{typical:,.2f} a day"
                        + (f" (range £{sim['daily_p10']:,.2f} to £{sim['daily_p90']:,.2f})" if sim else "")
                        + (f". £{milestone:,.0f} a day would take about £{need_equity:,.0f} of capital at "
                           f"{risk_pct:g}%" if need_equity else "") + ".")
    improve = []
    if not supported and r_day > 0:
        tpd = sum(c["per_day"] for c in useful)
        mean_r = r_day / tpd if tpd else 0.0
        if mean_r > 0:
            improve.append(f"about {milestone / (mean_r * risk):,.0f} trades a day at today's edge (now {tpd:.1f})")
        if tpd:
            improve.append(f"an edge of about {milestone / (tpd * risk):.2f}R a trade at today's pace "
                           f"(now {mean_r:.2f}R)")
        if need_equity:
            improve.append(f"about £{need_equity:,.0f} of capital at {risk_pct:g}%")
    if out["projection"]["uncertain"]:
        improve.append("more trades: the range of the average result is still wider than the result itself")
    out["improve"] = improve
    return out


# ------------------------------------------------------------------ the barriers --
def _corr(a: dict, b: dict) -> Optional[float]:
    common = sorted(set(a) & set(b))
    if len(common) < CORR_MIN_DAYS:
        return None
    x = [a[k] for k in common]
    y = [b[k] for k in common]
    mx, my = sum(x) / len(x), sum(y) / len(y)
    sx = math.sqrt(sum((v - mx) ** 2 for v in x))
    sy = math.sqrt(sum((v - my) ** 2 for v in y))
    if sx == 0 or sy == 0:
        return None
    return sum((p - mx) * (q - my) for p, q in zip(x, y)) / (sx * sy)


def barriers(ev: dict, projections: Sequence[dict], refresh: Optional[dict], demo_places: Optional[int] = None,
             demo_cap_pct: Optional[float] = None) -> list[dict]:
    """[{barrier, seen, doing}] - only the ones the records show. ``demo_places``: how many may be on Demo at
    once (``weekly.demo_target``); ``demo_cap_pct``: the demo account's combined open risk cap
    (``labdemo.engine.risk_cap_pct``), None when it could not be read."""
    from . import weekly as W
    from .sizing import MAX_COMBINED_PCT
    places = W.DEMO_TARGET if demo_places is None else int(demo_places)
    unknown = ev.get("DEMO_UNKNOWN") or []
    out = []
    every = ev.get("DEMO", []) + ev.get("PAPER", [])
    thin = [e for e in every if e["trades"] < MIN_TRADES]
    demo_n = sum(e["trades"] for e in ev.get("DEMO", []))
    if not ev.get("DEMO") or demo_n < MIN_TRADES or thin:
        if ev.get("DEMO"):
            seen = f"Demo has {demo_n} trade{'' if demo_n == 1 else 's'} in the last four weeks"
            if unknown:
                seen += f" ({len(unknown)} more on Demo waiting for MetaTrader's figures)"
        elif unknown:
            seen = f"Demo is waiting for MetaTrader's figures ({len(unknown)} on Demo not read yet)"
        else:
            seen = "Nothing is on Demo yet, so there is no broker evidence"
        if thin:
            seen += f"; {len(thin)} of the leading strategies have under {MIN_TRADES} trades"
        out.append({"barrier": "Too few trades", "seen": seen + ".",
                    "doing": f"Every test practises on paper; every UK trading day the best that pass the bar fill "
                             f"the free places on Demo (up to {places} at once), and the conveyor belt keeps fresh "
                             f"challengers coming."})
    heavy = [e for e in every if e.get("costs_share") is not None and e["costs_share"] >= COSTS_HEAVY]
    if heavy:
        e = heavy[0]
        out.append({"barrier": "Costs eating the edge",
                    "seen": f"{e['nick']} pays {e['costs_share'] * 100:.0f}% of what it makes before costs in "
                            f"spread and commission" + (f" (and {len(heavy) - 1} more)" if len(heavy) > 1 else "") + ".",
                    "doing": "Results are judged after costs only; the Saturday refresh benches tests that do not "
                             "beat their costs, and variants try wider targets."})
    pairs = []
    for league in ("DEMO", "PAPER"):
        lst = ev.get(league, [])
        for i in range(len(lst)):
            for j in range(i + 1, len(lst)):
                c = _corr(lst[i]["day_nets"], lst[j]["day_nets"])
                if c is not None and c >= CORR_HIGH:
                    pairs.append((c, lst[i]["nick"], lst[j]["nick"]))
    if pairs:
        c, a, b = max(pairs)
        out.append({"barrier": "Strategies moving together",
                    "seen": f"{a} and {b} win and lose on the same days (correlation {c:.2f}), so together they "
                            f"add risk faster than profit.",
                    "doing": ((f"On Demo, a lab idea's new trade is sized down or skipped so the account's combined "
                               f"open risk stays within {demo_cap_pct:g}% of equity; " if demo_cap_pct is not None
                               else "On Demo, the account's combined open risk cap applies to lab ideas' new trades; ")
                              + f"the proposed real-money plan caps planned risk at {MAX_COMBINED_PCT:g}% at once. "
                                f"New families are chosen for different markets and sessions.")})
    deep = [p for p in projections if (p.get("projection") or {}).get("dd_pct_p90") is not None
            and p["projection"]["dd_pct_p90"] > DD_LIMIT_PCT]
    if deep:
        p = deep[0]
        out.append({"barrier": "Drawdown",
                    "seen": f"On the {p['league'].lower()} evidence a bad month could fall "
                            f"{p['projection']['dd_pct_p90']:.0f}% from its peak at £{p['account']:,.0f}.",
                    "doing": f"Daily (2%), weekly (5%) and overall (10%) safeguards are proposed for real money, and "
                             f"a demo strategy goes back to Paper when its drawdown from its best reaches "
                             f"{W.DD_GUARD_RISKS:g} times its risk a trade."})
    if refresh:
        added = sum(len(refresh.get(k) or []) if isinstance(refresh.get(k), list) else 0 for k in ("new", "variants"))
        benched = len(refresh.get("retired") or []) if isinstance(refresh.get("retired"), list) else 0
        if added or benched:
            for b in out:
                b["doing"] += f" Last Saturday: {added} fresh challengers in, {benched} benched."
                break
    return out


# ------------------------------------------------------------------ the whole road --
def road(world: dict, built: dict, now: Optional[dt.datetime] = None, equity: Optional[float] = None,
         account: float = 1500.0, risk_pct: float = 0.5, demo_places: Optional[int] = None,
         demo_cap_pct: Optional[float] = None) -> dict:
    """Everything the ROAD TO PROFIT page shows. Read-only. While a demo record cannot be read (MetaTrader
    unread) the demo set says it is waiting for MetaTrader's figures: no projection, never "0 demo trades"."""
    now = to_utc(now or utcnow())
    ev = evidence(world, built, now)
    ms = milestones(equity if equity is not None else account)
    rows = []
    projections = []
    for m in ms:
        per = {}
        for league in ("DEMO", "PAPER"):
            p = project(ev[league], m, account, risk_pct, league)
            if league == "DEMO" and ev.get("DEMO_UNKNOWN"):
                p = dict(p, projection=None, supported=False, improve=[],
                         words="Waiting for MetaTrader's figures: the demo evidence appears once MetaTrader is read.")
            per[league] = p
            projections.append(p)
        rows.append({"milestone": m, "DEMO": per["DEMO"], "PAPER": per["PAPER"]})
    best = None
    for league in ("DEMO", "PAPER"):
        cands = [e for e in ev[league] if e.get("mean_r") is not None and e["mean_r"] > 0
                 and e["trades"] >= STRAT_MIN_TRADES]
        if cands:
            e = max(cands, key=lambda e: (e["mean_r"] * e["per_day"], e["id"]))
            from .sizing import risk_gbp
            day = e["mean_r"] * e["per_day"] * risk_gbp(account, risk_pct)
            need = []
            if e["trades"] < MIN_TRADES:
                need.append(f"{MIN_TRADES - e['trades']} more trades")
            if league == "PAPER":
                need.append("to prove it on Demo")
            if e.get("r_low") is not None and e["r_low"] <= 0:
                need.append("a steadier edge (its cautious result is not yet above zero)")
            best = {"id": e["id"], "nick": e["nick"], "league": league, "mean_r": round(e["mean_r"], 3),
                    "mean_net": e.get("mean_net"),
                    "per_day": e["per_day"], "trades": e["trades"], "day_at_account": round(day, 2),
                    "needs": need or ["more of the same: keep the trades coming"]}
            break
    return {"now": now, "account": account, "risk_pct": risk_pct, "milestones": rows, "evidence": ev,
            "best": best, "barriers": barriers(ev, projections, world.get("refresh"), demo_places, demo_cap_pct),
            "window_days": WINDOW_DAYS}
