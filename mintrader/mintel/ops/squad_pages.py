"""TRADING SQUAD: the leagues, the ROAD TO PROFIT, live readiness and a strategy's career (Aaron, 14 Oct 2026).

    "/leagues"   PAPER LEAGUE, DEMO LEAGUE, REAL MONEY LEAGUE ("Not connected yet") and the READINESS section
                 (the approval cards: their buttons only show the command to run on the Mac)
    "/road"      ROAD TO PROFIT: the milestones, the realistic £1,500 account, sizes by account, the barriers
    "/bot?id="   (squad.render_bot_page) gains the CAREER across the leagues
    "/"          (squad.render_squad) gains three league lines and the ROAD TO PROFIT and LIVE READINESS cards

PRESENTATION ONLY, in the existing squad style (squad.SQUAD_CSS, squad.shell): nothing here writes a file,
changes a mode or touches an order path. Every figure comes from leagues / roadmap / readiness / sizing, which
read the records. Paper, demo and real are never added together on any of these pages.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional
from urllib.parse import quote

from ..clock import to_utc
from . import squad as SQ
from .standing import uk_date

log = logging.getLogger("mintel.squad_pages")
_e, _money, _gbp, _pct, _tq, _d = SQ._e, SQ._money, SQ._gbp, SQ._pct, SQ._tq, SQ._d



# ======================================================================= the figures, once ==
_CACHE: dict = {}


def extras(b: dict, data_dir, sd: Optional[dict] = None, deals=None, config_path=None, test_id: str = "",
           cache_seconds: float = 60.0) -> dict:
    """Everything the new pages and cards need, worked out from the records (read-only), at most once every
    ``cache_seconds`` for the heavy part (the road's resampling): {period (the leagues in the board's
    timeline), all (since the reset), road, readiness, career (when ``test_id``), equity}. Never raises."""
    from . import leagues as L
    from . import readiness as RD
    from . import roadmap as RM
    from . import weekly as W
    out = {"period": None, "all": None, "road": None, "readiness": None, "career": None, "equity": None}
    if not data_dir:
        return out
    now = to_utc(b["now"])
    try:
        world = SQ.load_world(data_dir, config_path, now, b["reset"])
        positions = (W.live_positions_json(deals, (sd or {}).get("open_tickets"))["positions"]
                     if deals is not None else None)
        lab_demo = L.lab_demo_trades(data_dir)
        out["period"] = L.build(world, data_dir, positions, b["start"], b["end"], lab_demo)
        out["all"] = built = L.build(world, data_dir, positions, b["reset"], now + dt.timedelta(seconds=1), lab_demo)
        eq = (sd or {}).get("equity")
        out["equity"] = float(eq) if isinstance(eq, (int, float)) else None
        key = (str(data_dir), positions is not None, len(positions or ()))
        hit = _CACHE.get(key)
        if hit and 0 <= (now - hit[0]).total_seconds() < cache_seconds:
            out["road"], out["readiness"] = hit[1], hit[2]
        else:
            try:
                from ..labdemo.engine import risk_cap_pct
                demo_cap = risk_cap_pct(data_dir)
            except Exception:                # the cap's figure is left out, never guessed
                demo_cap = None
            out["road"] = RM.road(world, built, now, equity=out["equity"], demo_places=W.demo_target(data_dir),
                                  demo_cap_pct=demo_cap)
            out["readiness"] = _with_pace(RD.review(world, built, data_dir, now, previous=RD.read(data_dir)),
                                          built, now)
            if len(_CACHE) > 8:
                _CACHE.clear()
            _CACHE[key] = (now, out["road"], out["readiness"])
        if test_id:
            out["career"] = L.career(test_id, world, data_dir, built, snapshots=L.read_snapshots(data_dir, 60),
                                     now=now)
    except Exception as exc:                 # never a reason to fail the page
        log.warning("squad pages: %s", exc)
    return out


# ======================================================================= the front page bits ==
def league_lines(b: dict) -> str:
    """Three compact lines: PAPER +£x | N strategies; DEMO +£y | N active (the broker's); REAL Not connected yet.
    The paper figure is the PAPER PROFIT card's, the demo figure the DEMO PROFIT card's: never added."""
    h = b["head"]
    paper = (f'<a href="/leagues{_tq(b["timeline"])}#paper" data-league="PAPER"><span class="lk">PAPER</span>'
             f'<span class="lv">{_money(h["net"]) if h["trades"] else _money(0.0)}</span>'
             f'<span class="ln">{h["traded"]} strateg{"y" if h["traded"] == 1 else "ies"}</span></a>')
    d = b.get("demo")
    active = len(b.get("demo_rows") or [])
    demo = (f'<a class="demo" href="/leagues{_tq(b["timeline"])}#demo" data-league="DEMO"><span class="lk">DEMO</span>'
            f'<span class="lv">{_money(d["net"]) if d else _money(None)}</span>'
            f'<span class="ln">{active} active · the broker\'s figures</span></a>')
    real = ('<a class="real" href="/leagues#real" data-league="REAL"><span class="lk">REAL</span>'
            '<span class="lv">Not connected yet</span></a>')
    return f'<section class="lgs rise d1" id="league-lines">{paper}{demo}{real}</section>'


def road_card(x: Optional[dict]) -> str:
    """ROAD TO PROFIT: the strongest current opportunity and what it needs - from the road, or nothing yet."""
    road = (x or {}).get("road")
    best = (road or {}).get("best")
    if best:
        league = "demo" if best["league"] == "DEMO" else "paper"
        inner = (f'<p class="big">{_e(best["nick"])} <span class="bdg {"gold" if league == "demo" else "plain"}">'
                 f'{_e(league.upper())}</span></p>'
                 f'<p>{_e(_gbp(best.get("mean_net")))} a trade after costs, {best["per_day"]:g} trades a day: about '
                 f'{_e(_gbp(best["day_at_account"]))} a day on £1,500 at 0.5% risk.</p>'
                 f'<p>Next: {_e("; ".join(best["needs"]))}.</p>')
    else:
        inner = ('<p class="big">Building the evidence</p><p>No strategy has enough trades yet to show a road. '
                 'Every test keeps practising.</p>')
    return f'<div class="mini rise d4" id="road"><h2>ROAD TO PROFIT</h2>{inner}<a class="mini-go" href="/road">' \
           f'See the road →</a></div>'


def _stage_cls(stage: str) -> str:
    from . import readiness as RD
    return {RD.APPROACHING: "approaching", RD.PREPARATION: "prep", RD.QUALIFIED: "qualified"}.get(stage, "")


def readiness_card(x: Optional[dict]) -> str:
    from . import readiness as RD
    rd = (x or {}).get("readiness")
    stage = (rd or {}).get("stage") or RD.NONE
    if stage == RD.NONE or not (rd or {}).get("bots"):
        inner = ('<span class="stage">NO ACTION REQUIRED YET</span><p>No strategy is near real money yet. Demo '
                 'keeps gathering evidence.</p>')
    else:
        bt = rd["bots"][0]
        inner = (f'<span class="stage {_stage_cls(stage)}" data-stage="{_e(stage)}">{_e(RD.STAGE_WORDS[stage].upper())}'
                 f'</span><p class="big">{_e(bt["nick"])}</p><p>{_e(_gbp(bt["demo"]["net"]))} on demo over '
                 f'{bt["demo"]["trades"]} trades after costs. Earliest: {_e(bt["earliest"])}.</p>')
    return (f'<div class="mini rise d4" id="readiness-card"><h2>LIVE READINESS</h2>{inner}'
            f'<a class="mini-go" href="/leagues#readiness">See readiness →</a></div>')


# ======================================================================= /leagues ==
def _q_cells(r: dict) -> str:
    q = r.get("quality") or {}
    known = r.get("known", True)                 # unknown while MetaTrader is unread: "—", never 0
    cons = q.get("consistency") if known else None
    ntd = q.get("net_to_dd") if known else None
    return (f'<div class="lq-stats"><div class="lq-c" data-col="trades">{r["trades"] if known else "—"}<small> trades'
            f'</small></div>'
            f'<div class="lq-c" data-col="updays">{"—" if cons is None else f"{cons * 100:.0f}%"}<small> up days</small>'
            f'</div><div class="lq-c" data-col="netdd">{"—" if ntd is None else f"{ntd:.1f}×"}<small> net / drawdown'
            f'</small></div><div class="lq-c">{_stars_html(q)}</div></div>')


def _stars_html(q: dict) -> str:
    if not q or q.get("early"):
        return '<span class="sq-f" title="too few trades to judge yet">🧪 early</span>'
    return (f'<span class="stars" title="profitable · consistent · risk-adjusted · active">'
            f'{SQ._e(_stars(q.get("stars")))}</span>')


def _stars(n) -> str:
    from .leagues import stars_text
    return stars_text(n or 0)


def _lq_head() -> str:
    return ('<div class="lq-head"><span>POS</span><span>BOT</span><span class="r">PROFIT</span><span class="r">TRADES'
            '</span><span class="r">UP DAYS</span><span class="r">NET/DD</span><span class="r">QUALITY</span></div>')


def _lq_row(r: dict, timeline: str) -> str:
    pos = "–" if r.get("pos") is None else str(r["pos"])
    known = r.get("known", True)
    return (f'<a class="lq-row" href="/bot?id={quote(str(r["id"]))}{_tq(timeline).replace("?", "&")}" '
            f'data-id="{_e(r["id"])}"><b class="p">{_e(pos)}</b><div class="lg-bot"><div class="lg-name">'
            f'{_e(r.get("nick") or r["id"])}</div><div class="lg-desc">{_e(r.get("desc") or "")}</div></div>'
            f'<div class="lg-money">{_money(r["net"]) if known else _money(None)}</div>{_q_cells(r)}</a>')


def render_leagues_page(b: dict, x: Optional[dict], health=("ok", "running"), show_all: bool = False) -> str:
    """"/leagues": the three leagues apart, sorted by profit with the quality behind it, then READINESS."""
    from . import leagues as L
    built = (x or {}).get("period") or {}
    p = (built.get(L.PAPER) or {"rows": [], "total": {"net": 0.0, "strategies": 0, "trades": 0}})
    d = (built.get(L.DEMO) or {"rows": [], "total": {"net": 0.0, "strategies": 0, "trades": 0, "active": 0}})
    rows = p["rows"] if show_all else p["rows"][:SQ.TOP_SHOWN]
    more = ""
    if len(p["rows"]) > SQ.TOP_SHOWN and not show_all:
        more = (f'<div class="lg-more"><a href="/leagues{_tq(b["timeline"], "all=1")}#paper">Show all '
                f'{len(p["rows"])}</a></div>')
    paper_html = (_lq_head() + "".join(_lq_row(r, b["timeline"]) for r in rows) + more) if rows else (
        '<div class="sq-empty"><b>No paper trades in this period yet</b>The league fills up as trades close.</div>')
    pt = p["total"]
    sec_paper = (f'<section class="sq-sec rise d1" id="paper"><div class="sq-h2"><h2>PAPER LEAGUE</h2><small>'
                 f'{_e(_gbp(pt["net"]))} · {pt["strategies"]} strategies · practice money after costs · sorted by '
                 f'profit, quality alongside</small></div><div class="sq-panel">{paper_html}</div></section>')
    dt_ = d["total"]
    if d["rows"]:
        demo_inner = _lq_head() + "".join(_lq_row(r, b["timeline"]) for r in d["rows"])
        sub = ("the broker's results after all costs" if dt_.get("known", True) else
               "waiting for MetaTrader's figures")
        sec_demo = (f'<section class="sq-sec rise d2" id="demo"><div class="team"><div class="team-head"><h2>DEMO '
                    f'LEAGUE</h2><small>{_e(_gbp(dt_["net"]) if dt_.get("known", True) else "—")} · '
                    f'{dt_.get("active", 0)} active · {_e(sub)}</small>'
                    f'</div>{demo_inner}</div></section>')
    else:
        sec_demo = ('<section class="sq-sec rise d2" id="demo"><div class="team"><div class="team-head"><h2>DEMO '
                    'LEAGUE</h2><small>the broker\'s results after all costs</small></div><div class="sq-empty" '
                    'style="color:var(--team-dim)"><b style="color:var(--team-text)">Nobody on Demo in this period'
                    '</b>Tests that pass the bar move up every Friday.</div></div></section>')
    sec_real = (f'<section class="sq-sec rise d3" id="real"><div class="sq-h2"><h2>REAL MONEY LEAGUE</h2></div>'
                f'<div class="realbox"><b>Not connected yet</b>No real money is ever traded without your approval.'
                f'</div></section>')
    key = ('<p class="sq-f" style="margin:-12px 0 24px">Quality ★: in profit · up on most of its days · '
           'net at least twice its worst drawdown · at least a trade a day. Paper, Demo and Real are never added '
           'together.</p>')
    body = (SQ._hero("THE <span>LEAGUES</span>", b, "/leagues") + sec_paper + sec_demo + sec_real + key
            + render_readiness(x))
    return SQ.shell("Leagues · Trading Squad", body, "leagues", health, SQ._stamp(b["now"]))


def _with_pace(rd: dict, built: dict, now: dt.datetime) -> dict:
    """Each readiness card's expected frequency (``per_day``): its demo trades a UK trading day from its first
    demo trade to today (``leagues.quality``), the pace its proposed activation date is worked out at - never
    its trades on the days it happened to trade. None (shown "—") for a card carried over while its record
    cannot be read."""
    from . import leagues as L
    for bt in (rd or {}).get("bots") or ():
        tr = (built.get("demo_trades") or {}).get(bt.get("id"))
        known = (built.get("demo_known") or {}).get(bt.get("id"), True)
        bt["per_day"] = L.quality(tr, now)["per_day"] if tr and known and not bt.get("stale") else None
    return rd


def render_readiness(x: Optional[dict]) -> str:
    """READINESS: the stage, the bar, and a card for each strategy near real money; the approval card (APPROVE
    LIVE / KEEP ON DEMO / REJECT) only for a qualified one, and its buttons show the command, nothing more."""
    from . import readiness as RD
    from . import realmoney as RM
    rd = (x or {}).get("readiness") or {"stage": RD.NONE, "bots": []}
    stage = rd.get("stage") or RD.NONE
    head = (f'<div class="sq-h2"><h2>READINESS</h2><small>Real money stays switched off in the code. Approval is '
            f'yours alone, at the Mac.</small></div>')
    top = (f'<div class="box muted" style="margin-bottom:14px"><span class="stage {_stage_cls(stage)}">'
           f'{_e(RD.STAGE_WORDS[stage].upper())}</span><p><b>The bar:</b> {_e(RD.BAR_TEXT)}.</p></div>')
    cards = []
    for bt in rd.get("bots") or ():
        dm, pp = bt["demo"], bt["paper"]
        best = "; ".join(bt.get("promising") or []) or "—"
        need = bt.get("needed") or []
        per_day = bt.get("per_day")             # trades a trading day since its first demo trade (_with_pace)
        sim = bt.get("sim_1500") or {}
        lines = (f'<div class="trio"><div><span>DEMO</span><b>{_money(dm["net"])}</b></div><div><span>PAPER</span>'
                 f'<b>{_money(pp["net"])}</b></div><div><span>TRADES</span><b>{dm["trades"]}'
                 f'<span class="zero"> / {pp["trades"]}</span></b></div></div>'
                 f'<p>Proposed initial risk: <b>0.5%</b> a trade (£7.50 on £1,500). Expected frequency: '
                 f'{"—" if per_day is None else f"{per_day:.1f} trades a day"}. Worst drawdown: '
                 f'{_e(_gbp(dm["max_dd"], False))} on demo'
                 + (f', {sim["max_dd_pct"]:.1f}% on a £1,500 account' if sim.get("max_dd_pct") is not None else "")
                 + f'.</p><p>Why it is promising: {_e(best)}.</p>'
                 f'<p>Still needed: {_e("; ".join(need) if need else "nothing, it meets the bar")}.</p>'
                 f'<p>£1,500 justified: {_e(bt.get("justified_words") or "")}. Proposed activation: '
                 f'{_e(bt.get("earliest") or "no estimate yet")}.</p>')
        acts = ""
        if bt["stage"] == RD.QUALIFIED:
            acts = '<div class="rm-acts" data-approval="command-only">' + "".join(
                f'<details class="{cls}"><summary>{_e(label)}</summary><code>On the Mac, in Terminal:\n'
                f'{_e(RM.command_for(dec, bt["id"], bt["version"]))}</code></details>'
                for dec, label, cls in ((RM.APPROVE, "APPROVE LIVE", "ok"), (RM.KEEP_DEMO, "KEEP ON DEMO", ""),
                                        (RM.REJECT, "REJECT", "no"))) + '</div>'
        cards.append(f'<div class="rm" data-bot="{_e(bt["id"])}"><span class="stage {_stage_cls(bt["stage"])}">'
                     f'{_e(RD.STAGE_WORDS[bt["stage"]].upper())}</span><h3>{_e(bt["nick"])}</h3>'
                     f'<div class="v">{_e(bt["id"])} · version {_e(bt["version"])}</div>{lines}{acts}</div>')
    grid = f'<div class="cards2">{"".join(cards)}</div>' if cards else ""
    return f'<section class="sq-sec rise d4" id="readiness">{head}{top}{grid}</section>'


# ======================================================================= /road ==
def _daily(sim: Optional[dict]) -> str:
    if not sim:
        return ""
    return (f'<div class="trio"><div><span>DOWNSIDE</span><b>{_money(sim["daily_p10"])}</b></div><div><span>TYPICAL'
            f'</span><b>{_money(sim["daily_p50"])}</b></div><div><span>FAVOURABLE</span><b>{_money(sim["daily_p90"])}'
            f'</b></div></div><p class="sq-f">A day: their own trades resampled over {sim["days"]} trading days at '
            f'{_e(_gbp(sim["risk"], False))} a trade (the 10th, 50th and 90th percentiles).</p>')


def _evidence_box(p: dict) -> str:
    """What one evidence set shows at £1,500 and 0.5% (the same for every milestone): its strategies, the
    resampled days, the drawdown and how sure it is - or why there is nothing to show yet."""
    cls = "demo" if p["league"] == "DEMO" else ""
    title = "DEMO EVIDENCE · the broker's" if p["league"] == "DEMO" else "PAPER EVIDENCE · practice"
    pr = p.get("projection") or {}
    if not pr:
        return (f'<div class="box {cls}" data-evidence="{_e(p["league"])}"><h3>{_e(title.upper())}</h3>'
                f'<p>{_e(p["words"])}</p></div>')
    sim = pr.get("sim")
    facts = []
    if pr.get("dd_pct_p50") is not None:
        facts.append(f"Drawdown along the way: {pr['dd_pct_p50']:.1f}% typical, {pr['dd_pct_p90']:.1f}% in a bad run")
    facts.append(f"Evidence: {p['trades']} trades over {p['days']} days"
                 + (f"; the average result is known to within {pr['interval_width_r']:.2f}R (80% range)"
                    if pr.get("interval_width_r") is not None else ""))
    if pr.get("uncertain"):
        facts.append("Still uncertain: the range of the average is wider than the average itself")
    who = "".join(f"<li>{_e(c['nick'])}: {_e(_gbp(c['mean_net']))} a trade after costs, {c['per_day']:g} a day, "
                  f"{c['trades']} trades</li>" for c in p["contributors"][:5])
    return (f'<div class="box {cls}" data-evidence="{_e(p["league"])}"><h3>{_e(title.upper())}</h3>{_daily(sim)}'
            f'<ul class="ul">{who}</ul><ul class="ul">' + "".join(f"<li>{_e(f)}</li>" for f in facts) + "</ul></div>")


def _verdict(p: dict) -> str:
    pr = p.get("projection") or {}
    lab = "DEMO" if p["league"] == "DEMO" else "PAPER"
    bits = [f'<span class="lab">{lab}</span>']
    if p["supported"]:
        bits.append(f'<p><b>Supported.</b> Typical day £{pr["sim"]["daily_p50"]:,.2f}; downside '
                    f'£{pr["sim"]["daily_p10"]:,.2f}.</p>')
    elif not pr:
        bits.append(f'<p>{_e(p["words"])}</p>')
    else:
        bits.append(f'<p>Not at £{p["account"]:,.0f}: typical day £{(pr.get("sim") or {}).get("daily_p50", 0):,.2f}.</p>')
    if pr.get("equity_needed"):
        bits.append(f'<p>Capital for it at {p["risk_pct"]:g}%: about £{pr["equity_needed"]:,.0f}'
                    + (f' (EUR/USD {pr["eurusd_lots"]:g} lots, {_e(_gbp(pr["eurusd_per_pip"], False))} a pip)'
                       if pr.get("eurusd_lots") else "") + '.</p>')
    if p.get("improve") and pr:
        bits.append('<p class="sq-f">To get there: ' + _e("; ".join(p["improve"])) + '.</p>')
    return "<div>" + "".join(bits) + "</div>"


def _sim_box(label: str, sim: Optional[dict], recs: dict, nick: dict) -> str:
    cls = "demo" if label == "DEMO" else ""
    title = "DEMO STRATEGIES" if label == "DEMO" else "TOP PAPER CANDIDATES (paper)"
    if not sim or not sim.get("taken"):
        return (f'<div class="box {cls}"><h3>{_e(title)}</h3><p>No trades with their risk on record to replay yet.'
                f'</p></div>')
    rows = "".join(f'<li>{_e(nick.get(k, k))}: {v["taken"]} trades, {_e(_gbp(v["net"]))}'
                   + (f' — {_e(recs[k]["words"])}' if k in recs else "") + '</li>'
                   for k, v in sorted(sim["by"].items(), key=lambda kv: -kv[1]["net"]))
    skipped = ", ".join(f"{v} {k}" for k, v in sim.get("skipped", {}).items())
    return (f'<div class="box {cls}"><h3>{_e(title)}</h3><div class="trio"><div><span>RESULT</span>'
            f'<b>{_money(sim["net"])}</b></div><div><span>WORST DRAWDOWN</span><b>{sim["max_dd_pct"]:.1f}%</b></div>'
            f'<div><span>WORST DAY</span><b>{_money(sim["worst_day"])}</b></div></div><ul class="ul">{rows}</ul>'
            + (f'<p class="sq-f" style="margin-top:8px">Left out by the rules: {_e(skipped)}.</p>' if skipped else "")
            + ('<p class="sq-f">The 10% safeguard stopped it.</p>' if sim.get("halted") else "") + '</div>')


def _balance_table(t: Optional[dict]) -> str:
    if not t:
        return ""
    bal = t["balances"]
    cols = f"grid-template-columns:minmax(0,1.4fr) repeat({len(bal)},minmax(0,1fr))"
    head = '<div class="h">STRATEGY · £ A DAY</div>' + "".join(f'<div class="h r">£{b_:,.0f}</div>' for b_ in bal)
    rows = ""
    for s in t["strategies"][:6]:
        rows += (f'<div>{_e(s["nick"])} <span class="bdg {"gold" if s["league"] == "DEMO" else "plain"}">'
                 f'{_e(s["league"])}</span></div>'
                 + "".join(f'<div class="r">{_money(c["per_day"])}</div>' for c in s["cells"]))
    risk = '<div class="h">RISK A TRADE</div>' + "".join(
        f'<div class="h r">£{b_ * t["risk_pct"] / 100:,.2f}</div>' for b_ in bal)
    inst = '<div class="h">£ A PIP AT A TYPICAL STOP</div>' + "".join('<div class="h r"></div>' for _ in bal)
    for i in t["instruments"]:
        inst += f'<div>{_e(i["name"])} <span class="sq-f">per {_e(i["unit"])}</span></div>' + "".join(
            f'<div class="r">{_e(_gbp(c["per_pip"], False)) if c["ok"] else "too small"}</div>' for c in i["cells"])
    return (f'<div class="sq-panel" style="padding:4px 6px"><div class="grid-t" style="{cols}">{head}{risk}{rows}{inst}'
            f'</div></div><p class="sq-f">Realistic £ a day = each strategy\'s own average R a trade × the risk at that '
            f'balance × its trades a day (strategies with 10+ trades). £ a pip from reference instrument figures at '
            f'{t["risk_pct"]:g}% risk; the '
            f'live sizing always uses the broker\'s own. Exposure scales with the account both ways.</p>')


def road_extras(x: Optional[dict]) -> dict:
    """The £1,500 account replays (demo and paper apart), tier recommendations and the balance table."""
    from . import roadmap as RM
    from . import sizing as Z
    from . import weekly as W
    road = (x or {}).get("road") or {}
    built = (x or {}).get("all") or {}
    ev = road.get("evidence") or {"DEMO": [], "PAPER": []}
    stated = {s["id"]: s.get("risk_gbp") for s in built.get("stints") or () if s.get("risk_gbp")}
    demo_by = {e["id"]: [t for t in (built.get("demo_trades") or {}).get(e["id"]) or ()] for e in ev["DEMO"]}
    paper_by = {e["id"]: e.get("_trades") or [] for e in ev["PAPER"]}
    sims = {"DEMO": Z.simulate(demo_by, fallback_risk=stated) if demo_by else None,
            "PAPER": Z.simulate(paper_by) if paper_by else None}
    recs = {}
    for e in ev["DEMO"]:
        nets = [float(t["net"]) for t in demo_by.get(e["id"]) or ()]
        _p, lcb = W.bootstrap(nets, e["id"]) if len(nets) >= 2 else (1.0, None)
        recs[e["id"]] = Z.tier_recommendation(len(nets), lcb)
    rows = [{"id": e["id"], "nick": e["nick"], "league": e["league"], "mean_r": e.get("mean_r"),
             "per_day": e.get("per_day")} for e in ev["DEMO"] + ev["PAPER"]
            if e.get("mean_r") is not None and e["trades"] >= RM.STRAT_MIN_TRADES]
    nick = {e["id"]: e["nick"] for e in ev["DEMO"] + ev["PAPER"]}
    return {"sims": sims, "recs": recs, "table": Z.balance_table(rows), "nick": nick}


def render_road_page(b: dict, x: Optional[dict], health=("ok", "running")) -> str:
    """"/road": ROAD TO PROFIT - the strongest opportunity, what the demo and the paper evidence each show, the
    milestones against them, the realistic £1,500 account, sizes by account size, and the barriers."""
    from . import sizing as Z
    road = (x or {}).get("road")
    hero = ('<section class="sq-hero rise"><div><h1>ROAD TO <span>PROFIT</span></h1><p>Ambitions, not targets: '
            'what the evidence says, demo first, paper second, never added together.</p></div></section>')
    if not road:
        body = hero + ('<div class="sq-panel sq-empty"><b>Building the evidence</b>The road appears once tests have '
                       'trades on record.</div>')
        return SQ.shell("Road to Profit · Trading Squad", body, "road", health, SQ._stamp(b["now"]))
    top = f'<section class="sq-duo two">{road_card(x)}{readiness_card(x)}</section>'
    first = road["milestones"][0]
    ev = (f'<section class="sq-sec rise d1" id="evidence"><div class="sq-h2"><h2>WHAT THE EVIDENCE SAYS</h2><small>'
          f'the last {road["window_days"]} days at £{road["account"]:,.0f} and {road["risk_pct"]:g}% a trade</small>'
          f'</div><div class="cards2">{_evidence_box(first["DEMO"])}{_evidence_box(first["PAPER"])}</div></section>')
    rows = []
    for m in road["milestones"]:
        ok_d, ok_p = m["DEMO"]["supported"], m["PAPER"]["supported"]
        chip = ('<span class="chip yes">SUPPORTED ON DEMO</span>' if ok_d else
                '<span class="chip yes">ON PAPER ONLY</span>' if ok_p else '<span class="chip no">NOT YET</span>')
        rows.append(f'<div class="ms-row" data-milestone="{m["milestone"]:g}"><div><h2>£{m["milestone"]:,.0f}</h2>'
                    f'<span class="sq-f">a day</span><br>{chip}</div>{_verdict(m["DEMO"])}{_verdict(m["PAPER"])}</div>')
    ms = (f'<section class="sq-sec rise d2" id="milestones"><div class="sq-h2"><h2>MILESTONES</h2><small>net average a '
          f'day after costs</small></div><div class="sq-panel">{"".join(rows)}</div></section>')
    ex = road_extras(x)
    sims = (f'<section class="sq-sec rise d3" id="sim1500"><div class="sq-h2"><h2>A REALISTIC £1,500 ACCOUNT</h2>'
            f'<small>their recorded trades replayed at {Z.TIER_START:g}% a trade, compounding both ways; demo and '
            f'paper apart</small></div><div class="cards2">{_sim_box("DEMO", ex["sims"]["DEMO"], ex["recs"], ex["nick"])}'
            f'{_sim_box("PAPER", ex["sims"]["PAPER"], {}, ex["nick"])}</div>'
            f'<div class="box muted" style="margin-top:14px"><h3>THE PROPOSED REAL-MONEY SETTINGS (for review)</h3>'
            f'<ul class="ul"><li>{Z.TIER_START:g}% risk a trade to start; {Z.TIER_ESTABLISHED:g}% for an established '
            f'performer (a recommendation only); {Z.TIER_APPROVAL:g}% only with your separate approval</li>'
            f'<li>At most {Z.MAX_COMBINED_PCT:g}% planned risk open at once</li><li>Stop for the day at a '
            f'{Z.SAFEGUARDS["daily_pct"]:g}% loss, for the week at {Z.SAFEGUARDS["weekly_pct"]:g}%, and altogether at '
            f'{Z.SAFEGUARDS["overall_pct"]:g}% down from the peak</li><li>A higher tier needs {Z.TIER_TRADES}+ demo '
            f'trades and a cautious per-trade result above zero; a streak alone is never enough. Every increase needs '
            f'your approval.</li></ul></div></section>')
    table = (f'<section class="sq-sec rise d3" id="balances"><div class="sq-h2"><h2>BY ACCOUNT SIZE</h2><small>the '
             f'same evidence at £1,500, £3,000, £5,000 and £10,000</small></div>{_balance_table(ex["table"])}</section>')
    bars = road.get("barriers") or []
    bar_html = "".join(f'<div class="box"><h3>{_e(x_["barrier"].upper())}</h3><p>{_e(x_["seen"])}</p>'
                       f'<p><b>What is being done:</b> {_e(x_["doing"])}</p></div>' for x_ in bars) or (
        '<div class="box"><p>No barrier stands out in the records right now.</p></div>')
    barriers = (f'<section class="sq-sec rise d4" id="barriers"><div class="sq-h2"><h2>BARRIERS</h2><small>what the '
                f'records show, and what is being done</small></div><div class="cards2">{bar_html}</div></section>')
    note = ('<p class="sq-f">R is a trade\'s result as a multiple of the money it risked (+0.5R on a £7.50 risk is '
            '+£3.75). Projections are the record resampled, never a promise.</p>')
    body = hero + top + ev + ms + sims + table + barriers + note
    return SQ.shell("Road to Profit · Trading Squad", body, "road", health, SQ._stamp(b["now"]))


# ======================================================================= a career ==
def _league_box(lg: Optional[dict], kind: str) -> str:
    from . import leagues as L
    if kind == L.REAL:
        return ('<div class="box muted"><h3>REAL MONEY</h3><p><b>Not connected yet</b></p><p class="sq-f">No real '
                'money is ever traded without your approval.</p></div>')
    if lg is None:
        return ('<div class="box demo"><h3>DEMO</h3><p><b>Not on Demo yet</b></p><p class="sq-f">It moves up only '
                'once it passes the bar.</p></div>' if kind == L.DEMO else
                '<div class="box"><h3>PAPER</h3><p>No paper trades yet.</p></div>')
    q = lg["quality"]
    known = lg.get("known", True)
    cls = "demo" if kind == L.DEMO else ""
    title = "DEMO · the broker's" if kind == L.DEMO else "PAPER · practice"
    risk = lg.get("risk") or {}
    cons = q.get("consistency") if known else None
    return (f'<div class="box {cls}" data-career="{kind}"><h3>{_e(title.upper())}</h3><div class="trio"><div><span>MADE'
            f'</span><b>{_money(q["net"]) if known else _money(None)}</b></div><div><span>TRADES</span><b>'
            f'{q["trades"] if known else "—"}</b></div><div><span>UP DAYS</span><b>'
            f'{"—" if cons is None else f"{cons * 100:.0f}%"}</b></div></div>'
            f'<p>Worst drawdown {_e(_gbp(q["max_dd"], False)) if known else "—"}'
            + (f' · net {q["net_to_dd"]:.1f}× it' if known and q.get("net_to_dd") is not None else "")
            + (f' · risk a trade about {_e(_gbp(risk["median"], False))}' if risk.get("median") else "")
            + f'</p><p>{_stars_html(q)}</p></div>')


def _conds(lg: Optional[dict]) -> str:
    if not lg:
        return ""
    c = lg["conditions"]
    words = {"market": "", "session": "the ", "condition": "a "}
    best = "; ".join(f"{words[r['what']]}{r['label']} {_gbp(r['net'])} ({r['trades']})" for r in c["best"]) or "—"
    worst = "; ".join(f"{words[r['what']]}{r['label']} {_gbp(r['net'])} ({r['trades']})" for r in c["worst"]) or "—"
    return f'<p><b>Best:</b> {_e(best)}</p><p><b>Worst:</b> {_e(worst)}</p>'


def render_career(c: Optional[dict]) -> str:
    """CAREER: the three leagues side by side, the timeline of promotions and demotions with their reasons, the
    versions, the best and worst conditions, and the ranking history."""
    from . import leagues as L
    if not c:
        return ""
    lg = c["leagues"]
    boxes = (f'<div class="cards3">{_league_box(lg.get(L.PAPER), L.PAPER)}{_league_box(lg.get(L.DEMO), L.DEMO)}'
             f'{_league_box(lg.get(L.REAL), L.REAL)}</div>')
    tl = "".join(f'<li class="{_e(e["kind"])}"><b>{_e(e["words"])}</b> · {_e(_d(uk_date(e["when"])))}'
                 + (f'<span>{_e(e["why"])}</span>' if e.get("why") else "") + "</li>" for e in c["timeline"][-12:])
    tl = f'<ul class="tl">{tl}</ul>' if tl else "<p>No promotions or demotions yet.</p>"
    vers = "".join(f'<li>{_e(v["version"])} — {_e(v["words"])}</li>' for v in c["versions"])
    conds = ""
    if lg.get(L.PAPER):
        conds += f'<p class="sq-f">PAPER</p>{_conds(lg[L.PAPER])}'
    if lg.get(L.DEMO) and lg[L.DEMO].get("trades"):
        conds += f'<p class="sq-f" style="margin-top:8px">DEMO</p>{_conds(lg[L.DEMO])}'
    ranks = c.get("ranks") or []
    rank_txt = " → ".join(f"#{r['paper']}" for r in ranks[-8:] if r.get("paper")) or "its daily positions appear here."
    return (f'<section class="sq-sec rise d4" id="career"><div class="sq-h2"><h2>CAREER</h2><small>since the reset, '
            f'every league apart · version {_e(c["version"])}' + (" · retired, kept on record" if c.get("retired") else "")
            + f'</small></div>{boxes}<div class="cards3" style="margin-top:14px"><div class="box"><h3>TIMELINE</h3>'
            f'{tl}</div><div class="box"><h3>BEST AND WORST CONDITIONS</h3>{conds or "<p>Too few trades yet.</p>"}</div>'
            f'<div class="box"><h3>VERSIONS</h3><ul class="ul">{vers}</ul><p class="sq-f" style="margin-top:8px">'
            f'Paper league position, day by day: {_e(rank_txt)}</p></div></div></section>')
