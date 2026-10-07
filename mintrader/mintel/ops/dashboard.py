"""Status, results and "what it is thinking" pages.

Deliberately built on Python's standard-library HTTP server: no web framework,
no extra dependency, nothing else to install or break on a VPS at 3am.

Three pages, all written in plain English rather than trader jargon:

* ``/``        - is it running?  Is it healthy?  What is it looking at?
* ``/results`` - what happened today, and why each trade entered and exited.
* ``/health``  - machine-readable health, for the watchdog and for curl.
"""
from __future__ import annotations

import datetime as dt
import html
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional

from ..clock import to_utc, utcnow

log = logging.getLogger("mintel.dashboard")


def _pct(v) -> str:
    return "-" if v is None else f"{float(v):.0f}%"


def _fmt_money(v: float, ccy: str = "") -> str:
    sign = "+" if v > 0 else ("-" if v < 0 else "")
    sym = {"GBP": "£", "USD": "$", "EUR": "€"}.get(ccy, "")
    return f"{sign}{sym}{abs(v):,.2f}"


class DashboardState:
    """Snapshot the web server reads.  The trader pushes into it.

    Kept separate from the Trader so a dashboard failure can never affect
    trading, and so the page can still explain what is wrong when the trader
    itself is unhealthy.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.status: dict = {"bot": "STARTING"}
        self.health: dict = {}
        self.thinking: list = []
        self.results: dict = {}
        self.positions: list = []
        self.events: list = []
        self.strategies: dict = {}       # Overall | existing | Rapid Scalper (attribution)
        self.period_resolver: Optional[Callable[[str, str, str], dict]] = None
        self.updated: Optional[dt.datetime] = None

    def update(self, **kwargs) -> None:
        with self.lock:
            for k, v in kwargs.items():
                setattr(self, k, v)
            self.updated = utcnow()

    def snapshot(self) -> dict:
        with self.lock:
            return {"status": self.status, "health": self.health,
                    "thinking": self.thinking, "results": self.results,
                    "positions": self.positions, "events": self.events,
                    "strategies": self.strategies,
                    "updated": self.updated.isoformat() if self.updated else None}


def build_results(journal, account_currency: str = "GBP",
                  now: Optional[dt.datetime] = None) -> dict:
    """Today's results in plain language, plus a per-trade story."""
    now = to_utc(now or utcnow())
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    trades = journal.closed_trades(limit=400, since=start)
    wins = [t for t in trades if (t.get("pnl_money") or 0) > 0]
    losses = [t for t in trades if (t.get("pnl_money") or 0) < 0]
    money_won = sum(t.get("pnl_money") or 0 for t in wins)
    money_lost = sum(t.get("pnl_money") or 0 for t in losses)
    pips = sum(t.get("pnl_pips") or 0 for t in trades)
    best = max(trades, key=lambda t: t.get("pnl_money") or 0, default=None)
    worst = min(trades, key=lambda t: t.get("pnl_money") or 0, default=None)

    def story(t: dict) -> dict:
        entry_json = {}
        try:
            entry_json = json.loads(t.get("entry_state_json") or "{}")
        except Exception:
            pass
        why = (entry_json.get("notes") or [])
        return {
            "symbol": t["symbol"],
            "direction": "bought" if t["side"] == "BUY" else "sold",
            "opened": t.get("opened_utc"), "closed": t.get("closed_utc"),
            "result_money": round(t.get("pnl_money") or 0, 2),
            "result_pips": round(t.get("pnl_pips") or 0, 1),
            "result_r": round(t.get("realised_r") or 0, 2),
            "why_it_entered": _why_entered(t, entry_json),
            "what_happened": _what_happened(t),
            "why_it_exited": t.get("exit_reason") or "the broker's stop or "
                                                     "target was reached",
            "what_it_learned": t.get("learned") or "",
            "tactic": t.get("tactic"), "regime": t.get("regime"),
            "score": t.get("opportunity"), "tier": t.get("tier"),
            "mfe_r": round(t.get("mfe_r") or 0, 2),
            "mae_r": round(t.get("mae_r") or 0, 2),
            "mfe_captured_pct": round(t.get("mfe_capture_pct") or 0, 1),
        }

    return {
        "date": start.date().isoformat(),
        "currency": account_currency,
        "trades": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else None,
        "pips": round(pips, 1),
        "money_won": round(money_won, 2),
        "money_lost": round(money_lost, 2),
        "net": round(money_won + money_lost, 2),
        "best_trade": story(best) if best else None,
        "worst_trade": story(worst) if worst else None,
        "trade_stories": [story(t) for t in trades[:40]],
    }


def _why_entered(t: dict, entry_json: dict) -> str:
    tactic = (t.get("tactic") or "").replace("_", " ").lower()
    regime = (t.get("regime") or "").replace("_", " ").lower()
    news = t.get("news_state") or ""
    verb = "bought" if t.get("side") == "BUY" else "sold"
    bits = [f"The market was behaving like a {regime} market, so the bot used "
            f"a {tactic} approach and {verb} it."]
    ev = entry_json.get("evidence") or {}
    if ev:
        top = sorted(ev.items(), key=lambda kv: -abs(kv[1] - 50))[:3]
        readable = ", ".join(f"{k.replace('_', ' ').lower()} {v:.0f}/100"
                             for k, v in top)
        bits.append(f"The strongest evidence was {readable}.")
    bits.append(f"It scored {t.get('opportunity')}/100 "
                f"({t.get('tier')}), which was the best available at the time.")
    if news and "no recent" not in news:
        bits.append(f"News context: {news}")
    if t.get("reward_risk"):
        bits.append(f"It offered about {t['reward_risk']:.1f} to 1 after costs.")
    return " ".join(bits)


def _what_happened(t: dict) -> str:
    mfe = t.get("mfe_r") or 0
    mae = t.get("mae_r") or 0
    r = t.get("realised_r") or 0
    bits = [f"It went {mae:.2f}R against the position at worst and "
            f"{mfe:.2f}R in favour at best."]
    if r > 0:
        bits.append(f"It finished {r:.2f}R up.")
    elif r < 0:
        bits.append(f"It finished {abs(r):.2f}R down.")
    else:
        bits.append("It finished flat.")
    if t.get("final_flow_state"):
        bits.append(f"FlowLock was in its "
                    f"{t['final_flow_state'].replace('_', ' ').lower()} state "
                    f"at the end.")
    return " ".join(bits)


# ------------------------------------------------------------------- pages ----

CSS = """
body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
margin:0;background:#f5f6f8;color:#1b1c1e;font-size:13px}
.wrap{max-width:1500px;margin:0 auto;padding:10px 14px}
h1{font-size:18px;margin:4px 0 8px}
h2{font-size:12px;margin:0 0 6px;text-transform:uppercase;letter-spacing:.5px;color:#374151}
.card,.box{background:#fff;border:1px solid #e2e4e8;border-radius:8px;
padding:8px 10px;margin-bottom:8px}
.row{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px}
.tile{flex:1 1 92px;background:#fff;border:1px solid #e2e4e8;border-radius:8px;
padding:5px 8px}
.tile .k,.st .k{font-size:10px;text-transform:uppercase;letter-spacing:.3px;color:#6b7280;
white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tile .v{font-size:13px;white-space:nowrap;font-weight:600;margin-top:2px}
.ok{color:#0a7c35}.bad{color:#b91c1c}.warn{color:#b45309}
.pill{display:inline-block;padding:1px 7px;border-radius:99px;font-size:11px;
font-weight:600;background:#eef0f3}
.pill.ok{background:#e7f6ec;color:#0a7c35}
.pill.bad{background:#fdeaea;color:#b91c1c}
.pill.warn{background:#fef4e6;color:#b45309}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:3px 5px;border-bottom:1px solid #eef0f3;
vertical-align:top}
th{font-size:10px;text-transform:uppercase;color:#6b7280;letter-spacing:.3px}
.mono{font-variant-numeric:tabular-nums}
.small{font-size:11px;color:#4b5563}
a{color:#1d4ed8;text-decoration:none}
.top{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.banner{padding:6px 12px;border-radius:8px;font-weight:600;flex:1 1 auto}
.banner.ok{background:#e7f6ec;color:#0a7c35;border:1px solid #b7e3c6}
.banner.bad{background:#fdeaea;color:#b91c1c;border:1px solid #f3c0c0}
nav{font-size:13px}
nav a{margin-right:12px}
.tabs{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-bottom:8px}
.tab{padding:5px 11px;border:1px solid #ccd;border-radius:7px;background:#fff;text-decoration:none;color:#223;font-weight:600}
.tab.on{background:#223;color:#fff}
.tab.p{padding:3px 8px;font-weight:500;font-size:12px}
.sep{width:1px;height:22px;background:#ccd;margin:0 4px}
.dates{display:flex;gap:5px;align-items:center;font-size:12px;margin-left:auto}
.dates input{padding:2px;border:1px solid #ccd;border-radius:6px;font-size:12px}
.dates button{padding:3px 9px;border:1px solid #ccd;border-radius:7px;background:#fff;font-weight:600;cursor:pointer}
.dates button.on{background:#223;color:#fff}
.rs-title{font-weight:700;letter-spacing:.08em;margin-bottom:2px}
.grid{display:grid;grid-template-columns:1.15fr 1fr 1fr;gap:8px;align-items:start}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:4px}
.st{border:1px solid #eef0f3;border-radius:6px;padding:3px 6px;min-width:0}
.st .v{font-size:13px;font-weight:600;font-variant-numeric:tabular-nums}
.st.big{grid-column:span 3;background:#f8fafc}
.st.big .v{font-size:18px}
.kv{display:grid;grid-template-columns:auto 1fr;gap:1px 10px;font-size:12px}
.kv .k{color:#6b7280}
details{margin-top:4px}
summary{cursor:pointer;font-size:11px;color:#1d4ed8}
td.nw{white-space:nowrap}
pre{font-size:11px;white-space:pre-wrap;margin:4px 0}
@media (max-width:1150px){.grid{grid-template-columns:1fr 1fr}}
@media (max-width:720px){.grid{grid-template-columns:1fr}.stats{grid-template-columns:repeat(2,1fr)}
.st.big{grid-column:span 2}.dates{margin-left:0}}
"""

# Remembers which folded sections were open, so the 10-second refresh does not
# close them again. Display only; nothing is sent anywhere.
KEEP_OPEN_JS = """<script>
(function(){var k='mintel-open';var o={};
try{o=JSON.parse(localStorage.getItem(k)||'{}')}catch(e){}
document.querySelectorAll('details[id]').forEach(function(d){
if(o[d.id])d.open=true;
d.addEventListener('toggle',function(){o[d.id]=d.open;
try{localStorage.setItem(k,JSON.stringify(o))}catch(e){}});});})();
</script>"""

RECENT_TRADES_SHOWN = 8


def _strategy_parts(snap: dict, selected: str = "overall") -> dict:
    """The strategy tabs and the selected strategy's figures, in pieces for the
    one-screen layout. Display only."""
    strategies = snap.get("strategies") or {}
    if not strategies:
        return {}
    labels = strategies.get("labels") or {}
    order = [k for k in ("overall", "market_intelligence", "rapid_scalper", "momentum_runner", "band_breaker") if k in strategies]
    if selected not in strategies:
        selected = "overall"
    period = strategies.get("period") or {"key": "today", "label": "Today", "from": "", "to": ""}
    pkey = period.get("key") or "today"
    pq = f"&period={html.escape(pkey)}"
    if pkey == "custom":
        pq += f"&from={html.escape(str(period.get('from') or ''))}&to={html.escape(str(period.get('to') or ''))}"
    tabs = "".join(
        f'<a class="tab{" on" if k == selected else ""}" href="/?strategy={k}{pq}">{html.escape(labels.get(k, k))}</a>'
        for k in order)
    from .attribution import PERIODS
    pbar = "".join(
        f'<a class="tab p{" on" if k == pkey else ""}" href="/?strategy={selected}&period={k}">{html.escape(lbl)}</a>'
        for k, lbl in PERIODS)
    pbar += (f'<form class="dates" method="get" action="/"><input type="hidden" name="strategy" value="{selected}">'
             f'<input type="hidden" name="period" value="custom">'
             f'<label>From <input type="date" name="from" value="{html.escape(str(period.get("from") or ""))}"></label> '
             f'<label>To <input type="date" name="to" value="{html.escape(str(period.get("to") or ""))}"></label> '
             f'<button type="submit"{" class=on" if pkey == "custom" else ""}>Show</button></form>')
    s = strategies.get(selected) or {}
    cur = html.escape(str(s.get("currency", "")))
    plabel = html.escape(str(period.get("label") or "Today"))

    def money(v):
        if v is None:
            return "-"
        try:
            return f"{float(v):+.2f} {cur}"
        except Exception:
            return html.escape(str(v))

    def plain(v, suffix=""):
        return "-" if v is None else f"{v}{suffix}"

    def colour(v):
        try:
            return "ok" if float(v) >= 0 else "bad"
        except Exception:
            return ""
    wins, losses = s.get("wins"), s.get("losses")
    rows = [("Realised", money(s.get("realised")), colour(s.get("realised"))),
            ("Unrealised", money(s.get("unrealised")), colour(s.get("unrealised"))),
            ("Before commission", money(s.get("before_fees")), ""),
            ("Commission paid (broker)",
             "-" if s.get("total_costs") is None else f"{abs(float(s['total_costs'])):.2f} {cur}", "bad"),
            ("Trades", plain(s.get("trades")), ""),
            ("Wins / losses", f"{plain(wins)} / {plain(losses)}", ""),
            ("Win rate", plain(s.get("win_rate"), "%"), ""),
            ("Average win", money(s.get("avg_win")), ""), ("Average loss", money(s.get("avg_loss")), ""),
            ("Largest win", money(s.get("largest_win")), ""), ("Largest loss", money(s.get("largest_loss")), ""),
            ("Profit factor", plain(s.get("profit_factor")), ""),
            ("Max intraday drawdown", money(s.get("max_drawdown")), ""),
            ("Open positions", plain(s.get("open_positions")), ""),
            ("Average holding time",
             plain(None if s.get("avg_hold_seconds") is None else f"{s['avg_hold_seconds']:.0f}", " s"), ""),
            ("Estimated slippage", plain(s.get("est_slippage_points"), " points"), "")]
    if selected == "rapid_scalper":
        rows += [("Average trade duration",
                  plain(None if s.get("avg_duration_seconds") is None else f"{s['avg_duration_seconds']:.1f}", " s"), ""),
                 ("Exits under 10 s", plain(s.get("exits_under_10s")), ""),
                 ("Exits under 30 s", plain(s.get("exits_under_30s")), ""),
                 ("Reached protected-profit mode", plain(s.get("reached_protected")), ""),
                 ("Reached runner mode", plain(s.get("reached_runner")), ""),
                 ("Max favourable excursion", plain(s.get("max_favourable_r"), " R"), ""),
                 ("Max adverse excursion", plain(s.get("max_adverse_r"), " R"), ""),
                 ("Profit captured vs available", plain(s.get("captured_vs_available_pct"), "%"), "")]
    if selected == "momentum_runner":
        rows += [("Average best point reached", plain(s.get("avg_peak_r"), " R"), ""),
                 ("Reached the 3 R trail", plain(s.get("reached_trail")), "")]
    if selected == "band_breaker":
        rows += [("Average best point reached", plain(s.get("avg_peak_r"), " R"), "")]
    if selected == "overall" and s.get("by_strategy"):
        for b in s["by_strategy"]:
            rows.append((f"of which {html.escape(str(b['label']))}", money(b.get("net_today")),
                         colour(b.get("net_today"))))
    stats = (f'<div class="st big"><div class="k">Net P&amp;L ({plabel}) - after commission</div>'
             f'<div class="v {colour(s.get("net_today"))}">{money(s.get("net_today"))}</div></div>'
             + "".join(f'<div class="st"><div class="k" title="{k}">{k}</div><div class="v {c}">{v}</div></div>'
                       for k, v, c in rows))

    trades = (s.get("trades_today") or [])[-300:][::-1]

    def trow(t):
        return (f"<tr><td class=mono>{html.escape(str(t.get('closed') or '')[11:19])}</td>"
                f"<td>{html.escape(str(t.get('symbol')))}</td>"
                f"<td class=nw>{html.escape(str(labels.get(t.get('strategy'), t.get('strategy'))))}"
                f"{(' <span class=pill>' + html.escape(str(t.get('mode'))) + '</span>') if t.get('mode') else ''}</td>"
                f"<td class=small>{html.escape(str(t.get('tactic') or t.get('exit_reason') or ''))}</td>"
                f"<td class='mono nw {colour(t.get('net'))}'>{money(t.get('net'))}</td></tr>")
    thead = "<tr><th>Closed</th><th>Market</th><th>Bot</th><th>Approach / exit</th><th>Net</th></tr>"
    trade_html = ""
    if trades:
        shown = trades[:RECENT_TRADES_SHOWN]
        rest = trades[RECENT_TRADES_SHOWN:]
        trade_html = (f'<div class="box"><h2>Trades - {plabel} ({html.escape(labels.get(selected, selected))})'
                      f' - {len(trades)}</h2><table>{thead}{"".join(trow(t) for t in shown)}</table>'
                      + (f'<details id="all-trades"><summary>{len(rest)} earlier trades</summary>'
                         f'<table>{"".join(trow(t) for t in rest)}</table></details>' if rest else "")
                      + '</div>')

    detail_html = ""
    if selected == "market_intelligence":
        st0 = snap.get("status") or {}
        rules = html.escape(str(st0.get("strategy_name") or ""))
        since = html.escape(str(st0.get("tracking_start") or "")[:16].replace("T", " "))

        def tbl(title, rows_):
            if not rows_:
                return ""
            body = "".join(f"<tr><td>{html.escape(str(r['name']))}</td><td>{r['trades']}</td><td>{r['wins']}</td>"
                           f"<td class='mono {colour(r['net'])}'>{money(r['net'])}</td></tr>" for r in rows_)
            return (f'<h2>{title} ({plabel})</h2><table><tr><th>{title[:-1] if title.endswith("s") else title}</th>'
                    f'<th>Trades</th><th>Wins</th><th>Net</th></tr>{body}</table>')
        tables = tbl("By approach", s.get("by_approach")) + tbl("By kind of market", s.get("by_market_type"))
        if rules or tables:
            detail_html = ('<div class="box">'
                           + (f'<p class="small" style="margin:0 0 4px">Rules in force: <b>{rules}</b>, since {since} UTC.</p>'
                              if rules else "")
                           + (f'<details id="what-works"><summary>What is working: by approach and by kind of market'
                              f'</summary>{tables}</details>' if tables else "")
                           + '</div>')

    src_txt = html.escape(str(period.get("source") or "the bots' own records"))
    p_start = html.escape(str(period.get("start") or "")[:10])
    p_end = html.escape(str(period.get("end") or "")[:10])
    own = s.get("source")
    if s.get("note"):
        own = (str(own) + ". " if own else "") + str(s["note"])
    source = ((f"Figures on this tab come from {html.escape(str(own))}." if own else "Today's figures are the broker's own.")
              if pkey == "today" else
              f"Figures for {plabel} come from {src_txt}, {p_start} to {p_end} (UTC, end exclusive).")
    figures = (f'<div class="box"><div class="stats">{stats}</div>'
               f'<p class="small" style="margin:6px 0 0">Overall is every strategy added together - the account\'s '
               f'definitive result. Each bot\'s tab shows only its own trades. {source}</p></div>')
    return {"tabs": (f'<div class="tabs">{tabs}<span class="sep"></span>{pbar}</div>'),
            "figures": figures, "detail": detail_html, "trades": trade_html,
            "scalper": render_scalper_panel(snap) + render_runner_panel(snap) + render_bandbreaker_panel(snap)}


def render_strategies(snap: dict, selected: str = "overall") -> str:
    """The strategy tabs and the selected strategy's figures. Display only."""
    p = _strategy_parts(snap, selected)
    if not p:
        return ""
    return p["tabs"] + p["figures"] + p["detail"] + p["trades"] + p["scalper"]


def render_runner_panel(snap: dict) -> str:
    mr = (snap.get("strategies") or {}).get("runner") or {}
    if not mr:
        return ""
    e = html.escape

    def kv(k, v):
        return f'<div class="k">{k}</div><div>{v}</div>'
    body = (kv("Status", e(str(mr.get("status")))) + kv("Mode", f'<span class="pill">{e(str(mr.get("mode")))}</span>')
            + kv("Rule", f'stop left alone until +{mr.get("trail_r", 3)} R, then trailed {mr.get("trail_r", 3)} R behind the best price; '
                         f'out after {mr.get("window_hours", 8):g} h'))
    opens = mr.get("open") or []
    if opens:
        rows = "".join(f'<tr><td><b>{e(str(o.get("symbol")))}</b> {e(str(o.get("side")))}</td><td class=mono>{o.get("r_now"):+.2f} R</td>'
                       f'<td class=mono>peak {o.get("peak_r"):+.2f} R</td><td class=mono>{o.get("stop")}</td>'
                       f'<td>{"trailing" if o.get("trailing") else "original stop"}</td></tr>' for o in opens)
        body += kv("Shadows open", f'<table><tr><th>Trade</th><th>Now</th><th>Best</th><th>Stop</th><th></th></tr>{rows}</table>')
    else:
        body += kv("Shadows open", "none - it starts one whenever Trend &amp; Breakout opens an index trade")
    return (f'<div class="box"><div class="rs-title">MOMENTUM RUNNER</div>'
            f'<div class="small">{e(str(mr.get("tagline", "")))}</div><div class="kv">{body}</div></div>')


def render_bandbreaker_panel(snap: dict) -> str:
    bb = (snap.get("strategies") or {}).get("bandbreaker") or {}
    if not bb:
        return ""
    e = html.escape

    def kv(k, v):
        return f'<div class="k">{k}</div><div>{v}</div>'
    body = (kv("Status", e(str(bb.get("status")))) + kv("Mode", f'<span class="pill">{e(str(bb.get("mode")))}</span>')
            + kv("Session", e(str(bb.get("session") or ""))) + kv("Rule", e(str(bb.get("rule") or ""))))
    now = bb.get("markets_now") or {}
    if now:
        body += kv("Markets", "<br>".join(f"<b>{e(str(k))}</b>: {e(str(v))}" for k, v in now.items()))
    opens = bb.get("open") or []
    if opens:
        rows = "".join(f'<tr><td><b>{e(str(o.get("symbol")))}</b> {e(str(o.get("side")))}</td><td class=mono>{o.get("r_now"):+.2f} R</td>'
                       f'<td class=mono>peak {o.get("peak_r"):+.2f} R</td><td class=mono>stop {o.get("stop")}</td></tr>' for o in opens)
        body += kv("Open", f'<table>{rows}</table>')
    else:
        body += kv("Open", "none")
    return (f'<div class="box"><div class="rs-title">BAND BREAKER</div>'
            f'<div class="small">{e(str(bb.get("tagline", "")))}</div><div class="kv">{body}</div></div>')


def render_scalper_panel(snap: dict) -> str:
    rs = (snap.get("strategies") or {}).get("scalper") or {}
    if not rs:
        return ""
    e = html.escape

    def kv(k, v):
        return f'<div class="k">{k}</div><div>{v}</div>'
    pos = rs.get("position")
    head = (f'<div class="box"><div class="rs-title">RAPID SCALPER</div>'
            f'<div class="small">{e(str(rs.get("tagline", "")))}</div><div class="kv">'
            + kv("Status", e(str(rs.get("status")))) + kv("Mode", f'<span class="pill">{e(str(rs.get("mode")))}</span>'))
    if rs.get("thinking"):
        sess = rs.get("session") or {}
        thr = rs.get("threshold") or {}
        head += kv("Thinking", f'<b>{e(str(rs["thinking"]))}</b>')
        head += kv("Session", f'{e(str(sess.get("name", "")))} {e(str(sess.get("phase", "")).lower().replace("_", "-"))}'
                              f' - moving: {e(", ".join(sess.get("moving") or []) or "none")}; bar {thr.get("now", "")} ({e(str(thr.get("why", "")))})')
    if pos and rs.get("thinking"):
        body = (kv("Position", f'{e(str(pos.get("symbol")))} {e(str(pos.get("side")))} {pos.get("volume")} lots, {e(str(pos.get("setup", "")).replace("_", " ").lower())}')
                + kv("Session", e(str(pos.get("session", ""))))
                + kv("Entry / now / spread", f'{pos.get("entry")} / {pos.get("price")} / {pos.get("spread_points")} pts')
                + kv("Duration", f'{pos.get("duration_seconds")} s')
                + kv("Initial risk", f'{pos.get("initial_risk"):.2f}')
                + kv("P&amp;L now / peak / protected", f'{pos.get("current_pnl"):+.2f} / {pos.get("peak_pnl"):+.2f} / {pos.get("protected_pnl"):+.2f}')
                + kv("Trailing distance", f'{pos.get("trailing_points")} points')
                + kv("Momentum / acceleration", f'{pos.get("momentum")}/100 / {pos.get("acceleration"):+.2f}x')
                + kv("Stage", f'{e(str(pos.get("mode")))} - {e(str(pos.get("state_reason", "")))}')
                + kv("Entry reason", e(str(pos.get("reason")))))
    elif pos:
        floor_txt = "-" if pos.get("protected_floor") is None else f"{pos['protected_floor']:+.2f}"
        body = (kv("Position", f'{e(str(pos.get("symbol")))} {e(str(pos.get("side")))}')
                + kv("Opened", e(str(pos.get("opened"))))
                + kv("Duration", f'{pos.get("duration_seconds")} sec')
                + kv("Entry", pos.get("entry"))
                + kv("Current P&amp;L", f'{pos.get("current_pnl"):+.2f}')
                + kv("Peak P&amp;L", f'{pos.get("peak_pnl"):+.2f}')
                + kv("Planned maximum initial risk", f'{pos.get("planned_max_risk"):.2f}')
                + kv("Current protected floor", floor_txt)
                + kv("Position mode", e(str(pos.get("mode"))))
                + kv("Runner probability", f'{pos.get("runner_probability")}%')
                + kv("Exit tolerance", e(str(pos.get("exit_tolerance"))))
                + kv("Reason", e(str(pos.get("reason")))))
    else:
        conf_txt = "-" if rs.get("confidence") is None else f"{rs['confidence']:.0f}/100"
        body = (kv("Markets monitored", rs.get("markets_monitored", "-"))
                + kv("Best opportunity", f'{e(str(rs.get("best_opportunity") or "-"))} {e(str(rs.get("best_direction") or ""))}')
                + kv("Confidence", conf_txt)
                + kv("Spread quality", e(str(rs.get("spread_quality") or "-")))
                + kv("Momentum", e(str(rs.get("momentum") or "-")))
                + kv("Current position", "NONE"))
    blocked = rs.get("blocked_because") or []
    why = kv("Not trading because", e("; ".join(str(b) for b in blocked))) if blocked else ""
    detail = {"breakdown of the best opportunity": rs.get("best_breakdown"), "its blockers": rs.get("best_blockers"),
              "breakers": rs.get("breakers"), "tick age (s)": rs.get("tick_age_seconds"),
              "latency (ms)": rs.get("latency_ms"), "one full pass (ms)": rs.get("cycle_ms"), "build": rs.get("build")}
    tech = (f'<details id="scalper-tech"><summary>Technical detail</summary>'
            f'<pre>{e(json.dumps(detail, indent=2, default=str))}</pre></details>')
    return head + body + why + "</div>" + tech + "</div>"


def render_status(snap: dict, strategy: str = "overall") -> str:
    st = snap.get("status") or {}
    health = snap.get("health") or {}
    checks = health.get("checks") or []
    problems = [c for c in checks if c["severity"] != "OK"]
    running = st.get("bot") == "RUNNING" and not health.get("safe_mode")
    banner_cls = "ok" if running and not problems else "bad"
    if st.get("bot") == "WAITING FOR METATRADER":
        banner_txt = ("BOT: WAITING FOR METATRADER - "
                      + str(st.get("waiting") or "not connected yet")
                      + (f" ({st['detail']})" if st.get("detail") else "")
                      + f" - attempt {st.get('attempts', 0)}")
    else:
        banner_txt = ("BOT: RUNNING - everything is healthy" if running and not problems
                      else f"BOT: PROBLEM - {health.get('summary', 'see below')}")

    def tile(k, v, cls=""):
        return (f'<div class="tile"><div class="k">{html.escape(k)}</div>'
                f'<div class="v {cls}">{v}</div></div>')

    def yesno(flag, good="CONNECTED", bad="NOT CONNECTED"):
        return (f'<span class="pill ok">{good}</span>' if flag
                else f'<span class="pill bad">{bad}</span>')

    tiles = "".join([
        tile("Equity", _fmt_money(st.get("equity", 0.0),
                                  st.get("currency", ""))),
        tile("Today (broker's day)", _fmt_money(st.get("today_pnl", 0.0),
                                                st.get("currency", "")),
             "ok" if (st.get("today_pnl") or 0) >= 0 else "bad"),
        tile("Win rate today",
             "-" if st.get("win_rate_today") is None
             else f"{st['win_rate_today']:.0f}%"),
        tile("This strategy" + (f" (since {str(st['tracking_start'])[:16].replace('T', ' ')} UTC)"
                                if st.get("tracking_start") else ""),
             ("-" if st.get("since_start_pnl") is None else
              _fmt_money(st.get("since_start_pnl") or 0.0, st.get("currency", ""))
              + (f" over {st.get('since_start_trades')} trades"
                 if st.get("since_start_trades") is not None else "")),
             "ok" if (st.get("since_start_pnl") or 0) >= 0 else "bad"),
        tile("Open trades", st.get("open_positions", 0)),
        tile("Mode", f'<span class="pill {"bad" if st.get("mode")=="LIVE" else "ok"}">'
                     f'{html.escape(str(st.get("mode", "?")))}</span>'),
        tile("MT5", yesno(st.get("mt5_connected"))),
        tile("Broker", yesno(st.get("broker_connected"))),
        tile("Market data", yesno(st.get("data_live"), "LIVE", "STALE")),
        tile("News", yesno(st.get("news_live"), "LIVE", "DEGRADED")),
        tile("Watchdog", yesno(st.get("watchdog_ok"), "HEALTHY", "NOT SEEN")),
        tile("Last scan", html.escape(str(st.get("last_scan", "never")))),
        tile("Last trade", html.escape(str(st.get("last_trade", "none yet")))),
    ])

    prob_html = ""
    if problems:
        rows = "".join(
            f'<tr><td><span class="pill '
            f'{"bad" if c["severity"] == "FAIL" else "warn"}">'
            f'{c["severity"]}</span></td><td>{html.escape(c["message"])}</td></tr>'
            for c in problems)
        prob_html = (f'<div class="card"><h2>What is wrong</h2>'
                     f'<table>{rows}</table></div>')

    think = snap.get("thinking") or []
    if think:
        def trow(t):
            return (f'<tr><td class="mono">{t["rank"]}</td><td><b>{html.escape(t["symbol"])}</b> {t["direction"]}</td>'
                    f'<td class="mono">{t["score"]:.0f} {html.escape(t["tier"])}</td>'
                    f'<td>{html.escape(t["tactic"].replace("_", " ").title())}'
                    f'<div class="small">{html.escape(t["regime"])}'
                    + (f' - waiting: {html.escape("; ".join(t["blockers"]))}'
                       if t.get("blockers") else "")
                    + '</div></td></tr>')
        thead = ('<tr><th>#</th><th>Market</th><th>Score</th><th>Approach and why</th></tr>')
        top, rest = think[:4], think[4:]
        think_html = (
            '<div class="card"><h2>What the bot is looking at right now</h2>'
            '<table>' + thead + "".join(trow(t) for t in top) + '</table>'
            + (f'<details id="thinking-rest"><summary>{len(rest)} more markets</summary><table>'
               + "".join(trow(t) for t in rest) + '</table></details>' if rest else "")
            + '</div>')
    else:
        think_html = ('<div class="card"><h2>What the bot is looking at right now</h2>'
                      '<div class="small">No scan has completed yet.</div></div>')

    build_html = ""
    if st.get("build"):
        build_html = (f'Build <b class="mono">{html.escape(str(st["build"]))}</b>'
                      f' running since {html.escape(str(st.get("started", "")))}. '
                      f'Compare this code with the one in the latest message from the installer. ')
    src = str(st.get("pnl_source") or "")
    src_warn = ""
    if src == "broker":
        src_html = ('Profit figures come from the broker\'s own '
                    'deal records, commission included, for this strategy\'s trades only.')
    elif st.get("pnl_error"):
        src_html = ""
        src_warn = ('<div class="card small bad"><b>The broker\'s trade history could not '
                    'be read</b>, so the profit figures above are from the bot\'s own records '
                    'and may lag: ' + html.escape(str(st.get("pnl_error"))) + '</div>')
    else:
        src_html = 'Profit figures are from the bot\'s own records.'

    cur = st.get("currency", "")
    periods = st.get("periods") or []
    if periods:
        prow = "".join(
            f'<tr><td>{html.escape(str(p["label"]))}</td>'
            f'<td class="mono {"ok" if (p.get("net") or 0) >= 0 else "bad"}">'
            f'{_fmt_money(p.get("net") or 0.0, cur)}</td>'
            f'<td class="mono">{"-" if p.get("before_fees") is None else _fmt_money(p["before_fees"], cur)}</td>'
            f'<td class="mono">{"-" if p.get("commission") is None else _fmt_money(-abs(p["commission"]), cur)}</td>'
            f'<td class="mono">{p.get("trades", 0)}</td>'
            f'<td class="mono">{_pct(p.get("win_rate"))}</td></tr>'
            for p in periods)
        periods_html = ('<div class="card"><h2>Results by period</h2><table>'
                        '<tr><th>Period</th><th>Net (after commission)</th><th>Before commission</th>'
                        '<th>Commission</th><th>Trades</th><th>Win rate</th></tr>' + prow + '</table>'
                        '<div class="small">This strategy\'s trades only, by the time they '
                        'closed, on the broker\'s day - the same way MetaTrader\'s History '
                        'filter counts them.</div></div>')
    else:
        periods_html = ""

    why = [str(r) for r in (st.get("not_trading_because") or []) if r]
    rules = st.get("strategy_rules") or {}
    rules_txt = ""
    if rules:
        rules_txt = (f'<div class="small">Rules in force: {html.escape(str(rules.get("entry_tier", "")))} '
                     f'setups or better, at least {rules.get("min_reward_risk", "?")}x the risk after '
                     f'costs, costs under {int(float(rules.get("max_cost_fraction_of_stop", 0) or 0) * 100)}% '
                     f'of the stop'
                     + (f'; per session: {html.escape(", ".join(rules["sessions"]))}.'
                        if rules.get("sessions") else
                        (f', {rules.get("max_new_positions_per_hour")} an hour and '
                         f'{rules.get("max_new_positions_per_day")} a day.'
                         if (rules.get("max_new_positions_per_hour") or rules.get("max_new_positions_per_day"))
                         else '; no limit on the number of trades.'))
                     + '</div>')
    if why:
        why_html = ('<div class="card"><h2>Why no new trade right now</h2><ul class="small" '
                    'style="margin:0;padding-left:16px">'
                    + "".join(f'<li>{html.escape(w)}</li>' for w in why)
                    + '</ul>' + rules_txt + '</div>')
    else:
        why_html = ('<div class="card"><h2>Why no new trade right now</h2><div class="small">'
                    'Nothing is holding entries back overall - each market below '
                    'shows its own reason for waiting.</div>' + rules_txt + '</div>')

    pos = snap.get("positions") or []
    if pos:
        rows = "".join(
            f'<tr><td><b>{html.escape(p["symbol"])}</b></td><td>{p["side"]}</td>'
            f'<td class="mono">{p["volume"]}</td>'
            f'<td class="mono">{p["entry"]}</td><td class="mono">{p["stop"]}</td>'
            f'<td class="mono {"ok" if p["profit"] >= 0 else "bad"}">'
            f'{_fmt_money(p["profit"], cur)}</td>'
            f'<td class="small">{html.escape(str(p.get("flow_state", "")))} '
            f'{html.escape(str(p.get("note", "")))}</td></tr>' for p in pos)
        pos_html = ('<div class="card"><h2>Open trades</h2><table>'
                    '<tr><th>Market</th><th>Side</th><th>Lots</th><th>Entry</th>'
                    '<th>Stop</th><th>P&amp;L</th><th>State</th></tr>'
                    + rows + '</table></div>')
    else:
        pos_html = ('<div class="card"><h2>Open trades</h2><div class="small">'
                    'No open trades. The bot is watching and waiting for a '
                    'strong enough opportunity.</div></div>')

    parts = _strategy_parts(snap, strategy)
    col1 = parts.get("figures", "") + parts.get("detail", "")
    col2 = periods_html + pos_html + why_html + think_html
    col3 = parts.get("scalper", "") + parts.get("trades", "")
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="10"><title>Trading bot status</title>
<style>{CSS}</style></head><body><div class="wrap">
<div class="top"><nav><a href="/">Status</a><a href="/results">Results</a>
<a href="/health">Health (JSON)</a></nav>
<div class="banner {banner_cls}">{html.escape(banner_txt)}</div></div>
{prob_html}{src_warn}
<div class="row">{tiles}</div>
{parts.get("tabs", "")}
<div class="grid"><div>{col1}</div><div>{col2}</div><div>{col3}</div></div>
<p class="small">{build_html}{src_html} Updated {html.escape(str(snap.get('updated')))} (UTC).
This page refreshes itself every 10 seconds.</p>
</div>{KEEP_OPEN_JS}</body></html>"""


def render_results(results: dict) -> str:
    """Render the results page from a ``build_results`` dictionary."""
    r = results or {}
    ccy = r.get("currency", "")

    def tile(k, v, cls=""):
        return (f'<div class="tile"><div class="k">{html.escape(k)}</div>'
                f'<div class="v {cls}">{v}</div></div>')

    tiles = "".join([
        tile("Trades", r.get("trades", 0)),
        tile("Wins", r.get("wins", 0), "ok"),
        tile("Losses", r.get("losses", 0), "bad"),
        tile("Win rate", "-" if r.get("win_rate") is None
             else f"{r['win_rate']:.0f}%"),
        tile("Pips / points", f"{r.get('pips', 0):+.1f}"),
        tile("Money won", _fmt_money(r.get("money_won", 0), ccy), "ok"),
        tile("Money lost", _fmt_money(r.get("money_lost", 0), ccy), "bad"),
        tile("Net result", _fmt_money(r.get("net", 0), ccy),
             "ok" if (r.get("net") or 0) >= 0 else "bad"),
    ])

    def story_block(title: str, s: Optional[dict]) -> str:
        if not s:
            return ""
        return (f'<div class="card"><b>{html.escape(title)}: '
                f'{html.escape(s["symbol"])} '
                f'({html.escape(s["direction"])}) '
                f'{_fmt_money(s["result_money"], ccy)} '
                f'({s["result_r"]:+.2f}R)</b>'
                f'<p class="small"><b>Why it entered.</b> '
                f'{html.escape(s["why_it_entered"])}</p>'
                f'<p class="small"><b>What happened.</b> '
                f'{html.escape(s["what_happened"])}</p>'
                f'<p class="small"><b>Why it exited.</b> '
                f'{html.escape(s["why_it_exited"])}</p>'
                f'<p class="small"><b>What it learned.</b> '
                f'{html.escape(s["what_it_learned"])}</p></div>')

    stories = "".join(story_block(f"{s['symbol']} {s['direction']}", s)
                      for s in (r.get("trade_stories") or []))
    if not stories:
        stories = ('<div class="card small">No completed trades today yet.'
                   '</div>')

    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading results</title><style>{CSS}</style></head><body>
<div class="wrap">
<nav><a href="/">Status</a><a href="/results">Results</a>
<a href="/health">Health (JSON)</a></nav>
<h1>Today ({html.escape(str(r.get('date', '')))})</h1>
<div class="row">{tiles}</div>
{story_block('Best trade', r.get('best_trade'))}
{story_block('Worst trade', r.get('worst_trade'))}
<h2>Every trade today, explained</h2>{stories}
</div></body></html>"""


class _Handler(BaseHTTPRequestHandler):
    state: DashboardState = None       # set by start_dashboard
    server_version = "mintel"

    def log_message(self, fmt, *args):    # keep the console clean
        log.debug(fmt, *args)

    def _send(self, body: str, content_type: str = "text/html",
              code: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            snap = self.state.snapshot()
            if self.path.startswith("/results"):
                self._send(render_results(snap.get("results") or {}))
            elif self.path.startswith("/health"):
                code = 200 if (snap.get("health") or {}).get(
                    "entries_allowed", False) else 503
                self._send(json.dumps(snap, indent=2, default=str),
                           "application/json", code)
            elif self.path.startswith("/api"):
                self._send(json.dumps(snap, indent=2, default=str),
                           "application/json")
            else:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(self.path).query)
                strategy = (q.get("strategy") or ["overall"])[0]
                period = (q.get("period") or ["today"])[0]
                date_from = (q.get("from") or [""])[0]
                date_to = (q.get("to") or [""])[0]
                if period != "today" and self.state.period_resolver is not None:
                    try:
                        snap = dict(snap)
                        snap["strategies"] = self.state.period_resolver(period, date_from, date_to)
                    except Exception as exc:
                        log.warning("period %s: %s", period, exc)
                self._send(render_status(snap, strategy))
        except Exception as exc:      # never let the page kill the process
            self._send(f"<pre>dashboard error: {html.escape(str(exc))}</pre>",
                       code=500)


def start_dashboard(state: DashboardState, host: str = "127.0.0.1",
                    port: int = 8787) -> ThreadingHTTPServer:
    """Start the dashboard in a daemon thread.

    Bound to localhost by default: the VPS should be reached over Remote
    Desktop or an SSH tunnel rather than exposing an unauthenticated page to
    the internet.
    """
    handler = type("Handler", (_Handler,), {"state": state})
    httpd = ThreadingHTTPServer((host, port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True,
                              name="dashboard")
    thread.start()
    log.info("dashboard listening on http://%s:%d", host, port)
    return httpd
