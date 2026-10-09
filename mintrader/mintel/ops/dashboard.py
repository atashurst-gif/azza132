"""Status, results and "what it is thinking" pages.

Deliberately built on Python's standard-library HTTP server: no web framework,
no extra dependency, nothing else to install or break on a VPS at 3am.

The pages, all written in plain English rather than trader jargon:

* ``/``        - is it running?  Is it healthy?  What is it looking at?
* ``/results`` - what happened today, and why each trade entered and exited.
* ``/rider``   - the Rapid Momentum Rider: today, every trade's story, the
  live scanner, open positions with their FlowLock X state, health checks
  (read-only, from data/rider-status.json and data/rider.sqlite).
* ``/ian``     - Financial Ian: feed state, top opportunity, open positions,
  the reasoning and the order-book ladder (read-only, from
  data/ian-status.json).
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

from ..clock import TZ_LONDON, to_utc, utcnow

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
        self.standing: dict = {}         # the account's figure from the broker (mintel/ops/standing.py)
        self.deals: list = []            # the broker's deal rows since the reset (for periods; never published)
        self.open_live: Optional[list] = None   # every open position on the account, with its bot
        self.open_at: Optional[str] = None
        self.data_dir: str = ""
        self.bot_lines: dict = {}        # {bot id: one plain line} for the Rider and Ian, from their status files
        self.tnb_mode: str = ""          # Trend & Breakout's mode (LIVE or PAPER), from the config; "" = not told
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
                    "strategies": self.strategies, "standing": self.standing,
                    "open_live": self.open_live, "open_at": self.open_at, "bot_lines": self.bot_lines,
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
.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px}
@media (max-width:1100px){.cards{grid-template-columns:repeat(2,1fr)}}
@media (max-width:600px){.cards{grid-template-columns:1fr}}
.hc{background:#fff;border:1px solid #e2e4e8;border-radius:12px;padding:12px 14px}
.hc.acct{grid-column:1/-1;display:flex;flex-wrap:wrap;align-items:flex-end;gap:10px 40px;padding:16px 20px}
.hc .nm{font-size:14px;font-weight:700;display:flex;align-items:center;gap:8px;margin-bottom:6px}
.hc .figs{display:flex;gap:24px;flex-wrap:wrap}
.hc .fk{font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:#6b7280}
.hc .fv{font-size:26px;font-weight:800;font-variant-numeric:tabular-nums;line-height:1.15}
.hc.acct .fv{font-size:46px}
.hc .ft{font-size:12px;color:#4b5563;margin-top:6px}
.hc .pr{font-size:12px;color:#9ca3af;margin-top:4px}
.hc.paper{background:#fafafa}
.hero{background:#fff;border:2px solid #e2e4e8;border-radius:12px;padding:14px 18px;margin-bottom:8px}
.hero.ok{border-color:#b7e3c6}.hero.bad{border-color:#f3c0c0}
.hero .hk{font-size:12px;text-transform:uppercase;letter-spacing:.5px;color:#374151;font-weight:600}
.hero .hv{font-size:52px;font-weight:800;line-height:1.1;margin:4px 0;font-variant-numeric:tabular-nums}
.hero .hs{font-size:14px;color:#1b1c1e;margin-top:2px}
.hero .hsrc{font-size:11px;color:#6b7280;margin-top:6px}
.bots td,.bots th{font-size:13px;padding:5px 6px}
.bots tr.total td{font-weight:700;border-top:2px solid #1b1c1e}
.bots td.grey{color:#9ca3af}
details.more>summary{font-size:13px;font-weight:600;padding:6px 0}
.lad td{padding:1px 5px;font-variant-numeric:tabular-nums}
.lad .bar{height:13px;border-radius:3px;display:inline-block;vertical-align:middle;min-width:1px}
.lad .bar.ask{background:#f3c0c0}.lad .bar.bid{background:#b7e3c6}
.lad .bar.buy{background:#86c99c}.lad .bar.sell{background:#e79a9a}
.lad tr.mid td{background:#f8fafc;font-weight:600;border-top:2px solid #1b1c1e;border-bottom:2px solid #1b1c1e}
.lad tr.ask td.px{color:#b91c1c}.lad tr.bid td.px{color:#0a7c35}
.story{border-top:1px solid #eef0f3;padding:5px 0}
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

# Refreshes the page every 10 seconds, except while a date is being picked
# for the Custom filter (a plain meta refresh would wipe it half-entered).
REFRESH_JS = """<script>
setInterval(function(){
var busy=false;document.querySelectorAll('form.dates input[type=date]').forEach(function(i){
if(document.activeElement===i||i.value!==i.defaultValue)busy=true;});
if(!busy)location.reload();},10000);
</script>"""

RECENT_TRADES_SHOWN = 8

NAV = ('<nav><a href="/">Status</a><a href="/results">Results</a><a href="/rider">Rapid Momentum Rider</a>'
       '<a href="/ian">Financial Ian</a><a href="/health">Health (JSON)</a></nav>')


def _strategy_parts(snap: dict, selected: str = "overall") -> dict:
    """The strategy tabs and the selected strategy's figures, in pieces for the
    one-screen layout. Display only."""
    strategies = snap.get("strategies") or {}
    if not strategies:
        return {}
    labels = strategies.get("labels") or {}
    if selected not in strategies:
        selected = "overall"
    # the retired Rapid Scalper keeps a tab only while it has trades in the period or is still running
    scalper_shown = (selected == "rapid_scalper" or bool((strategies.get("rapid_scalper") or {}).get("trades"))
                     or _is_running(strategies.get("scalper")))
    order = [k for k in ("overall", "market_intelligence", "momentum_rider", "momentum_runner", "band_breaker",
                         "crowd_fader", "financial_ian", "rapid_scalper")
             if k in strategies and (k != "rapid_scalper" or scalper_shown)]
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
            ("of which still open (not yet banked)", money(s.get("unrealised")), colour(s.get("unrealised"))),
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
    if selected in ("band_breaker", "crowd_fader"):
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
    return {"tabs": (f'<div class="tabs">{tabs}<span class="small">&nbsp;showing {plabel} (the filter at the top)</span></div>'),
            "figures": figures, "detail": detail_html, "trades": trade_html,
            "scalper": (render_rider_panel(snap) + render_runner_panel(snap) + render_bandbreaker_panel(snap)
                        + render_crowd_panel(snap) + render_ian_panel(snap)
                        + (render_scalper_panel(snap) if _is_running(strategies.get("scalper")) else ""))}


def _is_running(st) -> bool:
    """A status block that says its bot is running now (present, not stale)."""
    if not isinstance(st, dict) or not st:
        return False
    return st.get("present", True) is not False and not str(st.get("status") or "").startswith("NOT RUNNING")


def render_rider_panel(snap: dict) -> str:
    """A short Rapid Momentum Rider box in the folded section; the full view is /rider."""
    rd = (snap.get("strategies") or {}).get("rider") or {}
    if not rd:
        return ""
    e = html.escape
    today = rd.get("today") or {}
    h = rd.get("health") or {}
    status = rd.get("status") or ("RUNNING" if rd.get("present") else "NOT RUNNING")
    body = (f'<div class="k">Status</div><div>{e(str(status))} - {e(str(h.get("summary") or ""))}</div>'
            f'<div class="k">Mode</div><div><span class="pill">{e(str(rd.get("mode") or ""))}</span></div>'
            f'<div class="k">Today</div><div>{e(str(today.get("trades", 0)))} trades, {e(str(today.get("won", 0)))} won, '
            f'{e(_signed(today.get("net_pips"), " pips"))}</div>'
            f'<div class="k">Watching</div><div>{len(rd.get("scanner") or [])} markets, '
            f'{len(rd.get("open_positions") or [])} open</div>')
    return (f'<div class="box"><div class="rs-title">RAPID MOMENTUM RIDER</div>'
            f'<div class="small">{e(str(rd.get("tagline", "")))}</div><div class="kv">{body}</div>'
            f'<div class="small"><a href="/rider">Everything it is doing, trade by trade</a></div></div>')


def render_ian_panel(snap: dict) -> str:
    """A short Financial Ian box in the folded section; the full view is /ian."""
    ia = (snap.get("strategies") or {}).get("ian") or {}
    if not ia:
        return ""
    e = html.escape
    feed = ia.get("feed") or {}
    body = (f'<div class="k">Status</div><div>{e(str(ia.get("status") or "NOT RUNNING"))}</div>'
            f'<div class="k">Mode</div><div><span class="pill">{e(str(ia.get("mode") or ""))}</span></div>'
            f'<div class="k">Feed</div><div>{e(str(feed.get("state") or "not started yet"))}</div>')
    return (f'<div class="box"><div class="rs-title">FINANCIAL IAN</div>'
            f'<div class="small">{e(str(ia.get("tagline", "")))}</div><div class="kv">{body}</div>'
            f'<div class="small"><a href="/ian">The order book, the reasoning and the trades</a></div></div>')


def _signed(v, suffix: str = "") -> str:
    try:
        return f"{float(v):+.1f}{suffix}"
    except (TypeError, ValueError):
        return "-"


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


def render_crowd_panel(snap: dict) -> str:
    cf = (snap.get("strategies") or {}).get("crowd") or {}
    if not cf:
        return ""
    e = html.escape

    def kv(k, v):
        return f'<div class="k">{k}</div><div>{v}</div>'
    body = (kv("Status", e(str(cf.get("status")))) + kv("Mode", f'<span class="pill">{e(str(cf.get("mode")))}</span>')
            + kv("Rule", e(str(cf.get("rule") or ""))))
    poll = cf.get("last_poll")
    poll_txt = f"last read {e(str(poll)[11:16])} UTC, {cf.get('polls', 0)} reads, {cf.get('api_calls', 0)} API calls" if poll else "no read yet"
    if cf.get("api_remaining") is not None:
        poll_txt += f", {cf['api_remaining']} calls left this minute"
    if cf.get("poll_error"):
        poll_txt += f' - <span class="bad">{e(str(cf["poll_error"]))}</span>'
    body += kv("Coinversa", poll_txt)
    reads = cf.get("reads") or {}
    if reads:
        rows = ""
        for sym, r in sorted(reads.items()):
            d = r.get("direction") or 0
            lean = "short" if d < 0 else ("long" if d > 0 else "-")
            rows += (f'<tr><td><b>{e(str(sym))}</b></td><td class=mono>crowd {r.get("crowd_long_pct", 0):.0f}% long '
                     f'({r.get("crowd_wallets", 0)})</td><td class=mono>winners {r.get("winners_bias", 0):+.2f}</td>'
                     f'<td class=mono>fuel {r.get("fuel_below", 0) / 1e6:.1f}m / {r.get("fuel_above", 0) / 1e6:.1f}m</td>'
                     f'<td class=mono>{r.get("score", 0):.0f} {lean}</td></tr>')
        body += kv("Reads", f'<table>{rows}</table>')
    now = cf.get("markets_now") or {}
    if now:
        body += kv("Markets", "<br>".join(f"<b>{e(str(k))}</b>: {e(str(v))}" for k, v in sorted(now.items())))
    opens = cf.get("open") or []
    if opens:
        rows = "".join(f'<tr><td><b>{e(str(o.get("symbol")))}</b> {e(str(o.get("side")))}</td><td class=mono>{o.get("r_now"):+.2f} R</td>'
                       f'<td class=mono>peak {o.get("peak_r"):+.2f} R</td><td class=mono>stop {o.get("stop")} target {o.get("target")}</td></tr>'
                       for o in opens)
        body += kv("Open", f'<table>{rows}</table>')
    else:
        body += kv("Open", "none")
    return (f'<div class="box"><div class="rs-title">CROWD FADER</div>'
            f'<div class="small">{e(str(cf.get("tagline", "")))}</div><div class="kv">{body}</div></div>')


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


def _uk(ts: str, fmt: str = "%a %d %b %H:%M") -> str:
    try:
        return dt.datetime.fromisoformat(str(ts)).astimezone(TZ_LONDON).strftime(fmt)
    except Exception:
        return ""


def render_period_bar(view: Optional[dict], strategy: str = "overall") -> str:
    """The filter at the top: the same eight choices every time."""
    from .standing import PERIOD_BUTTONS
    key = (view or {}).get("key") or "today"
    q = f"&strategy={html.escape(strategy)}" if strategy and strategy != "overall" else ""
    btns = "".join(f'<a class="tab p{" on" if k == key else ""}" href="/?period={k}{q}">{html.escape(lbl)}</a>'
                   for k, lbl in PERIOD_BUTTONS)
    f_val = html.escape(str((view or {}).get("from") or ""))
    t_val = html.escape(str((view or {}).get("to") or ""))
    custom = (f'<form class="dates" style="margin-left:6px" method="get" action="/"><input type="hidden" name="period" value="custom">'
              + (f'<input type="hidden" name="strategy" value="{html.escape(strategy)}">' if q else "")
              + f'<span class="tab p{" on" if key == "custom" else ""}" style="cursor:default">Custom</span>'
              f'<label>From <input type="date" name="from" value="{f_val}"></label> '
              f'<label>To <input type="date" name="to" value="{t_val}"></label> '
              f'<button type="submit"{" class=on" if key == "custom" else ""}>Show</button></form>')
    return f'<div class="tabs">{btns}{custom}</div>'


def render_open_trades(snap: dict) -> str:
    """Every open position on the account right now, with the bot that holds it."""
    rows = snap.get("open_live")
    e = html.escape
    cur = str((snap.get("standing") or {}).get("currency") or "GBP")
    when = _uk(snap.get("open_at") or "", "%H:%M:%S")
    if rows is None:
        return ('<div class="card"><h2>Open trades right now</h2><div class="small bad">The open positions could not '
                'be read from MetaTrader just now; this box fills again on its own.</div></div>')
    if not rows:
        return (f'<div class="card"><h2>Open trades right now</h2><div class="small">No open trades right now '
                f'(checked {e(when)} UK).</div></div>')
    total = round(sum(float(r.get("profit") or 0.0) for r in rows), 2)

    def px(v):
        return "-" if not v else f"{float(v):g}"
    body = "".join(
        f'<tr><td><b>{e(str(r.get("bot")))}</b></td><td><b>{e(str(r.get("symbol")))}</b></td>'
        f'<td>{e(str(r.get("side")))}</td><td class="mono">{float(r.get("volume") or 0):g}</td>'
        f'<td class="mono nw">{e(_uk(r.get("opened") or "", "%a %H:%M"))}</td>'
        f'<td class="mono">{px(r.get("entry"))}</td><td class="mono">{px(r.get("stop"))}</td>'
        f'<td class="mono">{px(r.get("target"))}</td>'
        f'<td class="mono {"ok" if float(r.get("profit") or 0) > 0 else ("bad" if float(r.get("profit") or 0) < 0 else "")}">'
        f'<b>{_fmt_money(float(r.get("profit") or 0.0), cur)}</b></td></tr>' for r in rows)
    return (f'<div class="card"><h2>Open trades right now - {len(rows)} open, '
            f'<span class="{"ok" if total > 0 else ("bad" if total < 0 else "")}">{_fmt_money(total, cur)}</span> '
            f'not banked yet</h2><table class="bots"><tr><th>Bot</th><th>Market</th><th>Side</th><th>Lots</th>'
            f'<th>Opened (UK)</th><th>Entry</th><th>Stop</th><th>Target</th><th>Profit now</th></tr>{body}</table>'
            f'<div class="small">Live from MetaTrader (profit includes swap), updated {e(when)} UK; the page refreshes '
            f'every 10 seconds.</div></div>')


def render_standing(snap: dict, top: Optional[dict] = None, strategy: str = "overall") -> str:
    """The filter, then clean headline cards: the account, and one card per
    bot, each with the chosen period (Today unless another is picked) and
    Overall. Every figure is MetaTrader's own record, after commission.
    Then the open trades. Display only."""
    sd = snap.get("standing") or {}
    top = top or {}
    sel, tot = top.get("sel"), top.get("tot")
    p_sel, p_tot = top.get("p_sel") or {}, top.get("p_tot") or {}
    e = html.escape
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")

    def m(v):
        return "-" if v is None else _fmt_money(float(v), cur)

    def cls(v):
        if v is None or abs(float(v)) < 0.005:
            return ""
        return "ok" if float(v) > 0 else "bad"
    try:
        began = dt.datetime.fromisoformat(str(sd.get("start"))).astimezone(TZ_LONDON).strftime("%a %d %b")
    except Exception:
        began = ""
    bar = render_period_bar(sel or {"key": "today"}, strategy)
    if not sel or not tot or tot.get("made") is None:
        why = e(str(sd.get("error") or "the page has not heard from MetaTrader yet"))
        return (bar + f'<div class="cards"><div class="hc acct bad"><div><div class="nm">Account</div>'
                f'<div class="fv">-</div><div class="ft">MetaTrader\'s records could not be read just now ({why}). '
                f'No figure is shown rather than a wrong one; it comes back on its own.</div></div></div></div>'
                + render_open_trades(snap))
    show_sel = sel.get("key") != "total"
    sel_label = str(sel.get("label") or "Today")
    open_live = snap.get("open_live") or []
    opened = round(sum(float(r.get("profit") or 0.0) for r in open_live), 2) if open_live else 0.0

    def fig(label, v, big=False):
        return f'<div><div class="fk">{e(label)}</div><div class="fv {cls(v)}">{m(v)}</div></div>'
    # the account
    acct_figs = (fig(sel_label, sel.get("made")) if show_sel else "") + fig("Overall", tot.get("made"))
    bal = sd.get("balance")
    start_bal = sd.get("start_balance")
    foot = []
    if bal is not None and start_bal is not None:
        foot.append(f"Balance {m(bal).lstrip('+')} - started with {m(start_bal).lstrip('+')}" + (f" on {e(began)}" if began else ""))
    if open_live:
        foot.append(f'open trades now <b class="{cls(opened)}">{m(opened)}</b> (not banked yet)')
    acct = (f'<div class="hc acct"><div><div class="nm">Account - all bots, after commission</div>'
            f'<div class="figs">{acct_figs}</div><div class="ft">{" &middot; ".join(foot)}</div></div></div>')
    # one card per bot
    open_by: dict = {}
    for r in open_live:
        open_by[r.get("bot")] = open_by.get(r.get("bot"), 0.0) + float(r.get("profit") or 0.0)
    cards = ""
    retired_notes = []
    for b in sd.get("bots") or []:
        bid, label, mode = b.get("id"), str(b.get("label")), str(b.get("mode") or "")
        live = mode == "LIVE"
        vs = (sel.get("bots") or {}).get(bid) or {}
        vt = (tot.get("bots") or {}).get(bid) or {}
        if b.get("retired"):
            # a retired bot: no card, one line, and only while it has money to show
            vals = [(sel_label, vs.get("made")) if show_sel else None, ("overall", vt.get("made")),
                    ("open now", b.get("open"))]
            shown = [(k, v) for k, v in (x for x in vals if x) if v is not None and abs(float(v)) >= 0.005]
            if shown:
                retired_notes.append(f'{e(label)}: ' + ", ".join(f'{e(k.lower())} <b class="{cls(v)}">{m(v)}</b>'
                                                                  for k, v in shown))
            continue
        figs = (fig(sel_label, vs.get("made")) if show_sel else "") + fig("Overall", vt.get("made"))
        bits = []
        n, when = (vs.get("trades"), sel_label.lower()) if show_sel else (vt.get("trades"), "overall")
        bits.append("trades not known just now" if n is None else f'{n} trade{"" if n == 1 else "s"} {e(when)}')
        if open_by.get(label):
            bits.append(f'open now <b class="{cls(open_by[label])}">{m(round(open_by[label], 2))}</b>')
        line = (snap.get("bot_lines") or {}).get(bid)
        if line:
            bits.append(e(str(line)))
        if bid == "market_intelligence" and mode == "PAPER":
            bits.append("real prices, simulated orders")
        if bid == "momentum_runner":
            bits.append("rides Trend &amp; Breakout's index entries")
        practice = ""
        if not live:
            ps, pt = p_sel.get(bid), p_tot.get(bid)
            practice = (f'<div class="pr">Practice only, not real money: '
                        + (f'{e(sel_label.lower())} {m(ps)} &middot; ' if show_sel and ps is not None else "")
                        + f'overall {m(pt)}</div>')
        pill = f'<span class="pill {"ok" if live else ""}">{e(mode)}</span>'
        cards += (f'<div class="hc{"" if live else " paper"}"><div class="nm">{e(label)} {pill}</div>'
                  f'<div class="figs">{figs}</div><div class="ft">{" &middot; ".join(bits)}</div>{practice}</div>')
    notes = []
    for v, txt in ((tot.get("other"), "from trades placed by hand"),
                   (tot.get("adjustments"), "from other changes to the balance (broker charges or corrections)")):
        if v is not None and abs(float(v)) >= 0.01:
            notes.append(f"{m(v)} {txt}")
    note = (f'<div class="small">Overall also includes {" and ".join(notes)}, so the cards add up to the account.</div>'
            if notes else "")
    if retired_notes:
        note += (f'<div class="small">{" &middot; ".join(retired_notes)} - its real trades from before it was retired '
                 f'(9 Oct), kept here so the cards add up to the account.</div>')
    err = str(sd.get("error") or "")
    if err or not tot.get("complete", True):
        note += (f'<div class="small bad">Part of MetaTrader\'s records could not be read just now'
                 f'{" (" + e(err) + ")" if err else ""}; a card showing "-" fills in again on its own.</div>')
    note += ('<div class="small">Every figure is MetaTrader\'s own record of the trades, after commission. '
             'A bot in PAPER places no real orders; its practice result is shown in grey and never counted.</div>')
    return bar + f'<div class="cards">{acct}{cards}</div>' + note + render_open_trades(snap)


def render_status(snap: dict, strategy: str = "overall", headline: Optional[dict] = None) -> str:
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
    if str(st.get("tnb_mode") or "").upper() == "PAPER":
        src_html = ('Trend &amp; Breakout is on PAPER: Today, Win rate today, This strategy and Open trades are '
                    'its practice on real prices with simulated orders (and any real trade left from LIVE), never '
                    'added to the account; Equity is the real account.')
    elif src == "broker":
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
    more = (f'<details class="more" id="more"><summary>Each bot in detail, open trades, what the bots are looking '
            f'at, connections</summary><div class="row">{tiles}</div>{src_warn}'
            f'<p class="small">{src_html} (These folded figures are each bot\'s own; the figures at the top are '
            f'the broker\'s.)</p>'
            f'{parts.get("tabs", "")}<div class="grid"><div>{col1}</div><div>{col2}</div><div>{col3}</div></div>'
            f'</details>')
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<noscript><meta http-equiv="refresh" content="10"></noscript><title>Trading bot status</title>
<style>{CSS}</style></head><body><div class="wrap">
<div class="top">{NAV}
<div class="banner {banner_cls}">{html.escape(banner_txt)}</div></div>
{prob_html}
{render_standing(snap, headline, strategy)}
{more}
<p class="small">{build_html}Updated {html.escape(str(snap.get('updated')))} (UTC).
This page refreshes itself every 10 seconds.</p>
</div>{KEEP_OPEN_JS}{REFRESH_JS}</body></html>"""


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
{NAV}
<h1>Today ({html.escape(str(r.get('date', '')))})</h1>
<div class="row">{tiles}</div>
{story_block('Best trade', r.get('best_trade'))}
{story_block('Worst trade', r.get('worst_trade'))}
<h2>Every trade today, explained</h2>{stories}
</div></body></html>"""


# ------------------------------------------------- the two bots of 9 Oct --
BOT_STALE_SECONDS = 120.0


def _read_status_file(path) -> tuple[Optional[dict], str]:
    """(the status file's object, why not) - never raises."""
    from pathlib import Path
    try:
        p = Path(path)
        if not p.exists():
            return None, "missing"
        raw = json.loads(p.read_text())
        if not isinstance(raw, dict):
            return None, "it does not hold a JSON object"
        return raw, ""
    except Exception as exc:
        return None, f"it could not be read ({exc})"


def _age_of(stamp, now: dt.datetime) -> Optional[float]:
    try:
        t = dt.datetime.fromisoformat(str(stamp))
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return (to_utc(now) - t).total_seconds()
    except Exception:
        return None


def _num(v, fmt: str = "{:g}", none: str = "-") -> str:
    try:
        return fmt.format(float(v))
    except (TypeError, ValueError):
        return none


def _money_txt(v, ccy: str = "GBP") -> str:
    try:
        return _fmt_money(float(v), ccy)
    except (TypeError, ValueError):
        return "-"


def _cls_of(v) -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    return "ok" if f > 0 else ("bad" if f < 0 else "")


def _page(title: str, body: str, refresh: bool = True) -> str:
    meta = '<meta http-equiv="refresh" content="10">' if refresh else ""
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">{meta}'
            f'<title>{html.escape(title)}</title><style>{CSS}</style></head><body><div class="wrap">'
            f'<div class="top">{NAV}</div>{body}'
            f'<p class="small">This page only reads the bot\'s own files; it changes nothing. '
            f'It refreshes itself every 10 seconds.</p></div></body></html>')


def _own_file(data, name: str) -> dict:
    raw, _why = _read_status_file(data / name)
    return raw or {}


def _rider_today_from_rows(rows: list) -> dict:
    """TODAY from the Rider's own rows, when no status file says it."""
    rows = [r for r in rows if not str(r.get("exit_reason") or "").startswith("SWITCHED")]
    vals = [r.get("net_money") if r.get("net_money") is not None else r.get("realised_pips") for r in rows]
    won = sum(1 for v in vals if (v or 0) > 0)
    best = max(rows, key=lambda r: float(r.get("net_money") if r.get("net_money") is not None
                                         else r.get("realised_pips") or 0.0), default=None)
    return {"trades": len(rows), "won": won, "lost": len(rows) - won,
            "win_rate": round(100.0 * won / len(rows), 1) if rows else None,
            "net_pips": round(sum(float(r.get("realised_pips") or 0.0) for r in rows), 1),
            "net_money": round(sum(float(r.get("net_money") or 0.0) for r in rows), 2) if rows else 0.0,
            "net_money_estimated": any(r.get("money_source") == "estimate" for r in rows),
            "money_source": "the bot's own records (all modes)",
            "best_trade": (f"{best.get('symbol')} {float(best.get('realised_pips') or 0.0):+.1f} pips"
                           if best is not None else None)}


def render_rider_page(data_dir: str, now: Optional[dt.datetime] = None) -> str:
    """The Rapid Momentum Rider's own page: TODAY, every trade's story, the
    live scanner, open positions with their FlowLock X state, and the
    health checks. Read-only, from data/rider-status.json and
    data/rider.sqlite. Renders (saying "not started yet") when they are
    missing."""
    from pathlib import Path
    e = html.escape
    now = to_utc(now or utcnow())
    title = "RAPID MOMENTUM RIDER"
    if not data_dir:
        return _page("Rapid Momentum Rider", f'<h1>{title}</h1><div class="card">Not started yet: the status page has '
                                             f'not been told where the bots keep their files.</div>')
    data = Path(data_dir)
    from .watchdog import own_file_mode
    settings = _own_file(data, "rider.json")
    mode_set = own_file_mode(data / "rider.json")
    status_name = settings.get("status_file") if isinstance(settings.get("status_file"), str) else "rider-status.json"
    db_name = settings.get("journal_file") if isinstance(settings.get("journal_file"), str) else "rider.sqlite"
    st, why = _read_status_file(data / (status_name or "rider-status.json"))
    day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    from .attribution import _rows_ro
    rows = _rows_ro(data / (db_name or "rider.sqlite"),
                    "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? ORDER BY closed_utc DESC "
                    "LIMIT 200", (day0.isoformat(),))
    parts = [f'<h1>{title} <span class="pill {"bad" if mode_set == "LIVE" else ""}">{e(mode_set)}</span></h1>']
    if st is None:
        what = ("Not started yet" if why == "missing" else f"Its status file {e(why)}")
        if mode_set == "OFF":
            what = "OFF: data/rider.json says OFF, so it is not started"
        parts.append(f'<div class="card"><b>{what}.</b> <span class="small">Nothing is shown rather than a guess. '
                     f'It is set to {e(mode_set)} in data/rider.json; the watchdog starts it when that is not OFF, '
                     f'and this page fills in once it reports.</span></div>')
        st = {}
    else:
        age = _age_of(st.get("updated_utc") or st.get("updated"), now)
        h = st.get("health") or {}
        if age is None or age > BOT_STALE_SECONDS:
            state = (f'<span class="pill bad">NOT RUNNING</span> last report '
                     f'{"never" if age is None else f"{age / 60:.0f} min ago"}')
        elif h.get("entries_allowed"):
            state = '<span class="pill ok">HEALTHY</span>'
        else:
            state = '<span class="pill warn">RUNNING, NEW ENTRIES PAUSED</span>'
        run_mode = str(st.get("mode") or mode_set)
        restart = (f' <span class="small bad">(running {e(run_mode)}; data/rider.json says {e(mode_set)}: '
                   f'it changes on the next restart)</span>' if run_mode != mode_set else "")
        parts.append(f'<div class="card">{state} {e(str(h.get("summary") or ""))}{restart}'
                     f'<div class="small">{e(str(st.get("tagline") or ""))} Exposure: '
                     f'GBP {_num(st.get("user_pip_value_gbp"), "{:.2f}")} per pip (Aaron\'s setting). '
                     f'Order flow: {e(str(st.get("order_flow") or "-"))}.</div></div>')
    # ---- TODAY
    today = st.get("today") if isinstance(st.get("today"), dict) else None
    source = "the Rider's status file"
    if today is None:
        today = _rider_today_from_rows(rows)
        source = "the Rider's own records since 00:00 UTC"
    money_src = str(today.get("money_source") or "")
    est = " (some estimated from the last price)" if today.get("net_money_estimated") else ""

    def tile(k, v, c=""):
        return f'<div class="tile"><div class="k">{e(k)}</div><div class="v {c}">{v}</div></div>'
    wr = today.get("win_rate")
    tiles = "".join([
        tile("Trades", e(str(today.get("trades", 0)))), tile("Won", e(str(today.get("won", 0))), "ok"),
        tile("Lost", e(str(today.get("lost", 0))), "bad"),
        tile("Win rate", "-" if wr is None else f"{_num(wr, '{:.0f}')}%"),
        tile("Net pips", _num(today.get("net_pips"), "{:+.1f}"), _cls_of(today.get("net_pips"))),
        tile("Net money", _money_txt(today.get("net_money")), _cls_of(today.get("net_money"))),
        tile("Best trade", e(str(today.get("best_trade") or "-")))])
    parts.append(f'<div class="card"><h2>Today</h2><div class="row">{tiles}</div><div class="small">From {e(source)}; '
                 f'money: {e(money_src or "-")}{e(est)}. The headline cards on the status page are MetaTrader\'s own '
                 f'figures.</div></div>')
    # ---- open positions with FlowLock X
    opens = st.get("open_positions") or []
    if opens:
        body = "".join(
            f'<tr><td><b>{e(str(o.get("market")))}</b></td><td>{e(str(o.get("side")))}</td>'
            f'<td class="mono">{_num(o.get("volume"))}</td><td class="mono">{_num(o.get("entry"))}</td>'
            f'<td class="mono">{_num(o.get("stop"))}</td>'
            f'<td class="mono {_cls_of(o.get("pips"))}">{_num(o.get("pips"), "{:+.1f}")}</td>'
            f'<td class="mono {_cls_of(o.get("money"))}">{_money_txt(o.get("money"))}</td>'
            f'<td><b>{e(str(o.get("flowlock_state") or ""))}</b> - {e(str(o.get("flowlock") or ""))}'
            f'<div class="small">momentum decay {_num(o.get("decay"), "{:.2f}")}</div></td></tr>' for o in opens)
        parts.append(f'<div class="card"><h2>Open positions - {len(opens)}</h2><table><tr><th>Market</th><th>Side</th>'
                     f'<th>Lots</th><th>Entry</th><th>Stop (only ever tightens)</th><th>Pips</th><th>Money (approx.)</th>'
                     f'<th>FlowLock X</th></tr>{body}</table><div class="small">Money here is pips x GBP per pip, an '
                     f'approximation; the broker\'s figure is on the status page.</div></div>')
    else:
        parts.append('<div class="card"><h2>Open positions</h2><div class="small">None open.</div></div>')
    # ---- the live scanner
    scan = [r for r in (st.get("scanner") or []) if isinstance(r, dict)]
    if scan:
        scan.sort(key=lambda r: -float(r.get("score") or 0.0))
        body = "".join(
            f'<tr><td><b>{e(str(r.get("market")))}</b></td><td>{e(str(r.get("direction")))}</td>'
            f'<td class="mono">{_num(r.get("score"), "{:.0f}")}</td><td>{e(str(r.get("state")))}</td>'
            f'<td class="small">{e(str(r.get("reason") or ""))}</td></tr>' for r in scan)
        parts.append(f'<div class="card"><h2>The scanner right now - {len(scan)} markets</h2><table><tr><th>Market</th>'
                     f'<th>Direction</th><th>Score</th><th>State</th><th>Reason</th></tr>{body}</table></div>')
    else:
        parts.append('<div class="card"><h2>The scanner right now</h2><div class="small">No scan reported yet.</div></div>')
    # ---- every trade's story
    if rows:
        stories = "".join(
            f'<div class="story"><b>{e(str(r.get("symbol")))}</b> {e(str(r.get("side") or ""))} '
            f'<span class="pill">{e(str(r.get("mode") or "PAPER"))}</span> '
            f'<span class="mono">{e(str(r.get("closed_utc") or "")[11:19])} UTC</span> '
            f'<span class="mono {_cls_of(r.get("realised_pips"))}">{_num(r.get("realised_pips"), "{:+.1f}")} pips</span> '
            f'<span class="mono {_cls_of(r.get("net_money"))}">{_money_txt(r.get("net_money"))}</span>'
            f'{" (estimate)" if r.get("money_source") == "estimate" else ""}'
            f'<div class="small">{e(str(r.get("explanation") or r.get("exit_detail") or ""))}</div></div>' for r in rows)
        parts.append(f'<div class="card"><h2>Every trade today, explained - {len(rows)}</h2>{stories}</div>')
    elif st.get("stories"):
        stories = "".join(f'<div class="story small">{e(str(x))}</div>' for x in st["stories"])
        parts.append(f'<div class="card"><h2>Recent trades, explained</h2>{stories}</div>')
    else:
        parts.append('<div class="card"><h2>Every trade today, explained</h2><div class="small">No trades today yet.'
                     '</div></div>')
    # ---- health checks
    checks = (st.get("health") or {}).get("checks") or []
    if checks:
        body = "".join(
            f'<tr><td><span class="pill {"ok" if c.get("ok") else ("bad" if c.get("critical") else "warn")}">'
            f'{"OK" if c.get("ok") else ("FAIL" if c.get("critical") else "WARN")}</span></td>'
            f'<td>{e(str(c.get("name") or ""))}</td><td>{e(str(c.get("message") or ""))}</td></tr>'
            for c in checks if isinstance(c, dict))
        parts.append(f'<div class="card"><h2>Health checks</h2><table>{body}</table></div>')
    notes = st.get("notes") or []
    if notes:
        parts.append('<div class="card"><h2>What it has been doing</h2><ul class="small" style="margin:0;padding-left:16px">'
                     + "".join(f'<li>{e(str(n))}</li>' for n in notes[::-1]) + '</ul></div>')
    parts.append('<p class="small">LIVE money is the broker\'s figure where its deal could be read ("estimate" marks one '
                 'booked from the last price); PAPER is practice, not money, and never in the account total.</p>')
    return _page("Rapid Momentum Rider", "".join(parts))


def _ladder_html(lad: dict) -> str:
    """The order book as a ladder: asks above, bids below, resting sizes as
    bars, the volume bought and sold at each price, and the pressure."""
    e = html.escape
    rows = [r for r in (lad.get("rows") or []) if isinstance(r, dict)]
    if not rows:
        return '<div class="small">No book yet.</div>'
    rows.sort(key=lambda r: -float(r.get("price") or 0.0))
    biggest = max([float(r.get("ask") or 0) for r in rows] + [float(r.get("bid") or 0) for r in rows] + [1e-9])
    traded = max([float(r.get("bought") or 0) for r in rows] + [float(r.get("sold") or 0) for r in rows] + [1e-9])
    best_ask = lad.get("ask")
    best_bid = lad.get("bid")

    def bar(v, cls, scale):
        try:
            f = float(v or 0.0)
        except (TypeError, ValueError):
            f = 0.0
        if f <= 0:
            return ""
        return f'<span class="bar {cls}" style="width:{max(2, int(90 * f / scale))}px"></span> {f:g}'
    pressure = str(lad.get("pressure") or "balanced")
    arrow = "&#9650;" if pressure.startswith("buyers") else ("&#9660;" if pressure.startswith("sellers") else "&#9670;")
    mid_row = (f'<tr class="mid"><td colspan="6">spread {_num(best_bid)} / {_num(best_ask)} - book imbalance '
               f'{_num(lad.get("imbalance"), "{:+.2f}")} - pressure {arrow} {e(pressure)}</td></tr>')
    out, placed = [], False
    for r in rows:
        try:
            price = float(r.get("price"))
        except (TypeError, ValueError):
            continue
        side = "ask" if (r.get("ask") or 0) and not (r.get("bid") or 0) else ("bid" if (r.get("bid") or 0) else "")
        if not placed and best_ask is not None and price < float(best_ask):
            out.append(mid_row)
            placed = True
        flags = ", ".join(str(f) for f in (r.get("flags") or []))
        out.append(f'<tr class="{side}"><td class="px mono">{price:g}</td>'
                   f'<td>{bar(r.get("ask"), "ask", biggest)}</td><td>{bar(r.get("bid"), "bid", biggest)}</td>'
                   f'<td>{bar(r.get("bought"), "buy", traded)}</td><td>{bar(r.get("sold"), "sell", traded)}</td>'
                   f'<td class="small">{e(flags)}</td></tr>')
    if not placed:
        out.append(mid_row)
    return (f'<div class="small">{e(str(lad.get("instrument") or ""))} - {e(str(lad.get("data_class") or ""))} data '
            f'(the CME futures book, not MetaTrader)</div>'
            f'<table class="lad"><tr><th>Price</th><th>Offered (asks)</th><th>Bid for (bids)</th><th>Bought here</th>'
            f'<th>Sold here</th><th>Notes</th></tr>{"".join(out)}</table>')


def render_ian_page(data_dir: str, now: Optional[dt.datetime] = None) -> str:
    """Financial Ian's own page, read-only from data/ian-status.json:
    running, the feed (NOT CONFIGURED / LIVE / DEGRADED / DOWN), MetaTrader,
    the top opportunity and its plain-English reasoning, open positions,
    today, the last trade and the order-book ladder. Renders (saying "not
    started yet") when the file is missing."""
    from pathlib import Path
    e = html.escape
    now = to_utc(now or utcnow())
    title = "FINANCIAL IAN"
    if not data_dir:
        return _page("Financial Ian", f'<h1>{title}</h1><div class="card">Not started yet: the status page has not been '
                                      f'told where the bots keep their files.</div>')
    data = Path(data_dir)
    from .watchdog import own_file_mode
    mode_set = own_file_mode(data / "ian.json")
    st, why = _read_status_file(data / "ian-status.json")
    parts = [f'<h1>{title} <span class="pill {"bad" if mode_set == "LIVE" else ""}">{e(mode_set)}</span></h1>']
    if st is None:
        what = "Not started yet" if why == "missing" else f"Its status file {e(why)}"
        if mode_set == "OFF":
            what = "OFF: data/ian.json says OFF, so it is not started"
        parts.append(f'<div class="card"><b>{what}.</b> <span class="small">It is set to {e(mode_set)} in data/ian.json. '
                     f'With no institutional data feed it runs DATA-DEGRADED: no signals, no trades, and says so here '
                     f'once it reports.</span></div>')
        return _page("Financial Ian", "".join(parts))
    age = _age_of(st.get("updated") or st.get("updated_utc"), now)
    running = age is not None and age <= BOT_STALE_SECONDS
    feed = st.get("feed") or {}
    fstate = str(feed.get("state") or "UNKNOWN")
    fcls = {"LIVE": "ok", "DEGRADED": "warn", "NOT CONFIGURED": "warn", "CONNECTING": "warn"}.get(fstate, "bad")
    mt5 = (st.get("mt5") or {}).get("connected")
    today = st.get("today") or {}
    wr = today.get("win_rate")
    label = str(st.get("data_label") or "LIVE")

    def tile(k, v, c=""):
        return f'<div class="tile"><div class="k">{e(k)}</div><div class="v {c}">{v}</div></div>'
    last = st.get("last_trade") or {}
    last_txt = (f'{e(str(last.get("spot_symbol")))} {e(str(last.get("side")))} {_money_txt(last.get("net_pnl"))}'
                if last else "none yet")
    tiles = "".join([
        tile("Running", '<span class="pill ok">YES</span>' if running else
             f'<span class="pill bad">NO</span> {"" if age is None else f"{age / 60:.0f} min ago"}'),
        tile("Feed", f'<span class="pill {fcls}">{e(fstate)}</span>'),
        tile("MetaTrader", '<span class="pill ok">CONNECTED</span>' if mt5 else '<span class="pill bad">NOT CONNECTED</span>'),
        tile("Today's profit", _money_txt(today.get("net")), _cls_of(today.get("net"))),
        tile("Trades today", e(str(today.get("trades", 0)))),
        tile("Win rate", "-" if wr is None else f"{_num(float(wr) * 100.0, '{:.0f}')}%"),
        tile("Open positions", e(str(len(st.get("open_positions") or [])))),
        tile("Last trade", last_txt)])
    synthetic = label != "LIVE"
    parts.append(f'<div class="card"><b>{e(str(st.get("status") or ""))}</b>'
                 + (f' <span class="pill bad">{e(label)}</span>' if synthetic else "")
                 + f'<div class="small">Feed: {e(str(feed.get("vendor") or "none"))} - {e(str(feed.get("reason") or ""))}'
                 f'</div><div class="row" style="margin-top:6px">{tiles}</div>'
                 f'<div class="small">Today\'s figures are Financial Ian\'s own records ({e(str(today.get("data_source") or "-"))}'
                 f', {e(str(st.get("mode") or mode_set))}); PAPER is practice, not money.</div></div>')
    top = st.get("top_opportunity") or {}
    if top:
        reg = top.get("regime") or {}
        parts.append(f'<div class="card"><h2>Top opportunity</h2><b>{e(str(top.get("spot_symbol")))} '
                     f'{e(str(top.get("spot_side") or "no side"))}</b> from {e(str(top.get("instrument") or top.get("root")))} - '
                     f'score {_num(top.get("score"), "{:.0f}")}, {e(str(reg.get("regime") if isinstance(reg, dict) else reg))}, '
                     f'{"tradeable" if top.get("tradeable") else "not tradeable: " + e(str(top.get("why_not") or ""))}'
                     f'<p>{e(str(st.get("reasoning") or top.get("reasoning") or ""))}</p></div>')
    else:
        parts.append('<div class="card"><h2>Top opportunity</h2><div class="small">None yet: it needs the institutional '
                     'order book to form a view.</div></div>')
    opens = st.get("open_positions") or []
    if opens:
        body = "".join(
            f'<tr><td><b>{e(str(o.get("symbol")))}</b> ({e(str(o.get("future") or ""))})</td><td>{e(str(o.get("side")))}</td>'
            f'<td class="mono">{_num(o.get("volume"))}</td><td class="mono">{_num(o.get("entry"))}</td>'
            f'<td class="mono">{_num(o.get("stop"))}</td><td class="mono">{_num(o.get("r_now"), "{:+.2f}")} R '
            f'(best {_num(o.get("best_r"), "{:+.2f}")})</td>'
            f'<td class="mono {_cls_of(o.get("pnl"))}">{_money_txt(o.get("pnl"))}</td>'
            f'<td class="small">{e(str(o.get("trail") or ""))}<br>{e(str(o.get("reasoning") or ""))}</td></tr>'
            for o in opens if isinstance(o, dict))
        parts.append(f'<div class="card"><h2>Open positions - {len(opens)}</h2><table><tr><th>Market</th><th>Side</th>'
                     f'<th>Lots</th><th>Entry</th><th>Stop (only ever tightens)</th><th>Now</th><th>Profit</th>'
                     f'<th>Why / the stop</th></tr>{body}</table></div>')
    else:
        parts.append('<div class="card"><h2>Open positions</h2><div class="small">None open.</div></div>')
    if last:
        parts.append(f'<div class="card"><h2>Last trade</h2><b>{e(str(last.get("spot_symbol")))} {e(str(last.get("side")))}'
                     f'</b> {_num(last.get("entry"))} to {_num(last.get("exit_price"))}, '
                     f'<span class="{_cls_of(last.get("net_pnl"))}">{_money_txt(last.get("net_pnl"))}</span> '
                     f'({_num(last.get("realised_r"), "{:+.2f}")} R, {e(str(last.get("mode") or ""))}, '
                     f'{e(str(last.get("data_source") or ""))})<div class="small">{e(str(last.get("exit_explanation") or ""))}'
                     f'</div></div>')
    lad = st.get("ladder")
    parts.append('<div class="card"><h2>The order book</h2>'
                 + (_ladder_html(lad) if isinstance(lad, dict) else
                    '<div class="small">No order book: no institutional feed is delivering data.</div>') + '</div>')
    opps = st.get("opportunities") or {}
    if opps:
        body = "".join(
            f'<tr><td><b>{e(str(k))}</b></td><td>{e(str(o.get("spot")))}</td><td>{e(str(o.get("spot_side") or "-"))}</td>'
            f'<td class="mono">{_num(o.get("score"), "{:.0f}")}</td><td>{e(str(o.get("regime") or ""))}</td>'
            f'<td class="small">{"tradeable" if o.get("tradeable") else e(str(o.get("why_not") or ""))}</td></tr>'
            for k, o in sorted(opps.items()) if isinstance(o, dict))
        parts.append(f'<div class="card"><h2>Every future it watches</h2><table><tr><th>Future</th><th>Spot</th>'
                     f'<th>Side</th><th>Score</th><th>Regime</th><th></th></tr>{body}</table></div>')
    parts.append('<p class="small">INSTITUTIONAL data is the CME futures order book from the configured vendor; RETAIL data '
                 'is MetaTrader\'s spot quotes, used only for the chart, confirmation and execution, never as the book. '
                 + ('<b>This run is not the live feed: nothing here is a result.</b>' if synthetic else '') + '</p>')
    return _page("Financial Ian", "".join(parts))


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
            elif self.path.startswith("/rider"):
                with self.state.lock:
                    data_dir = self.state.data_dir
                self._send(render_rider_page(data_dir))
            elif self.path.startswith("/ian"):
                with self.state.lock:
                    data_dir = self.state.data_dir
                self._send(render_ian_page(data_dir))
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
                headline = None
                with self.state.lock:                    # the figures and their deal rows from the same refresh
                    sd = dict(self.state.standing or {})
                    deals = list(self.state.deals or ())
                    data_dir = self.state.data_dir
                snap = dict(snap)
                snap["standing"] = sd
                if sd.get("start") and sd.get("made") is not None:
                    try:
                        from .standing import period_view, practice_figures
                        sel = period_view(sd, deals, period, date_from, date_to)
                        sel["from"], sel["to"] = date_from, date_to
                        tot = period_view(sd, deals, "total")
                        headline = {"sel": sel, "tot": tot, "p_sel": {}, "p_tot": {}}
                        if data_dir:
                            ccy = str(sd.get("currency") or "GBP")
                            for k, v in (("p_sel", sel), ("p_tot", tot)):
                                headline[k] = practice_figures(data_dir, dt.datetime.fromisoformat(v["start"]), utcnow(),
                                                          ccy, end=dt.datetime.fromisoformat(v["end"]))
                    except Exception as exc:
                        log.warning("period %s: %s", period, exc)
                        headline = None
                # the folded tabs follow the same filter (from the bots' own records)
                tab_period, tf, tt = period, date_from, date_to
                if period == "total" and sd.get("start"):
                    tab_period, tf, tt = "custom", str(sd["start"])[:10], utcnow().date().isoformat()
                if tab_period != "today" and self.state.period_resolver is not None:
                    try:
                        snap = dict(snap)
                        resolver = self.state.period_resolver
                        tnb = getattr(self.state, "tnb_mode", "") or ""
                        if tnb and getattr(resolver, "takes_tnb_mode", False):
                            snap["strategies"] = resolver(tab_period, tf, tt, tnb_mode=tnb)
                        else:
                            snap["strategies"] = resolver(tab_period, tf, tt)
                        snap["strategies"]["period"]["key"] = period
                    except Exception as exc:
                        log.warning("period %s: %s", period, exc)
                self._send(render_status(snap, strategy, headline))
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
