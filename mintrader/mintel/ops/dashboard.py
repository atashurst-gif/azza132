"""Status, results and "what it is thinking" pages.

Deliberately built on Python's standard-library HTTP server: no web framework,
no extra dependency, nothing else to install or break on a VPS at 3am.

The pages, all written in plain English rather than trader jargon:

* ``/``        - the simple page (Aaron, 9 Oct 16:45 UK: "something really
  easy for me to read"): the period buttons, the account (balance, the
  chosen period, Overall), one row per bot with its verdict from its own
  record and what to do (mintel/ops/verdicts.py), then the plan
  (docs/plan.json) - see ``render_home``. ``/?bot=<id>`` is one bot: what
  happened, the recommendation and the evidence, above its existing
  detail (``render_bot_detail``). Since 12 Oct (the lab's showcase,
  mintel/ops/showcase.py) "/" also shows today's standouts and what changed
  since yesterday, and puts the bots and the lab's candidates on one ladder:
  Real money, Practising, Binned, each row with what to do and five checks.
* ``/lab``     - the strategy lab in full, and ``/lab?id=<id>`` one of its
  ideas (``render_lab_board``, ``render_candidate``): read-only, from
  data/lab/summary.json and data/lab-status.json alone.
* ``/details`` - the full page, as the main page was until 9 Oct: is it
  running?  Is it healthy?  What is it looking at?  Every figure, the
  folded tabs and the bot buttons (``render_status``; its links stay on
  /details).
* ``/results`` - what happened today, and why each trade entered and exited.
* ``/rider``   - the Rapid Momentum Rider: today, every trade's story, the
  live scanner, open positions with their FlowLock X state, health checks
  (read-only, from data/rider-status.json and data/rider.sqlite).
* ``/ian``     - Financial Ian: feed state, top opportunity, open positions,
  the reasoning and the order-book ladder (read-only, from
  data/ian-status.json).
* ``/health``  - machine-readable health, for the watchdog and for curl.

Every period on these pages is a UK calendar day (Europe/London, midnight
to midnight): a trade counts, in full, on the UK day it closed (see
mintel/ops/standing.py). Under the period buttons a row of bot buttons
(``?bot=<id>``) shows one bot alone: its card, its trades closed in the
period, its open trades and its own live view, reusing the renderers of
the folded "Each bot in detail" section.

Every bot card ends with one plain "Now: ..." line - what that bot is doing
right now and its last trade, from its own status file and records - and
the All bots view opens with a short "No live trade since HH:MM UK - here
is why:" banner when no LIVE bot has traded for 30 minutes in market hours
(``bot_now_lines``, ``render_quiet_banner``). Both live on /details and on
each bot's own view; the simple page "/" carries neither.
"""
from __future__ import annotations

import datetime as dt
import html
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Optional

from ..clock import TZ_LONDON, fx_market_open, to_utc, utcnow
from .standing import UK_DAYS_NOTE, uk_date, uk_day_start
from . import coach_page          # 12 Oct: the coach's line on "/", /coach and coach_progress in /api

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
        self.clock: Optional[Callable[[], dt.datetime]] = None   # "now" for the pages (tests set it); UTC now otherwise
        self.updated: Optional[dt.datetime] = None

    def now(self) -> dt.datetime:
        try:
            return to_utc(self.clock()) if self.clock is not None else utcnow()
        except Exception:
            return utcnow()

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
    """Today's results in plain language, plus a per-trade story. Today is
    the UK day (from 00:00 UK), like every period on the pages."""
    now = to_utc(now or utcnow())
    start = uk_day_start(now)
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
        "date": uk_date(now).isoformat(),
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
.botbar{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:-2px 0 8px}
.botbar .tab{padding:4px 10px;font-size:12px}
.botbar .days{font-size:11px;color:#6b7280;margin-left:6px}
.why{font-size:11px;color:#6b7280;margin-top:4px}
.cards.one{grid-template-columns:minmax(0,640px)}
.scroll{overflow-x:auto}
.trades td,.trades th{font-size:12px;padding:3px 6px}
.trades tr.practice td{color:#9ca3af}
.trades td.money{font-weight:700;color:#1b1c1e;white-space:nowrap}
.trades tr.practice td.money{color:#9ca3af;font-weight:500}
.hc .now{font-size:12px;color:#1b1c1e;margin-top:6px;border-top:1px solid #eef0f3;padding-top:5px}
.quiet{background:#fef4e6;border:1px solid #f5d9a8;border-radius:8px;padding:8px 12px;margin-bottom:8px;
color:#78350f;font-size:12px}
.quiet .qh{font-weight:700;font-size:13px;margin-bottom:3px}
.quiet div{margin-top:2px}
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
# location.reload() reloads this very address, query and all, so the chosen
# bot (?bot=), period and dates stay chosen.
REFRESH_JS = """<script>
setInterval(function(){
var busy=false;document.querySelectorAll('form.dates input[type=date]').forEach(function(i){
if(document.activeElement===i||i.value!==i.defaultValue)busy=true;});
if(!busy)location.reload();},10000);
</script>"""

RECENT_TRADES_SHOWN = 8

NAV = ('<nav><a href="/">Status</a><a href="/results">Results</a><a href="/rider">Rapid Momentum Rider</a>'
       '<a href="/ian">Financial Ian</a><a href="/health">Health (JSON)</a></nav>')


def _strategy_parts(snap: dict, selected: str = "overall", base: str = "/") -> dict:
    """The strategy tabs and the selected strategy's figures, in pieces for the
    one-screen layout. Display only. ``base``: the page the links go back to
    ("/details" for the full page)."""
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
    if _bot_choice(snap.get("bot_selected")) != "all":                 # the chosen bot stays chosen
        pq += f"&bot={html.escape(str(snap['bot_selected']))}"
    tabs = "".join(
        f'<a class="tab{" on" if k == selected else ""}" href="{base}?strategy={k}{pq}">{html.escape(labels.get(k, k))}</a>'
        for k in order)
    from .attribution import PERIODS
    pbar = "".join(
        f'<a class="tab p{" on" if k == pkey else ""}" href="{base}?strategy={selected}&period={k}">{html.escape(lbl)}</a>'
        for k, lbl in PERIODS)
    pbar += (f'<form class="dates" method="get" action="{base}"><input type="hidden" name="strategy" value="{selected}">'
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
    p_start = html.escape(_uk(period.get("start") or "", "%d %b %Y"))
    try:                                                   # the last UK day in the period (its end is the next midnight)
        p_end = html.escape(_uk((dt.datetime.fromisoformat(str(period.get("end"))) - dt.timedelta(seconds=1)).isoformat(),
                                "%d %b %Y"))
    except Exception:
        p_end = ""
    own = s.get("source")
    if s.get("note"):
        own = (str(own) + ". " if own else "") + str(s["note"])
    source = ((f"Figures on this tab come from {html.escape(str(own))}." if own else "Today's figures are the broker's own.")
              if pkey == "today" else
              f"Figures for {plabel} come from {src_txt}, {p_start} to {p_end} (UK days, both included).")
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
    broker_day = (snap.get("bot_today") or {}).get("momentum_rider")
    if isinstance(broker_day, dict) and broker_day.get("trades") is not None:
        # MetaTrader's figures for the UK day: the same as its card
        today_txt = (f'{e(str(broker_day.get("trades")))} trades, {e(str(broker_day.get("wins")))} won, '
                     f'{e(_money_txt(broker_day.get("made")))} (MetaTrader, since 00:00 UK)')
    else:
        today_txt = (f'{e(str(today.get("trades", 0)))} trades, {e(str(today.get("won", 0)))} won, '
                     f'{e(_signed(today.get("net_pips"), " pips"))}')
    body = (f'<div class="k">Status</div><div>{e(str(status))} - {e(str(h.get("summary") or ""))}</div>'
            f'<div class="k">Mode</div><div><span class="pill">{e(str(rd.get("mode") or ""))}</span></div>'
            f'<div class="k">Today</div><div>{today_txt}</div>'
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


def render_strategies(snap: dict, selected: str = "overall", base: str = "/") -> str:
    """The strategy tabs and the selected strategy's figures. Display only."""
    p = _strategy_parts(snap, selected, base)
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


BOT_IDS = ("market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
           "financial_ian", "rapid_scalper")


def _bot_choice(bot) -> str:
    """The bot a ``?bot=`` names, or "all" (anything else is "all")."""
    return bot if bot in BOT_IDS else "all"


def _keep(view: Optional[dict], bot: str = "all", strategy: str = "overall", period: bool = True) -> str:
    """The query that keeps the chosen period (and its dates), bot and folded tab."""
    v = view or {}
    key = str(v.get("key") or "today")
    q = []
    if period:
        q.append(f"period={html.escape(key)}")
        if key == "custom":
            q.append(f"from={html.escape(str(v.get('from') or ''))}")
            q.append(f"to={html.escape(str(v.get('to') or ''))}")
    if bot and bot != "all":
        q.append(f"bot={html.escape(bot)}")
    if strategy and strategy != "overall":
        q.append(f"strategy={html.escape(strategy)}")
    return "&".join(q)


def render_period_bar(view: Optional[dict], strategy: str = "overall", bot: str = "all", base: str = "/") -> str:
    """The filter at the top: the same eight choices every time. The chosen
    bot (``?bot=``) and folded tab stay chosen. ``base``: the page the
    buttons stay on ("/details" for the full page)."""
    from .standing import PERIOD_BUTTONS
    bot = _bot_choice(bot)
    key = (view or {}).get("key") or "today"
    rest = _keep(None, bot, strategy, period=False)
    q = f"&{rest}" if rest else ""
    btns = "".join(f'<a class="tab p{" on" if k == key else ""}" href="{base}?period={k}{q}">{html.escape(lbl)}</a>'
                   for k, lbl in PERIOD_BUTTONS)
    f_val = html.escape(str((view or {}).get("from") or ""))
    t_val = html.escape(str((view or {}).get("to") or ""))
    hidden = ((f'<input type="hidden" name="strategy" value="{html.escape(strategy)}">'
               if strategy and strategy != "overall" else "")
              + (f'<input type="hidden" name="bot" value="{html.escape(bot)}">' if bot != "all" else ""))
    custom = (f'<form class="dates" style="margin-left:6px" method="get" action="{base}"><input type="hidden" name="period" value="custom">'
              + hidden
              + f'<span class="tab p{" on" if key == "custom" else ""}" style="cursor:default">Custom</span>'
              f'<label>From <input type="date" name="from" value="{f_val}"></label> '
              f'<label>To <input type="date" name="to" value="{t_val}"></label> '
              f'<button type="submit"{" class=on" if key == "custom" else ""}>Show</button></form>')
    return f'<div class="tabs">{btns}{custom}</div>'


def render_bot_bar(view: Optional[dict], bot: str = "all", strategy: str = "overall",
                   retired_money: bool = False, base: str = "/") -> str:
    """The bot buttons, right under the period buttons and always there: All
    bots (the account and every card) or one bot alone, keeping the chosen
    period. The retired Rapid Scalper has a button only while it has real
    money in the period. Says once that days are UK days."""
    from .standing import BOTS
    bot = _bot_choice(bot)
    labels = dict(BOTS)
    order = ["all", "market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
             "financial_ian"] + (["rapid_scalper"] if retired_money or bot == "rapid_scalper" else [])
    btns = []
    for k in order:
        q = _keep(view, k, strategy)
        # "&#38;" is the same "&" as "&amp;": the button reads "Trend & Breakout", and the first
        # "Trend &amp; Breakout" on the page is still its card's name

        name = "All bots" if k == "all" else html.escape(labels.get(k, k)).replace("&amp;", "&#38;")
        btns.append(f'<a class="tab{" on" if k == bot else ""}" href="{base}?{q}">{name}</a>')
    return (f'<div class="botbar">{"".join(btns)}<span class="days">{html.escape(UK_DAYS_NOTE)}.</span></div>')


def render_open_trades(snap: dict, magic: Optional[int] = None, title: str = "Open trades right now") -> str:
    """Every open position on the account right now, with the bot that holds
    it (or one bot's, ``magic``). Open trades count in no period until they
    close; the commission already charged on them is said under the table."""
    rows = snap.get("open_live")
    e = html.escape
    cur = str((snap.get("standing") or {}).get("currency") or "GBP")
    when = _uk(snap.get("open_at") or "", "%H:%M:%S")
    title = e(title)
    if rows is None:
        return (f'<div class="card"><h2>{title}</h2><div class="small bad">The open positions could not '
                'be read from MetaTrader just now; this box fills again on its own.</div></div>')
    if magic is not None:
        rows = [r for r in rows if int(r.get("magic") or 0) == int(magic)]
    if not rows:
        return (f'<div class="card"><h2>{title}</h2><div class="small">No open trades right now '
                f'(checked {e(when)} UK).</div></div>')
    total = round(sum(float(r.get("profit") or 0.0) for r in rows), 2)
    charged = snap.get("open_charged") or {}
    booked = round(sum(float(charged.get(int(r.get("ticket") or 0), 0.0)) for r in rows), 2)

    def px(v):
        return "-" if not v else f"{float(v):.10g}"
    body = "".join(
        f'<tr><td><b>{e(str(r.get("bot")))}</b></td><td><b>{e(str(r.get("symbol")))}</b></td>'
        f'<td>{e(str(r.get("side")))}</td><td class="mono">{float(r.get("volume") or 0):g}</td>'
        f'<td class="mono nw">{e(_uk(r.get("opened") or "", "%a %H:%M"))}</td>'
        f'<td class="mono">{px(r.get("entry"))}</td><td class="mono">{px(r.get("stop"))}</td>'
        f'<td class="mono">{px(r.get("target"))}</td>'
        f'<td class="mono {"ok" if float(r.get("profit") or 0) > 0 else ("bad" if float(r.get("profit") or 0) < 0 else "")}">'
        f'<b>{_fmt_money(float(r.get("profit") or 0.0), cur)}</b></td></tr>' for r in rows)
    fee_note = ""
    if abs(booked) >= 0.005:
        what = (f"{_fmt_money(abs(booked), cur).lstrip('+')} has already been taken from" if booked < 0
                else f"{_fmt_money(booked, cur)} has already been added to")
        fee_note = (f'<div class="small">{what} the balance for these open trades (the commission charged when they '
                    f'opened, and any part already closed); it counts in their result on the UK day they close.</div>')
    return (f'<div class="card"><h2>{title} - {len(rows)} open, '
            f'<span class="{"ok" if total > 0 else ("bad" if total < 0 else "")}">{_fmt_money(total, cur)}</span> '
            f'not banked yet</h2><table class="bots"><tr><th>Bot</th><th>Market</th><th>Side</th><th>Lots</th>'
            f'<th>Opened (UK)</th><th>Entry</th><th>Stop</th><th>Target</th><th>Profit now</th></tr>{body}</table>'
            f'<div class="small">Live from MetaTrader (profit includes swap), updated {e(when)} UK; the page refreshes '
            f'every 10 seconds. Open trades count in no period until they close.</div>{fee_note}</div>')


DEFAULT_MAGICS = {"market_intelligence": 990_311, "momentum_rider": 990_811, "momentum_runner": 990_511,
                  "band_breaker": 990_611, "crowd_fader": 990_711, "financial_ian": 990_911, "rapid_scalper": 990_411}


def _cls(v) -> str:
    if v is None or abs(float(v)) < 0.005:
        return ""
    return "ok" if float(v) > 0 else "bad"


def _open_of(open_live, magic, label) -> tuple[float, int]:
    """(open profit, open trades) of one bot, from the account's open positions."""
    money, n = 0.0, 0
    for r in open_live or ():
        mine = (int(r.get("magic") or 0) == int(magic or 0)) if "magic" in r else (r.get("bot") == label)
        if mine:
            money += float(r.get("profit") or 0.0)
            n += 1
    return round(money, 2), n


def _bot_card(snap: dict, b: dict, sel: dict, tot: dict, p_sel: dict, p_tot: dict, cur: str,
              alone: bool = False, now_line: str = "") -> str:
    """One bot's card: its mode, the chosen period and Overall (MetaTrader's
    figures, after commission), its trades, what it has open, and - in grey,
    never added - its practice. ``alone``: the card at the top of its own view.
    ``now_line``: what it is doing now (``bot_now_lines``), shown as "Now: ..."."""
    e = html.escape
    bid, label, mode = b.get("id"), str(b.get("label")), str(b.get("mode") or "")
    # 9 Oct evening: Trend & Breakout on PAPER with some kinds of market LIVE (Formula 1: its minor pairs) leads
    # with its real money, as a LIVE card does; its practice follows, labelled. Never plain "PAPER".
    groups = ((snap.get("tnb_live") or {}).get("groups") or ()) if bid == "market_intelligence" else ()
    tactics = ((snap.get("tnb_live") or {}).get("tactics") or ()) if groups else ()    # 10 Oct: tnb.live_tactics
    mixed = bool(groups) and mode != "LIVE"
    live = mode == "LIVE" or mixed
    show_sel = sel.get("key") != "total"
    sel_label = str(sel.get("label") or "Today")
    vs = (sel.get("bots") or {}).get(bid) or {}
    vt = (tot.get("bots") or {}).get(bid) or {}

    def m(v):
        return "-" if v is None else _fmt_money(float(v), cur)

    def fig(lbl, v):
        return f'<div><div class="fk">{e(lbl)}</div><div class="fv {_cls(v)}">{m(v)}</div></div>'
    ps, pt = p_sel.get(bid), p_tot.get(bid)
    bits = []
    if live:
        figs = (fig(sel_label, vs.get("made")) if show_sel else "") + fig("Overall", vt.get("made"))
        v = vs if show_sel else vt
        n, when = v.get("trades"), (sel_label.lower() if show_sel else "overall")
        if n is None:
            bits.append("trades not known just now")
        else:
            won = f' ({v.get("wins")} won, {v.get("losses")} lost)' if n and v.get("wins") is not None else ""
            bits.append(f'{n} trade{"" if n == 1 else "s"} {e(when)}{won}')
    else:
        # Aaron, 9 Oct: "overall showing the exact same constantly" - a PAPER bot's real money is frozen
        # at what it made while LIVE, so its card leads with what it is doing now: its practice
        # (simulated orders on real prices), labelled as such and never added to the account.
        figs = ((fig(f"{sel_label} (practice)", ps) if show_sel else "")
                + fig("Overall (practice)", pt))
        v = vs if show_sel else vt
        n, when = v.get("trades"), (sel_label.lower() if show_sel else "overall")
        if n:                                           # real trades from its LIVE days in this period
            won = f' ({v.get("wins")} won, {v.get("losses")} lost)' if v.get("wins") is not None else ""
            bits.append(f'{n} trade{"" if n == 1 else "s"} {e(when)}{won} (real, from its LIVE days)')
    money, n_open = _open_of(snap.get("open_live"), b.get("magic"), label)
    if snap.get("open_live") is None and alone:
        bits.append("open trades not readable just now")
    elif n_open:
        bits.append(f'open now <b class="{_cls(money)}">{m(money)}</b>'
                    + (f' ({n_open} open)' if alone else ""))
    elif alone:
        bits.append("nothing open now")
    line = (snap.get("bot_lines") or {}).get(bid)
    if line:
        bits.append(e(str(line)))
    if mixed:
        from .verdicts import live_kinds
        bits.append(f"{e(live_kinds(groups, tactics))} LIVE (real money); the rest real prices, simulated orders")
    elif bid == "market_intelligence" and mode == "PAPER":
        bits.append("real prices, simulated orders")
    if bid == "momentum_runner":
        bits.append("rides Trend &amp; Breakout's index entries")
    practice = ""
    if not live:
        real_sel = vs.get("made") if show_sel else None
        practice = ('<div class="pr">Practice only, not real money. Real money from its LIVE days (in the account): '
                    + (f'{e(sel_label.lower())} {m(real_sel)} &middot; '
                       if real_sel is not None and abs(float(real_sel)) >= 0.005 else "")
                    + f'overall {m(vt.get("made"))}</div>')
    elif mixed and pt is not None:
        practice = ('<div class="pr">The rest is practice (PAPER), not real money: '
                    + (f'{e(sel_label.lower())} {m(ps)} &middot; ' if show_sel and ps is not None else "")
                    + f'overall {m(pt)}</div>')
    elif show_sel and ps is not None and abs(float(ps)) >= 0.005:
        practice = (f'<div class="pr">Practice only, not real money: '
                    + f'{e(sel_label.lower())} {m(ps)} &middot; overall {m(pt)}</div>')
    if mixed:
        from .verdicts import live_words
        pill = f'<span class="pill ok">{e(live_words(groups, tactics))}</span>'
    else:
        pill = f'<span class="pill {"ok" if live else ""}">{e(mode)}</span>'
    now_html = f'<div class="now"><b>Now:</b> {e(str(now_line))}</div>' if now_line else ""
    return (f'<div class="hc{"" if live else " paper"}"><div class="nm">{e(label)} {pill}</div>'
            f'<div class="figs">{figs}</div><div class="ft">{" &middot; ".join(bits)}</div>{practice}{now_html}</div>')


# --------------------------------------------- "Now:" - what each bot is doing --
# Aaron, 9 Oct 12:40 UK: "looks like we've stopped trading, don't even know
# what's going on". Every card says in one plain line what its bot is doing
# right now and its last trade, from the bot's own status file and records
# (read-only); the top of the page says why when no LIVE bot has traded for
# a while in market hours. Display only: nothing here takes part in a decision.
QUIET_MINUTES = 30                     # no LIVE trade this long in market hours: the page says why
NOW_STATUS_FILES = {"momentum_runner": "runner-status.json", "band_breaker": "bandbreaker-status.json",
                    "crowd_fader": "crowd-status.json", "financial_ian": "ian-status.json"}
_FRACTIONS = ((0.5, "half"), (1 / 3, "a third"), (0.25, "a quarter"), (0.2, "a fifth"), (0.1, "a tenth"))
# The Rapid Momentum Rider's scanner reasons (mintel/rider/score.py reason_for), grouped as the line says them
_RIDER_GROUPS = (("costs would eat the move", "moving but costs would eat the move"),
                 ("but too choppy", "moving but too choppy"),
                 ("Choppy and going nowhere", "choppy and going nowhere"),
                 ("Quiet", "quiet"),
                 ("Spread too wide", "held back by a wide spread"),
                 ("Hardly any price updates", "too thin (hardly any price updates)"),
                 ("Warming up", "still warming up"),
                 ("No clear direction", "without a clear direction"))
_RIDER_COST_GROUP = _RIDER_GROUPS[0][1]
# A scanner row the Rider's 9 Oct guards hold back (loss brake, loss cooldown or pause, a currency resting after a
# loss, the currency cluster): its build sets "held_back" and puts "Held back: <words>. " in front of the reason
# (mintel/rider/core.py scan). Such a row is never "moving with nothing in the way".
_RIDER_HELD_LEAD = "Held back: "
_RIDER_HELD_GROUP = "held back by its loss or currency guards"


def _rider_held(r: dict) -> tuple[str, str]:
    """(what holds this scanner row back in the guard's own words, or '';
    the row's reason without the "Held back: ..." its build puts in front)."""
    reason = str(r.get("reason") or "")
    held = r.get("held_back")
    held = held.strip() if isinstance(held, str) else ""
    if held:
        lead = f"{_RIDER_HELD_LEAD}{held}. "
        return held, (reason[len(lead):] if reason.startswith(lead) else reason)
    if reason.startswith(_RIDER_HELD_LEAD):                # the words in the reason alone
        held, _sep, rest = reason[len(_RIDER_HELD_LEAD):].partition(". ")
        return held.strip().rstrip("."), rest
    return "", reason


def _uk_at(t, now) -> str:
    """'11:50 UK' on today's UK day, else 'Thu 08 Oct 23:36 UK'; '' when it is not a time."""
    w = _when(t)
    if w is None:
        return ""
    fmt = "%H:%M UK" if uk_date(w) == uk_date(to_utc(now)) else "%a %d %b %H:%M UK"
    return w.astimezone(TZ_LONDON).strftime(fmt)


def _fraction(x) -> str:
    """0.25 -> 'a quarter'; any other share as a percentage."""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return ""
    for v, word in _FRACTIONS:
        if abs(f - v) < 1e-6:
            return word
    return f"{f:.0%}"


def _plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def _not_reporting(st: Optional[dict], why: str, now: dt.datetime) -> str:
    """'' while the status file is fresh; else, plainly, that the bot is not reporting."""
    if st is None:                                        # _read_status_file says "it could not be read (...)"
        return ("not reporting - no status file from it yet" if why == "missing"
                else f"not reporting - its status file {why[3:] if why.startswith('it ') else why}")
    stamp = st.get("updated_utc") or st.get("updated")
    age = _age_of(stamp, now)
    if age is None or _uk_at(stamp, now) == "":
        return "not reporting - its status file carries no time"
    if age > BOT_STALE_SECONDS:
        return f"not reporting since {_uk_at(stamp, now)}"
    return ""


def _last_practice(data: Path, bot: str) -> Optional[dict]:
    """The bot's last PAPER trade from its own records - Trend & Breakout's
    from its paper record - as {closed, symbol, net}; None when it has none."""
    from .attribution import _rows_ro, is_test_data, row_mode
    if bot == "market_intelligence":
        from .standing import TnbPaperRecord
        record = TnbPaperRecord(data)
        done = [] if record.error else record.positions()
        if not done:
            return None
        p = done[-1]
        return {"closed": _when(p.get("closed")), "symbol": p.get("symbol"), "net": p.get("net")}
    path = _own_db(data, bot)
    if path is None or not path.exists():
        return None
    money_col = OWN_RECORDS[bot][1]
    for r in _rows_ro(path, "SELECT * FROM trades WHERE closed_utc IS NOT NULL ORDER BY closed_utc DESC LIMIT 200", ()):
        if row_mode(r) == "LIVE":
            continue                                    # real money is MetaTrader's (the caller's ``closed``)
        if bot == "momentum_rider" and str(r.get("exit_reason") or "").startswith("SWITCHED"):
            continue                                    # closed in its records at a switch: never a trade
        if bot == "financial_ian" and is_test_data(r):
            continue                                    # a synthetic or replayed feed: a test, not a result
        return {"closed": _when(r.get("closed_utc")), "net": r.get(money_col),
                "symbol": str(r.get("symbol") or r.get("spot_symbol") or r.get("instrument") or "")}
    return None


def _trade_txt(kind: str, t: dict, now: dt.datetime, cur: str) -> str:
    when = _uk_at(t.get("closed"), now) or "at a time not on record"
    money = "-" if t.get("net") is None else _money_txt(t.get("net"), cur)
    return f"last {kind}trade {when} {t.get('symbol') or '-'} {money}"


def _last_trade_txt(data: Optional[Path], bot: str, magic: int, mode: str, closed: Optional[list],
                    now: dt.datetime, cur: str, began: str) -> str:
    """Its last trade: time (UK), market and result. Real trades are
    MetaTrader's closed positions for its magic (``closed``; None when they
    could not be read), each in full after commission; practice is its own
    PAPER record. A LIVE bot leads with its real trades, a PAPER bot with
    its practice (a real one left from LIVE when that is the newer)."""
    real = None
    if closed is not None:
        mine = [p for p in closed if p.get("bot_magic") == magic and _when(p.get("closed")) is not None]
        if mine:
            p = max(mine, key=lambda x: _when(x["closed"]))
            real = {"closed": _when(p["closed"]), "symbol": p.get("symbol"), "net": p.get("net")}
    practice = None
    if data is not None:
        try:
            practice = _last_practice(data, bot)
        except Exception as exc:                         # never a reason to fail the page
            log.debug("last practice trade of %s: %s", bot, exc)
    if mode == "LIVE":
        if real is not None:
            return _trade_txt("", real, now, cur)
        out = ("last real trade not known yet (MetaTrader's records not read)" if closed is None
               else (f"no real trade since {began}" if began else "no real trade on record"))
        return out + (f"; {_trade_txt('practice ', practice, now, cur)}" if practice else "")
    epoch = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if real is not None and (practice is None or real["closed"] >= (practice.get("closed") or epoch)):
        return _trade_txt("real ", real, now, cur)
    if practice is not None:
        return _trade_txt("practice ", practice, now, cur)
    return "no practice trade on record"


def _rider_now(data: Path, now: dt.datetime) -> str:
    """The Rider: entries paused or not, what it is riding, what the scanner
    sees (how many markets, the biggest group of reasons, the best score and
    what it still needs - a market its loss or currency guards hold back
    says so, never "nothing in the way") and, from its why_no_trade block
    when its build writes one, the last hour; older builds: the scanner
    rows alone."""
    _mode_set, status_name, _db = _rider_files(data)
    st, why = _read_status_file(data / status_name)
    gone = _not_reporting(st, why, now)
    if gone:
        return gone
    try:
        from ..rider.config import RiderConfig
        rcfg = RiderConfig.load(data / "rider.json")    # read-only: what the Rider reads at its start
    except Exception:
        rcfg = None
    w = st.get("why_no_trade")
    w = w if isinstance(w, dict) and not w.get("unavailable") else None
    trigger = (w or {}).get("trigger_score")
    if not isinstance(trigger, (int, float)) or isinstance(trigger, bool):
        trigger = getattr(rcfg, "trigger_score", None)
    bits = []
    h = st.get("health") if isinstance(st.get("health"), dict) else {}
    if h.get("entries_allowed") is False:
        bits.append(str(h.get("summary") or "new entries paused"))
    opens = [o for o in (st.get("open_positions") or []) if isinstance(o, dict)]
    if opens:
        bits.append("riding " + ", ".join(f"{o.get('market')} {str(o.get('side') or '').lower()} "
                                          f"{_num(o.get('pips'), '{:+.1f}')} pips" for o in opens[:4]))
    scan = [r for r in (st.get("scanner") or []) if isinstance(r, dict)]
    names = [str(r.get("market") or "") for r in scan] or [str(k) for k in ((w or {}).get("markets") or {})]
    word = "pairs" if names and all(len(s) == 6 and s.isalpha() for s in names) else "markets"
    if names:
        groups: dict = {}
        for r in scan:
            guard, reason = _rider_held(r)
            ha = r.get("headroom_atr")
            if str(r.get("state") or "") == "ENTERED":
                g = "in a trade"
            elif guard:
                g = _RIDER_HELD_GROUP
            else:
                g = next((said for words, said in _RIDER_GROUPS if words in reason), None)
            if g is None and ", but " in reason:          # score.py: news is judged before costs, room after
                if rcfg is not None and isinstance(ha, (int, float)) and not isinstance(ha, bool):
                    g = ("moving but too close to the next level" if ha < rcfg.min_headroom_atr
                         else "moving but held back by news")
                else:
                    g = "moving but held back"
            elif g is None and "momentum" in reason:
                g = "moving with nothing in the way yet"
            if g is not None:
                groups[g] = groups.get(g, 0) + 1
        order = sorted(groups.items(), key=lambda kv: (-kv[1], kv[0]))
        shown = order[:1] + [kv for kv in order[1:] if kv[0] in (_RIDER_HELD_GROUP, _RIDER_COST_GROUP)]
        watch = f"watching {len(names)} {word}"
        if shown:
            watch += " - " + ", ".join(f"{n} {'is' if n == 1 else 'are'} {g}" for g, n in shown)
        bits.append(watch)
    else:
        bits.append("no scan reported yet")
    need = f"needs {trigger:g}" if isinstance(trigger, (int, float)) else ""
    pointed = [r for r in scan if str(r.get("direction") or "").upper() in ("LONG", "SHORT")
               and str(r.get("state") or "") != "ENTERED"]          # a market it is in is "riding", above
    if pointed:
        best = max(pointed, key=lambda r: (float(r.get("score") or 0.0), str(r.get("market") or "")))
        guard, reason = _rider_held(best)
        holds = [guard] if guard else []                # a guard first: it refuses the entry whatever the score
        cr = best.get("cost_ratio")
        cost = "costs would eat the move" in reason or (
            rcfg is not None and isinstance(cr, (int, float)) and cr > rcfg.max_cost_ratio)
        if cost and rcfg is not None:
            need = (need + " and " if need else "needs ") + f"costs under {_fraction(rcfg.max_cost_ratio)} of the move"
        elif ", but " in reason:
            holds.append(reason.split(", but ", 1)[1])
        elif reason and "momentum" not in reason:
            need = (need + "; " if need else "") + reason[:1].lower() + reason[1:]
        if holds:
            need = (need + "; " if need else "") + "held back: " + "; also ".join(holds)
        bits.append(f"best {best.get('market')} {str(best.get('direction')).lower()} "
                    f"{_num(best.get('score'), '{:.0f}')}" + (f" ({need})" if need else ""))
    lead = "last hour" if str((w or {}).get("covers") or "").startswith("the last") else "since it started"
    if w is not None and not pointed and isinstance(w.get("best"), dict):
        b = w["best"]
        bits.append(f"best {'in the last hour' if lead == 'last hour' else lead} {b.get('market')} "
                    f"{str(b.get('direction') or '').lower()} "
                    f"{_num(b.get('score'), '{:.0f}')}" + (f" ({need})" if need else ""))
    if w is not None:
        scans = w.get("scans")
        if not scans:
            bits.append(f"{lead}: no scans")
        else:
            held = [x for x in (w.get("blocked_by") or []) if isinstance(x, dict) and x.get("gate") != "one_position"]
            txt = (f"{lead}: {_plural(int(w.get('triggers') or 0), 'trigger', 'triggers')}, "
                   f"{_plural(int(w.get('entries') or 0), 'entry', 'entries')}")
            if held:
                txt += f", held back most by {held[0].get('what') or held[0].get('gate')}"
            bits.append(txt)
    return "; ".join(bits)


def _runner_now(data: Path, now: dt.datetime) -> str:
    st, why = _read_status_file(data / NOW_STATUS_FILES["momentum_runner"])
    gone = _not_reporting(st, why, now)
    if gone:
        return gone
    if str(st.get("status") or "") == "OFF":
        return "OFF - it places no trades"
    opens = [o for o in (st.get("open") or []) if isinstance(o, dict)]
    if opens:
        out = "riding " + ", ".join(f"{o.get('symbol')} {str(o.get('side') or '').lower()} "
                                    f"{_num(o.get('r_now'), '{:+.2f}')} R" for o in opens[:4])
    else:
        skip = [str(t).replace("_", " ").lower() for t in (st.get("skip_tactics") or []) if str(t)]
        out = ("waiting for Trend & Breakout's next index entry"
               + (f" (it skips {' and '.join(skip)})" if skip else ""))
    if st.get("executor_error"):
        out += f"; last order problem: {st.get('executor_error')}"
    return out


def _band_now(data: Path, now: dt.datetime) -> str:
    st, why = _read_status_file(data / NOW_STATUS_FILES["band_breaker"])
    gone = _not_reporting(st, why, now)
    if gone:
        return gone
    status = str(st.get("status") or "")
    opens = [o for o in (st.get("open") or []) if isinstance(o, dict)]
    if status == "OFF":
        return "OFF - it places no trades"
    if opens:
        return "in a trade: " + ", ".join(f"{o.get('symbol')} {str(o.get('side') or '').lower()} "
                                          f"{_num(o.get('r_now'), '{:+.2f}')} R" for o in opens[:4])
    if status == "OUTSIDE SESSION":
        return f"outside its session ({st.get('session') or 'the New York session'})"
    checks = [c for c in (st.get("checks_today") or []) if isinstance(c, dict)]
    if checks:
        c = checks[-1]
        return (f"in its session; last check {c.get('check_ny')} New York on {c.get('symbol')}: "
                f"{c.get('decision') or 'no decision recorded'}")
    notes = {k: v for k, v in (st.get("markets_now") or {}).items() if k != "Mode switch"}
    if notes:
        k = sorted(notes)[0]
        return f"in its session; {k}: {notes[k]}"
    return status.lower() or "running"


def _crowd_now(data: Path, now: dt.datetime) -> str:
    st, why = _read_status_file(data / NOW_STATUS_FILES["crowd_fader"])
    gone = _not_reporting(st, why, now)
    if gone:
        return gone
    status = str(st.get("status") or "")
    opens = [o for o in (st.get("open") or []) if isinstance(o, dict)]
    if status == "OFF":
        return "OFF - it places no trades"
    if status.startswith("NO KEY"):
        return "no Coinversa key saved - it places no trades"
    if opens:
        return "in a trade: " + ", ".join(f"{o.get('symbol')} {str(o.get('side') or '').lower()} "
                                          f"{_num(o.get('r_now'), '{:+.2f}')} R" for o in opens[:4])
    if status == "COINVERSA UNAVAILABLE":
        return f"Coinversa is not answering ({st.get('poll_error') or 'no reason given'}) - no new trades"
    head = "outside its entry hours (managing only)" if status.startswith("OUTSIDE HOURS") else ""
    notes = {k: v for k, v in (st.get("markets_now") or {}).items() if k != "Mode switch"}
    reads = {k: v for k, v in (st.get("reads") or {}).items() if isinstance(v, dict)}
    if reads:
        sym = max(reads, key=lambda k: (float(reads[k].get("score") or 0.0), k))
        r = reads[sym]
        d = r.get("direction") or 0
        lean = " short" if d < 0 else (" long" if d > 0 else "")
        said = notes.get(sym) or r.get("reason") or ""
        best = f"best read {sym} {_num(r.get('score'), '{:.0f}')}{lean}" + (f": {said}" if said else "")
        return f"{head}; {best}" if head else best
    if notes:
        k = sorted(notes)[0]
        return (f"{head}; " if head else "") + f"{k}: {notes[k]}"
    return head or status.lower() or "running"


def _ian_now(data: Path, now: dt.datetime) -> str:
    st, why = _read_status_file(data / NOW_STATUS_FILES["financial_ian"])
    gone = _not_reporting(st, why, now)
    if gone:
        return gone
    feed = st.get("feed") if isinstance(st.get("feed"), dict) else {}
    state = str(feed.get("state") or "UNKNOWN")
    reason = str(feed.get("reason") or "")
    label = str(st.get("data_label") or "LIVE")
    pre = "" if label == "LIVE" else f"running on {label} data, not a result - "
    opens = [o for o in (st.get("open_positions") or []) if isinstance(o, dict)]
    if state == "NOT CONFIGURED":
        out = "no data feed - places no trades (see Financial Ian)"
    elif opens:
        out = "in a trade: " + ", ".join(f"{o.get('symbol')} {str(o.get('side') or '').lower()} "
                                         f"{_num(o.get('r_now'), '{:+.2f}')} R" for o in opens[:4])
    elif state == "LIVE":
        top = st.get("top_opportunity") if isinstance(st.get("top_opportunity"), dict) else None
        if top:
            out = (f"watching the futures order book; top {top.get('spot_symbol')} "
                   f"{str(top.get('spot_side') or 'no side').lower()} {_num(top.get('score'), '{:.0f}')}, "
                   + ("tradeable" if top.get("tradeable") else f"not tradeable: {top.get('why_not') or 'no reason given'}"))
        else:
            out = "watching the futures order book; no opportunity yet"
    elif state == "DEGRADED":
        out = f"data degraded ({reason or 'no reason given'}) - no new trades"
    else:
        out = f"feed {state.lower()} ({reason or 'no reason given'}) - no new signals"
    mt5 = st.get("mt5") if isinstance(st.get("mt5"), dict) else {}
    if not mt5.get("connected"):
        out += "; MetaTrader not connected"
    return pre + out


def _tnb_now(snap: dict) -> str:
    """Trend & Breakout runs in the trader itself: its state, its open trades
    and what it is looking at, from the page's own snapshot."""
    st = snap.get("status") or {}
    state = str(st.get("bot") or "")
    if state == "WAITING FOR METATRADER":
        return f"waiting for MetaTrader ({st.get('waiting') or 'not connected yet'})"
    if state in ("", "STARTING"):
        return "starting - it has not reported yet"
    bits = []
    health = snap.get("health") or {}
    if state != "RUNNING" or health.get("safe_mode"):
        said = "safe mode" if (health.get("safe_mode") or state == "SAFE MODE") else state.lower()
        bits.append(f"{said}: {health.get('summary') or 'no new entries'}")
    paper = str(st.get("tnb_mode") or "").upper() == "PAPER"
    mixed = paper and bool((snap.get("tnb_live") or {}).get("groups"))
    pos = [p for p in (snap.get("positions") or []) if isinstance(p, dict)]
    if pos:
        bits.append(("trades open (real on its LIVE markets, practice on the rest): " if mixed else
                     "trades open (practice, and any left from LIVE): " if paper else "trades open: ")
                    + ", ".join(f"{p.get('symbol')} {str(p.get('side') or '').lower()}" for p in pos[:4]))
    think = [t for t in (snap.get("thinking") or []) if isinstance(t, dict)]
    if think:
        t = min(think, key=lambda x: float(x.get("rank") or 0.0))
        look = (f"looking at {t.get('symbol')} {str(t.get('direction') or '').lower()} "
                f"{_num(t.get('score'), '{:.0f}')} ({str(t.get('tier') or '').lower() or 'no tier'})")
        if t.get("blockers"):
            look += f" - waiting: {(t.get('blockers') or [''])[0]}"
        bits.append(look)
    else:
        bits.append("no scan has completed yet")
    held = [str(r) for r in (st.get("not_trading_because") or []) if r]
    if held:
        bits.append(f"held back: {held[0]}")
    return "; ".join(bits)


def bot_now_lines(snap: dict, top: Optional[dict] = None, only: Optional[str] = None) -> dict:
    """{bot id: what it is doing now, and its last trade} for each card - one
    plain line from that bot's own status file and records (Trend &
    Breakout's from the snapshot), read-only. Never empty: a missing or old
    status file says "not reporting". ``only``: that one bot. Unescaped text."""
    top = top or {}
    sd = snap.get("standing") or {}
    now = to_utc(top.get("now") or utcnow())
    data_dir = top.get("data_dir") or ""
    data = Path(data_dir) if data_dir else None
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")
    tot = top.get("tot") if isinstance(top.get("tot"), dict) else None
    closed = tot.get("closed") if tot is not None and isinstance(tot.get("closed"), list) else None
    unknown = set(sd.get("unknown_magics") or ())
    began = _uk(sd.get("start") or "", "%a %d %b")
    makers = {"momentum_rider": _rider_now, "momentum_runner": _runner_now, "band_breaker": _band_now,
              "crowd_fader": _crowd_now, "financial_ian": _ian_now}
    groups = (snap.get("tnb_live") or {}).get("groups") or ()    # Trend & Breakout's kinds of market kept LIVE
    tactics = (snap.get("tnb_live") or {}).get("tactics") or ()  # 10 Oct: only those approaches (tnb.live_tactics)
    from .verdicts import live_words
    out: dict = {}
    for b in sd.get("bots") or ():
        bid = b.get("id")
        if b.get("retired") or (only is not None and bid != only):
            continue
        mode = str(b.get("mode") or "")
        magic = int(b.get("magic") or DEFAULT_MAGICS.get(bid, 0))
        try:
            if mode == "OFF":
                doing = "OFF - it places no trades"
            elif bid == "market_intelligence":
                doing = _tnb_now(snap)
            elif data is None:
                doing = "not reporting - the page has not been told where the bots keep their files"
            elif bid in makers:
                doing = makers[bid](data, now)
            else:
                continue
        except Exception as exc:                         # never a reason to fail the page
            log.warning("now line of %s: %s", bid, exc)
            doing = f"what it is doing could not be read just now ({exc})"
        try:
            last = _last_trade_txt(data, bid, magic, mode, None if magic in unknown else closed, now, cur, began)
        except Exception as exc:
            log.warning("last trade of %s: %s", bid, exc)
            last = f"its last trade could not be read just now ({exc})"
        if mode == "PAPER" and bid == "market_intelligence" and groups:
            out[bid] = f"{live_words(groups, tactics)} - {last}; {doing}"   # 9 Oct: part LIVE, never plain PAPER
        else:
            out[bid] = f"PAPER - {last}; {doing}" if mode == "PAPER" else f"{doing}; {last}"
    return out


def render_quiet_banner(snap: dict, top: Optional[dict], lines: dict) -> str:
    """One line at the top of the All bots view when no LIVE bot has traded
    for QUIET_MINUTES in market hours: since when, then each LIVE bot's
    "Now:" line. Not shown at weekends (the FX market is shut), when a trade
    is open (or the open trades could not be read), or when a LIVE bot's
    records could not be read (never a false "no trade")."""
    top = top or {}
    sd = snap.get("standing") or {}
    now = to_utc(top.get("now") or utcnow())
    quiet = dt.timedelta(minutes=QUIET_MINUTES)
    if not (fx_market_open(now) and fx_market_open(now - quiet)):
        return ""
    if snap.get("open_live") is None or snap.get("open_live"):
        return ""
    # 10 Oct: Trend & Breakout on PAPER with kinds of market kept LIVE (Formula 1) places real trades too
    groups = (snap.get("tnb_live") or {}).get("groups") or ()
    live = [b for b in sd.get("bots") or () if not b.get("retired")
            and (str(b.get("mode") or "") == "LIVE"
                 or (groups and b.get("id") == "market_intelligence" and str(b.get("mode") or "") == "PAPER"))]
    tot = top.get("tot") if isinstance(top.get("tot"), dict) else None
    if not live or tot is None or not isinstance(tot.get("closed"), list):
        return ""
    magics = {int(b.get("magic") or 0) for b in live}
    if magics & set(sd.get("unknown_magics") or ()):
        return ""
    times = [_when(p.get("closed")) for p in tot["closed"] if p.get("bot_magic") in magics]
    times = [t for t in times if t is not None]
    last = max(times) if times else None
    if last is not None and now - last < quiet:
        return ""
    e = html.escape
    since = (f"since {_uk_at(last, now)}" if last is not None
             else f"since the account started ({_uk(sd.get('start') or '', '%a %d %b') or 'its reset'})")
    rows = "".join(f'<div><b>{e(str(b.get("label")))}</b>: '
                   f'{e(str(lines.get(b.get("id")) or "what it is doing could not be read just now"))}</div>'
                   for b in live)
    return f'<div class="quiet"><div class="qh">No live trade {e(since)} - here is why:</div>{rows}</div>'


def render_standing(snap: dict, top: Optional[dict] = None, strategy: str = "overall", bot: str = "all",
                    base: str = "/") -> str:
    """The filter (the period buttons, then the bot buttons), then clean
    headline cards: the account, and one card per bot, each with the chosen
    period (Today unless another is picked; UK days) and Overall. Every
    figure is MetaTrader's own record, after commission. Then the open
    trades. With a bot chosen (``bot``), that bot alone instead: see
    ``render_bot_view``. Display only."""
    sd = snap.get("standing") or {}
    top = top or {}
    bot = _bot_choice(bot)
    sel, tot = top.get("sel"), top.get("tot")
    p_sel, p_tot = top.get("p_sel") or {}, top.get("p_tot") or {}
    e = html.escape
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")

    def m(v):
        return "-" if v is None else _fmt_money(float(v), cur)
    cls = _cls
    try:
        began = dt.datetime.fromisoformat(str(sd.get("start"))).astimezone(TZ_LONDON).strftime("%a %d %b")
    except Exception:
        began = ""
    shown_period = sel or top.get("period") or {"key": "today"}
    scalper_money = (((sel or {}).get("bots") or {}).get("rapid_scalper") or {}).get("made")
    bar = (render_period_bar(shown_period, strategy, bot, base)
           + render_bot_bar(shown_period, bot, strategy,
                            retired_money=scalper_money is not None and abs(float(scalper_money)) >= 0.005, base=base))
    if not sel or not tot or tot.get("made") is None:
        why = e(str(sd.get("error") or "the page has not heard from MetaTrader yet"))
        missing = (f'<div class="cards"><div class="hc acct bad"><div><div class="nm">Account</div>'
                   f'<div class="fv">-</div><div class="ft">MetaTrader\'s records could not be read just now ({why}). '
                   f'No figure is shown rather than a wrong one; it comes back on its own.</div></div></div></div>')
        if bot != "all":
            return bar + missing + render_bot_view(snap, top, bot)
        return bar + missing + render_open_trades(snap)
    if bot != "all":
        return bar + render_bot_view(snap, top, bot)
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
    why_line = f'<div class="why">{e(str(sel["why"]))}</div>' if show_sel and sel.get("why") else ""
    acct = (f'<div class="hc acct"><div><div class="nm">Account - all bots, after commission</div>'
            f'<div class="figs">{acct_figs}</div><div class="ft">{" &middot; ".join(foot)}</div>{why_line}</div></div>')
    # one card per bot, each with what it is doing now; and, when the LIVE bots have gone quiet, why - at the top
    try:
        now_lines = bot_now_lines(snap, top)
    except Exception as exc:                             # never a reason to fail the page
        log.warning("now lines: %s", exc)
        now_lines = {}
    try:
        quiet = render_quiet_banner(snap, top, now_lines)
    except Exception as exc:
        log.warning("quiet banner: %s", exc)
        quiet = ""
    cards = ""
    retired_notes = []
    for b in sd.get("bots") or []:
        bid, label = b.get("id"), str(b.get("label"))
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
        cards += _bot_card(snap, b, sel, tot, p_sel, p_tot, cur, now_line=now_lines.get(bid, ""))
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
    note += ('<div class="small">Every figure is MetaTrader\'s own record of the trades, after commission. Each trade '
             'counts once, in full (the commission charged when it opened too), on the day it closed, for the bot that '
             'opened it; a trade still open counts when it closes. Overall is what the balance has made since it '
             'started. '
             'A bot in PAPER places no real orders; its practice result is shown in grey and never counted.</div>')
    return bar + quiet + f'<div class="cards">{acct}{cards}</div>' + note + render_open_trades(snap)


def _in_period(view: dict) -> str:
    """How a period reads in a sentence: "today", "this week", "in 01 Oct 2026 to 05 Oct 2026"."""
    key, label = str(view.get("key") or "today"), str(view.get("label") or "Today")
    if key == "custom":
        return ("from " + label) if " to " in label else ("on " + label)
    if key == "total":
        return "in total"
    return label.lower() if key in ("today", "yesterday") or label.lower().startswith("this") else "in the " + label.lower()


def render_bot_trades(rows: list, view: dict, label: str, cur: str = "GBP") -> str:
    """One bot's trades that closed in the period, newest first: LIVE money
    in black (MetaTrader's figure for the whole position, after both halves
    of the commission), practice in grey and labelled practice."""
    e = html.escape
    when = _in_period(view)
    if not rows:
        return (f'<div class="card"><h2>{e(label)} - trades closed {e(when)}</h2>'
                f'<div class="small">None closed {e(when)}.</div></div>')
    fmt = "%H:%M:%S" if view.get("key") in ("today", "yesterday") else "%a %d %b %H:%M"

    def num(v):
        try:
            return f"{float(v):.10g}" if v not in (None, "") and float(v) else "-"
        except (TypeError, ValueError):
            return "-"

    def row(r):
        practice = r.get("mode") != "LIVE"
        closed = r.get("closed")
        t = _uk(closed.isoformat(), fmt) if isinstance(closed, dt.datetime) else "-"
        net = r.get("net")
        money = "-" if net is None else _fmt_money(float(net), cur)
        tag = ' <span class="pill">practice</span>' if practice else ""
        return (f'<tr class="{"practice" if practice else "live"}"><td class="mono nw">{e(t)}</td>'
                f'<td><b>{e(str(r.get("symbol") or "-"))}</b></td><td>{e(str(r.get("side") or "-"))}</td>'
                f'<td class="mono">{num(r.get("volume"))}</td><td class="mono">{num(r.get("entry"))}</td>'
                f'<td class="mono">{num(r.get("exit"))}</td><td class="money mono">{e(money)}{tag}</td>'
                f'<td class="small">{e(str(r.get("reason") or "-"))}</td></tr>')
    head = ('<tr><th>Closed (UK)</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Exit</th>'
            '<th>Result after commission</th><th>Exit reason</th></tr>')
    shown, rest = rows[:30], rows[30:]
    live = [r for r in rows if r.get("mode") == "LIVE"]
    practice = [r for r in rows if r.get("mode") != "LIVE"]
    live_sum = round(sum(float(r.get("net") or 0.0) for r in live), 2)
    prac_sum = round(sum(float(r.get("net") or 0.0) for r in practice), 2)
    foot = []
    if live:
        foot.append(f'LIVE: {len(live)} trade{"" if len(live) == 1 else "s"}, {_fmt_money(live_sum, cur)} - '
                    f'MetaTrader\'s own figures, each trade once and in full (both halves of its commission)')
    if practice:
        foot.append(f'practice: {len(practice)} trade{"" if len(practice) == 1 else "s"}, '
                    f'{_fmt_money(prac_sum, cur)} - simulated, not money, never in the account')
    return (f'<div class="card"><h2>{e(label)} - trades closed {e(when)} - {len(rows)}</h2>'
            f'<div class="scroll"><table class="trades">{head}{"".join(row(r) for r in shown)}</table></div>'
            + (f'<details id="bot-trades-rest"><summary>{len(rest)} earlier trades</summary><div class="scroll">'
               f'<table class="trades">{head}{"".join(row(r) for r in rest)}</table></div></details>' if rest else "")
            + f'<div class="small">{"; ".join(foot)}.</div></div>')


def render_bot_view(snap: dict, top: Optional[dict], bot: str) -> str:
    """One bot alone, for the chosen period: its card (mode, the period,
    Overall, what it has open), the trades it closed in the period, its open
    trades, and its own live view. Renders whatever is missing as missing."""
    from .standing import BOTS
    e = html.escape
    top = top or {}
    sd = snap.get("standing") or {}
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")
    b = next((x for x in sd.get("bots") or () if x.get("id") == bot), None)
    label = str((b or {}).get("label") or dict(BOTS).get(bot, bot))
    magic = int((b or {}).get("magic") or DEFAULT_MAGICS.get(bot, 0))
    mode = str((b or {}).get("mode") or "")
    sel, tot = top.get("sel"), top.get("tot")
    parts = []
    if b is not None and sel and tot and tot.get("made") is not None:
        try:
            now_line = bot_now_lines(snap, top, only=bot).get(bot, "")
        except Exception as exc:                          # never a reason to fail the page
            log.warning("now line of %s: %s", bot, exc)
            now_line = ""
        parts.append(f'<div class="cards one">{_bot_card(snap, b, sel, tot, top.get("p_sel") or {}, top.get("p_tot") or {}, cur, alone=True, now_line=now_line)}</div>')
        if magic in set(sd.get("unknown_magics") or ()):
            parts.append(f'<div class="card"><h2>{e(label)} - trades closed {e(_in_period(sel))}</h2><div class="small bad">'
                         f'MetaTrader\'s records for its magic number {magic} could not be read just now, so its trades '
                         f'are not listed rather than listed wrong; they come back on their own.</div></div>')
        else:
            try:
                rows = bot_trade_rows(top.get("data_dir") or "", bot, sel, magic)
            except Exception as exc:                      # never a reason to fail the page
                log.warning("trades of %s: %s", bot, exc)
                rows = []
            parts.append(render_bot_trades(rows, sel, label, cur))
    parts.append(render_open_trades(snap, magic=magic, title=f"{label} - open trades right now"))
    parts.append(_bot_live_view(snap, bot, top.get("data_dir") or "", mode, top.get("now")))
    return "".join(parts)


def render_thinking(snap: dict) -> str:
    """What Trend & Breakout is looking at right now: its ranked markets."""
    think = snap.get("thinking") or []
    if not think:
        return ('<div class="card"><h2>What the bot is looking at right now</h2>'
                '<div class="small">No scan has completed yet.</div></div>')

    def trow(t):
        return (f'<tr><td class="mono">{html.escape(str(t.get("rank", "")))}</td><td><b>{html.escape(str(t.get("symbol", "")))}</b> '
                f'{html.escape(str(t.get("direction", "")))}</td>'
                f'<td class="mono">{_num(t.get("score"), "{:.0f}")} {html.escape(str(t.get("tier", "")))}</td>'
                f'<td>{html.escape(str(t.get("tactic") or "").replace("_", " ").title())}'
                f'<div class="small">{html.escape(str(t.get("regime", "")))}'
                + (f' - waiting: {html.escape("; ".join(str(x) for x in t["blockers"]))}'
                   if t.get("blockers") else "")
                + '</div></td></tr>')
    thead = '<tr><th>#</th><th>Market</th><th>Score</th><th>Approach and why</th></tr>'
    first, rest = think[:4], think[4:]
    return ('<div class="card"><h2>What the bot is looking at right now</h2>'
            '<table>' + thead + "".join(trow(t) for t in first) + '</table>'
            + (f'<details id="thinking-rest"><summary>{len(rest)} more markets</summary><table>'
               + "".join(trow(t) for t in rest) + '</table></details>' if rest else "")
            + '</div>')


def _panel_snap(snap: dict, key: str, data_dir: str, status_file: str, sid: str, label: str,
                now: Optional[dt.datetime] = None) -> dict:
    """The status block a folded panel reads: the snapshot's own, else its
    status file (read-only, NOT RUNNING when missing)."""
    st = (snap.get("strategies") or {}).get(key)
    if not st and data_dir:
        if key == "scalper":
            from .attribution import read_scalper_status
            st = read_scalper_status(data_dir, now=now)
        else:
            from .attribution import _read_status
            st = _read_status(Path(data_dir) / status_file, sid, label, to_utc(now or utcnow()))
    return {"strategies": {key: st}} if st else {}


def _bot_live_view(snap: dict, bot: str, data_dir: str, mode: str, now: Optional[dt.datetime] = None) -> str:
    """The bot's own live view, from the renderers of "Each bot in detail"
    and its status file. Never fails the page."""
    e = html.escape
    try:
        now = to_utc(now or utcnow())
        if bot == "market_intelligence":
            groups = (snap.get("tnb_live") or {}).get("groups") or ()
            if mode != "LIVE" and groups:              # 9 Oct evening: Formula 1 LIVE, the rest on PAPER
                from .verdicts import live_kinds
                tactics = (snap.get("tnb_live") or {}).get("tactics") or ()     # 10 Oct: tnb.live_tactics
                what = (f"It is on PAPER except its {live_kinds(groups, tactics)}: those orders go to MetaTrader "
                        f"with real money; everything else is decided on real prices and simulated, and the "
                        f"Momentum Runner rides its index entries with real orders.")
            else:
                what = ("It is on PAPER: it decides on real prices and its orders are simulated, while the Momentum "
                        "Runner rides its index entries with real orders." if mode == "PAPER" else
                        "It is LIVE: its orders go to MetaTrader, and the Momentum Runner rides its index entries.")
            return f'<div class="card small">{e(what)}</div>' + render_thinking(snap)
        if bot == "momentum_rider":
            return _rider_live_html(data_dir, now)
        if bot == "financial_ian":
            return _ian_live_html(data_dir, now)
        panels = {"momentum_runner": ("runner", "runner-status.json", "momentum_runner", "Momentum Runner",
                                      render_runner_panel),
                  "band_breaker": ("bandbreaker", "bandbreaker-status.json", "band_breaker", "Band Breaker",
                                   render_bandbreaker_panel),
                  "crowd_fader": ("crowd", "crowd-status.json", "crowd_fader", "Crowd Fader", render_crowd_panel),
                  "rapid_scalper": ("scalper", "scalper-status.json", "rapid_scalper", "Rapid Scalper",
                                    render_scalper_panel)}
        if bot in panels:
            key, fname, sid, label, fn = panels[bot]
            out = fn(_panel_snap(snap, key, data_dir, fname, sid, label, now))
            return out or f'<div class="card small">{e(label)} has not reported yet.</div>'
    except Exception as exc:
        log.warning("live view of %s: %s", bot, exc)
        return f'<div class="card small">Its live view could not be shown just now ({e(str(exc))}).</div>'
    return ""


def render_status(snap: dict, strategy: str = "overall", headline: Optional[dict] = None, bot: str = "all",
                  base: str = "/") -> str:
    """The full page: everything, for the technical detail. Served at
    /details (``base``), its links stay there; the main page "/" is the
    simple one (``render_home``)."""
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
        tile("Today (UK day)" if st.get("day_basis") == "UK" else "Today (broker's day)",
             _fmt_money(st.get("today_pnl", 0.0), st.get("currency", "")),
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

    think_html = render_thinking(snap)

    build_html = ""
    if st.get("build"):
        build_html = (f'Build <b class="mono">{html.escape(str(st["build"]))}</b>'
                      f' running since {html.escape(str(st.get("started", "")))}. '
                      f'Compare this code with the one in the latest message from the installer. ')
    src = str(st.get("pnl_source") or "")
    src_warn = ""
    kept = (snap.get("tnb_live") or {}).get("groups") or ()
    if str(st.get("tnb_mode") or "").upper() == "PAPER" and kept:
        from .verdicts import live_kinds              # 9 Oct: its minor pairs (Formula 1) trade for real
        tactics = (snap.get("tnb_live") or {}).get("tactics") or ()       # 10 Oct: tnb.live_tactics
        src_html = (f'Trend &amp; Breakout is on PAPER except its {html.escape(live_kinds(kept, tactics))}, which '
                    f'trade LIVE with real money: Today, Win rate today, This strategy and Open trades are its real trades '
                    f'there and its practice together (the practice on real prices with simulated orders, never '
                    f'added to the account; its real money alone is on the main page and in Overall); Equity is '
                    f'the real account.')
    elif str(st.get("tnb_mode") or "").upper() == "PAPER":
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
                        + ('<div class="small">This strategy\'s trades only, each in full on the UK day it '
                           'closed (midnight to midnight UK).</div></div>' if st.get("day_basis") == "UK" else
                           '<div class="small">This strategy\'s trades only, by the time they '
                           'closed, on the broker\'s day - the same way MetaTrader\'s History '
                           'filter counts them.</div></div>'))
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

    parts = _strategy_parts(snap, strategy, base)
    col1 = parts.get("figures", "") + parts.get("detail", "")
    col2 = periods_html + pos_html + why_html + think_html
    col3 = parts.get("scalper", "") + parts.get("trades", "")
    more = (f'<details class="more" id="more"><summary>Each bot in detail, open trades, what the bots are looking '
            f'at, connections</summary><div class="row">{tiles}</div>{src_warn}'
            f'<p class="small">{src_html} (These folded figures are each bot\'s own; the figures at the top are '
            f'the broker\'s.)</p>'
            f'{parts.get("tabs", "")}<div class="grid"><div>{col1}</div><div>{col2}</div><div>{col3}</div></div>'
            f'</details>')
    body = f"""<div class="legacy"><div class="top">
<div class="banner {banner_cls}">{html.escape(banner_txt)}</div></div>
{prob_html}
{render_standing(snap, headline, strategy, bot, base)}
{more}
<p class="small">{build_html}Updated {html.escape(str(snap.get('updated')))} (UTC).
This page refreshes itself every 10 seconds.</p>
</div>"""
    view = (headline or {}).get("sel") or (headline or {}).get("period") or {"key": "today"}
    return _shell("Trading bot status", body, active="details", q=_period_q(view), old_css=True,
                  heading="Technical detail", stamp=_stamp(snap.get("updated")))


# ------------------------------------------------------- the simple page --
# Aaron, 9 Oct 16:45 UK: "turn the UI of the bot display into something really easy for me to read ... Just a simple
# dashboard showing the results for today and all other filters (always correctly), the bots' overall logic on what's
# happened and its recommendations for each separate bot and a nice simple but intelligent plan on where we're going,
# what we're looking to bin off, change or keep."
#
# "/" is that page: the period buttons, the account (balance, the chosen period, Overall), one row per bot with its
# verdict and what to do (mintel/ops/verdicts.py), then the plan. "/?bot=<id>" is one bot: what happened, the recommendation and the
# evidence, above its existing detail. Every money figure is the same one the full page shows (standing.period_view
# for real money, standing.practice_figures for practice); "/details" is the full page, unchanged.
HOME_CSS = """
:root{--bg:#eef4f3;--surface:#ffffff;--surface-2:#f4f8f7;--line:#dbe6e4;--text:#12302d;--text-2:#3f5a56;
--text-3:#73908b;--navy:#0b5a52;--accent:#14b8a6;--accent-soft:#dff5f2;--good:#0e9f6e;--good-soft:#e2f6ee;
--bad:#d92d20;--bad-soft:#fdeceb;--warn:#b7791f;--warn-soft:#fdf3dc;--info:#2f6fed;--info-soft:#e8f0fe;
--prac:#9aa3b8;--shadow:0 1px 2px rgba(18,48,45,.06);--side:#0b3b38;--side-text:#cfe7e3;--side-dim:#7fb3ab}
:root[data-theme="dark"]{--bg:#0f1524;--surface:#171f33;--surface-2:#1d2740;--line:#2a3552;--text:#e8ecf5;
--text-2:#b4bdd2;--text-3:#8490ab;--navy:#5eead4;--accent:#2dd4bf;--accent-soft:#103a36;--good:#34d399;
--good-soft:#123328;--bad:#f87171;--bad-soft:#3b1a1a;--warn:#fbbf24;--warn-soft:#3a2d10;--info:#7aa2ff;
--info-soft:#1a2748;--prac:#6b7590;--shadow:none;--side:#082624;--side-text:#cfe7e3;--side-dim:#6fa39b}
*{box-sizing:border-box}
html,body{margin:0;padding:0;overflow-x:hidden;max-width:100%;-webkit-text-size-adjust:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Roboto,Helvetica,Arial,sans-serif;
background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:var(--info);text-decoration:none}
h1,h2,h3{font-weight:650;letter-spacing:-.01em;margin:0}
/* the shell: a sidebar like the AI hub's */
.app{display:flex;min-height:100vh}
.side{width:216px;flex:none;background:var(--side);border-right:1px solid var(--side);display:flex;
flex-direction:column;position:sticky;top:0;height:100vh}
.side-top{padding:20px 18px 14px}
.brand{font-size:20px;font-weight:700;color:#fff;letter-spacing:-.01em;white-space:nowrap}
.brand i{color:var(--accent);font-style:normal}
.side-top small{display:block;color:var(--side-dim);font-size:12px;margin-top:2px}
.side nav{padding:6px 10px;display:flex;flex-direction:column;gap:2px;flex:1;overflow:auto;font-size:14px}
.nav-label{font-size:11px;font-weight:650;letter-spacing:.06em;text-transform:uppercase;color:var(--side-dim);
padding:14px 10px 6px}
.side nav a{display:flex;align-items:center;gap:10px;margin:0;border-radius:9px;padding:9px 10px;
color:var(--side-text);font-weight:550;white-space:nowrap}
.side nav a:hover{background:rgba(255,255,255,.08);text-decoration:none}
.side nav a.on{background:var(--accent);color:#062420}
:root[data-theme="dark"] .side nav a.on{background:var(--accent);color:#062420}
.side nav svg{width:17px;height:17px;flex:none;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;
stroke-linejoin:round}
.side-foot{padding:14px 18px;border-top:1px solid rgba(255,255,255,.1);font-size:12px;color:var(--side-dim);display:flex;
align-items:center;justify-content:space-between;gap:8px}
.linkbtn{background:none;border:0;color:var(--text-3);cursor:pointer;font:inherit;font-size:12.5px;padding:0}
.linkbtn:hover{color:var(--text);text-decoration:underline}
.side .linkbtn{color:var(--side-dim)}.side .linkbtn:hover{color:#fff}
.main{flex:1;min-width:0;overflow-x:hidden}
.topbar{display:flex;align-items:center;gap:12px;padding:18px 28px 0;flex-wrap:wrap}
.topbar h1{font-size:22px;flex:1;min-width:0}
.topbar .grow{flex:1}
.topbar .back{font-size:13px;font-weight:600;white-space:nowrap}
.stamp{color:var(--text-3);font-size:12.5px;white-space:nowrap}
.content{padding:18px 28px 60px;max-width:1240px}
/* building blocks */
.card{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:var(--shadow);
margin-bottom:16px;color:var(--text)}
.card-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:12px;
flex-wrap:wrap}
.card-head h2{font-size:15px}
.sub{color:var(--text-3);font-size:12.5px}
a.card:hover{box-shadow:0 6px 18px rgba(22,33,62,.08);text-decoration:none}
.g2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin-bottom:16px}
.g2.top{align-items:start}
.g2>.card{margin-bottom:0}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin-bottom:16px}
.kpis.three{grid-template-columns:repeat(3,minmax(0,1fr))}
.kpis>.card{margin-bottom:0}
.kpi .label{font-size:12.5px;color:var(--text-3);font-weight:600}
.kpi .value{font-size:28px;font-weight:700;letter-spacing:-.02em;margin:2px 0;font-variant-numeric:tabular-nums;
white-space:nowrap}
.kpi .foot{font-size:12px;color:var(--text-3);margin-top:6px}
.kpi-note{font-size:12.5px;color:var(--text-3);margin:-4px 0 16px}
.pos{color:var(--good)}.neg{color:var(--bad)}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;background:var(--text-3);flex:none}
.dot.green{background:var(--good)}.dot.red{background:var(--bad)}.dot.amber{background:var(--warn)}
.dot.blue{background:var(--info)}.dot.grey{background:var(--line)}
/* the period buttons: the hub's segmented control */
.periodbar{display:flex;align-items:center;gap:10px 14px;flex-wrap:wrap;margin-bottom:16px}
.seg{display:inline-flex;flex-wrap:wrap;border:1px solid var(--line);border-radius:9px;overflow:hidden;
background:var(--surface)}
.seg a,.seg span{padding:6px 12px;font-size:12.5px;font-weight:600;color:var(--text-3);white-space:nowrap}
.seg a:hover{background:var(--surface-2);text-decoration:none}
.seg .on{background:var(--navy);color:#fff}
:root[data-theme="dark"] .seg .on{color:#0f1524}
.periodbar form{display:inline-flex;align-items:center;gap:6px;flex-wrap:wrap;margin:0;font-size:12.5px;
color:var(--text-3)}
.periodbar input[type=date]{padding:4px 8px;border:1px solid var(--line);border-radius:9px;background:var(--surface);
color:var(--text);font:inherit;font-size:12.5px}
:root[data-theme="dark"] .periodbar input[type=date]{color-scheme:dark}
.periodbar button{padding:5px 12px;border:1px solid var(--line);border-radius:9px;background:var(--surface);
color:var(--text);font:inherit;font-size:12.5px;font-weight:600;cursor:pointer}
.periodbar button.on{background:var(--navy);border-color:var(--navy);color:#fff}
/* every bot and every lab idea as a tile */
.bt-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px}
.bt{display:flex;flex-direction:column;gap:10px;background:var(--surface);border:1px solid var(--line);
border-radius:12px;padding:13px 14px;color:var(--text);transition:box-shadow .12s,transform .12s;min-width:0}
.bt:hover{box-shadow:0 6px 18px rgba(22,33,62,.08);transform:translateY(-1px);text-decoration:none}
.bt.prac{background:var(--surface-2)}
.bt-head{display:flex;align-items:flex-start;gap:8px;min-width:0}
.bt-name{font-weight:650;font-size:14px;line-height:1.35;min-width:0;overflow-wrap:anywhere}
.bt-star{color:var(--accent);font-size:13px}
.bt-st{margin-left:auto;display:inline-block;font-size:10.5px;font-weight:700;letter-spacing:.05em;padding:2px 8px;
border-radius:999px;white-space:nowrap;background:var(--surface-2);color:var(--text-3)}
.bt-st.live{background:var(--navy);color:#fff}
:root[data-theme="dark"] .bt-st.live{color:#0f1524}
.bt-st.practice{background:var(--info-soft);color:var(--info)}
.bt-st.testing{background:var(--warn-soft);color:var(--warn)}
.bt-st.off{background:var(--surface-2);color:var(--text-3);border:1px solid var(--line)}
.bt-figs{display:grid;grid-template-columns:1fr 1fr 52px;gap:8px}
.bt-figs.two{grid-template-columns:1fr 52px}
.bt-m,.bt-t{font-size:16px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap;min-width:0}
.bt-t{text-align:right}
.bt-m::before,.bt-t::before{content:attr(data-k);display:block;font-size:11px;font-weight:600;color:var(--text-3);
letter-spacing:.02em}
.bt-m.prac,.bt-t.prac,.bt-m.prac .pos,.bt-m.prac .neg{color:var(--prac)}
.bt-test{font-size:12.5px;font-weight:650;color:var(--text-3);padding:6px 0}
.bt-v{font-size:13px;font-weight:650;display:flex;align-items:center;color:var(--text-3);margin-top:auto}
.bt-v.green{color:var(--good)}.bt-v.red{color:var(--bad)}.bt-v.amber{color:var(--warn)}.bt-v.blue{color:var(--info)}
.bt-none{color:var(--text-3);font-size:13.5px;margin:0}
/* Home's headline numbers (13 Oct): on real money, testing, this week's pick */
.wk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px}
.wk-grid.counts{grid-template-columns:repeat(6,minmax(0,1fr));margin-bottom:12px}
.wk-t{display:flex;flex-direction:column;gap:6px;background:var(--surface);border:1px solid var(--line);
border-radius:12px;padding:12px 14px;min-width:0}
.wk-t.prac{background:var(--surface-2)}
.wk-n{font-size:12px;font-weight:600;color:var(--text-3);overflow-wrap:anywhere}
.wk-v{font-size:26px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums;white-space:nowrap}
.wk-nums{display:grid;grid-template-columns:1fr 1fr;gap:6px 10px}
.wk-nums div{font-size:17px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap}
.wk-nums div::before{content:attr(data-k);display:block;font-size:10.5px;font-weight:600;color:var(--text-3)}
.wk-f{font-size:12px;color:var(--text-3);font-variant-numeric:tabular-nums}
.wk-c{align-self:flex-start;font-size:10.5px;font-weight:700;letter-spacing:.05em;padding:2px 8px;border-radius:999px;
background:var(--surface-2);color:var(--text-3);white-space:nowrap}
.wk-c.making{background:var(--good);color:#fff}.wk-c.losing{background:var(--bad);color:#fff}
.wk-c.ready{background:var(--navy);color:#fff}
.wk-h{font-size:13px;font-weight:650;color:var(--text-2);margin:14px 0 8px}
.wk-row{display:grid;grid-template-columns:minmax(0,1fr) 110px 150px 130px;gap:10px;align-items:center;
padding:9px 8px;border-top:1px solid var(--line)}
.wk-row:first-child{border-top:0}
.wk-name{font-weight:600;color:var(--text);min-width:0;overflow-wrap:anywhere}
.wk-money{font-size:17px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap;text-align:right}
.wk-chip{text-align:right;white-space:nowrap}.wk-chip .wk-c{align-self:auto}
.wk-early{font-size:10.5px;font-weight:600;color:var(--text-3);border:1px solid var(--line);border-radius:999px;padding:0 6px;margin-left:4px}
.wk-total{border-color:var(--accent)}
.wk-h:first-child{margin-top:0}
details.wk-all>summary{cursor:pointer;font-size:13px;font-weight:600;color:var(--text-3);margin:4px 0 12px}
.wk-gap{font-size:11px;font-weight:600;color:var(--text-3);text-align:center;padding:6px 0;border-top:1px solid var(--line)}
.wk-more{display:inline-block;margin-top:10px;font-size:13px;font-weight:650;color:var(--accent);text-decoration:none}
.wk-name a{color:inherit;text-decoration:none}.wk-name a:hover{text-decoration:underline}
details.wk-wait{margin-top:10px;border-top:1px solid var(--line);padding-top:8px}
details.wk-wait>summary{cursor:pointer;font-size:13px;font-weight:600;color:var(--text-3)}
details.wk-wait .wk-f{margin-top:6px;overflow-wrap:anywhere}
/* the lab and the coach, small */
.mini-title{display:flex;align-items:center;justify-content:space-between;font-size:15px;font-weight:650;
margin-bottom:10px}
.mini-title .go{color:var(--text-3);font-size:13px;font-weight:600}
.lab-big{font-size:28px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.lab-big small{font-size:14px;font-weight:600;color:var(--text-2);letter-spacing:0;margin-left:6px}
.lab-one{font-size:14px;font-weight:600;color:var(--text)}
.prog{height:6px;background:var(--surface-2);border-radius:999px;overflow:hidden;margin-top:10px}
.prog span{display:block;height:100%;background:var(--accent);border-radius:999px}
.funnel{display:grid;grid-template-columns:repeat(auto-fit,minmax(70px,1fr));gap:8px}
.funnel div{background:var(--surface-2);border-radius:10px;padding:8px 10px}
.funnel b{display:block;font-size:20px;font-variant-numeric:tabular-nums}
.funnel span{font-size:11.5px;color:var(--text-3);font-weight:600}
.grey-line{font-size:12.5px;color:var(--text-3);margin:8px 0 0}
.flag{display:flex;gap:12px;align-items:center;border-radius:14px;padding:12px 16px;margin-bottom:16px;
background:var(--bad-soft);color:var(--bad);border:1px solid var(--line);font-size:13.5px}
.flag .light{width:10px;height:10px;border-radius:50%;flex:none;background:var(--bad)}
/* the detail pages' content (each bot, /lab, /coach) */
.s-card{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:18px;margin-bottom:16px;
box-shadow:var(--shadow)}
.s-card h1,.s-v h1{font-size:20px;margin:0 0 6px}
.s-card h2{font-size:12px;margin:0 0 10px;text-transform:uppercase;letter-spacing:.05em;color:var(--text-3);
font-weight:650}
.s-card h3,.s-group{font-size:11.5px;margin:0 0 8px;text-transform:uppercase;letter-spacing:.05em;
color:var(--text-3)}
.s-k{font-size:12.5px;color:var(--text-3);font-weight:600;margin-bottom:2px}
.s-big{font-size:28px;font-weight:700;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.s-sub,.s-note,.s-small{font-size:13px;color:var(--text-2);margin:4px 0 0}
.s-prac,.s-prac .pos,.s-prac .neg{color:var(--prac)}
.s-aim{font-size:17px;font-weight:650;margin:0 0 12px}
.s-focus{background:var(--surface-2);border-radius:10px;padding:12px 14px;margin-bottom:14px}
.s-steps{margin:4px 0 16px;padding-left:22px}
.s-steps li{margin-bottom:6px}
.s-cols{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
.s-cols ul{margin:0;padding-left:18px;font-size:13.5px}
.s-cols li{margin-bottom:8px}
.s-cols summary,.s-more summary{cursor:pointer}
.s-cols details .s-why{display:block;margin:2px 0 0 14px}
.s-more{margin-top:14px;font-size:13px;color:var(--text-3)}
.s-more .s-src{margin-top:6px}
.s-why{color:var(--text-2)}
.s-flag{margin-top:14px;padding:10px 14px;border-radius:10px;background:var(--warn-soft);color:var(--warn);
font-size:13.5px}
.s-src{font-size:12px;color:var(--text-3);margin:14px 0 0}
.s-bot{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);gap:8px 24px;padding:14px 6px;
border-top:1px solid var(--line);color:var(--text)}
.s-bot:first-of-type{border-top:0}
.s-bot:hover{background:var(--surface-2);text-decoration:none}
.s-hl{font-size:13.5px;color:var(--text-2);margin-top:4px}
.s-rec{font-size:13.5px;color:var(--text);margin-top:4px}
.s-nums{display:flex;gap:20px;flex-wrap:wrap;justify-content:flex-end;text-align:right}
.s-m{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums}
.s-m0{font-size:15px;font-weight:600;font-variant-numeric:tabular-nums}
.s-extra{grid-column:1/-1;font-size:13px;color:var(--text-3)}
.s-mode{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.05em;padding:2px 9px;border-radius:999px;
vertical-align:middle;margin-left:8px;background:var(--surface-2);color:var(--text-3)}
.s-mode.live{background:var(--navy);color:#fff}
.s-chip{display:inline-block;font-size:11.5px;font-weight:700;letter-spacing:.04em;padding:2px 9px;
border-radius:999px;vertical-align:middle;margin-left:8px;white-space:nowrap}
.s-chip.blue{background:var(--info-soft);color:var(--info)}.s-chip.sky{background:var(--good-soft);color:var(--good)}
.s-chip.sand{background:var(--warn-soft);color:var(--warn)}.s-chip.amber{background:var(--bad-soft);color:var(--bad)}
.s-chip.grey{background:var(--surface-2);color:var(--text-3)}
.s-v p{margin:0 0 14px}
.s-v h2{margin-top:20px}
.s-ev{list-style:none;margin:0;padding:0}
.s-ev li{padding:10px 0;border-top:1px solid var(--line);font-size:13.5px}
.s-ev b{display:block;margin-bottom:2px}
.s-old{overflow-x:auto}
.s-old table{display:block;max-width:100%;overflow-x:auto}
.s-stage{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.05em;padding:2px 8px;border-radius:6px;
vertical-align:middle;margin-left:8px;background:var(--surface-2);color:var(--text-2);white-space:nowrap}
.s-checks{display:flex;flex-wrap:wrap;gap:6px;grid-column:1/-1;font-size:12.5px}
.s-ck{padding:1px 9px;border:1px solid var(--line);border-radius:999px;background:var(--surface);white-space:nowrap}
.s-ck.ok{color:var(--good)}.s-ck.no{color:var(--bad)}.s-ck.run{color:var(--info)}.s-ck.ny{color:var(--text-3)}
.s-group{margin:20px 0 2px}
.s-cname{font-weight:650;font-size:15px}
.s-sim{font-size:13px;color:var(--text-3);text-align:right;flex:1 1 100%}
.s-none{font-size:13.5px;color:var(--text-3);margin:0}
.s-changes{margin:0;padding-left:18px;font-size:13.5px}
.s-changes li{margin-bottom:4px}
.s-test{margin:0 0 16px;padding:10px 14px;border-radius:10px;background:var(--warn-soft);color:var(--warn);
font-size:14px}
.s-lt th,.s-lt td{text-align:left;padding:6px 12px 6px 0;border-bottom:1px solid var(--line);font-size:13px;
vertical-align:top}
.s-lt th{font-size:11.5px;color:var(--text-3);font-weight:650}
.s-lt pre{white-space:pre-wrap;font-size:12px;margin:0}
/* the older detail (each bot's cards, /details) inside the same shell */
.legacy .card,.legacy .box,.legacy .hc{background:var(--surface);border:1px solid var(--line);border-radius:14px;
padding:14px 16px;margin-bottom:12px;box-shadow:var(--shadow);color:var(--text)}
.legacy .tile{background:var(--surface);border-color:var(--line);border-radius:12px}
.legacy h2{color:var(--text-3)}
.legacy th,.legacy td{border-bottom-color:var(--line)}
.legacy .tab,.legacy .dates button{background:var(--surface);border-color:var(--line);color:var(--text)}
.legacy .tab.on,.legacy .dates button.on{background:var(--navy);border-color:var(--navy);color:#fff}
.legacy .small,.legacy .k,.legacy .fk,.legacy th{color:var(--text-3)}
.legacy .st,.legacy .hc.paper,.legacy .st.big{background:var(--surface-2);border-color:var(--line)}
:root[data-theme="dark"] .legacy .hero,:root[data-theme="dark"] .legacy .quiet{background:var(--surface);
color:var(--text)}
@media (max-width:980px){.kpis,.kpis.three{grid-template-columns:repeat(2,minmax(0,1fr))}
.g2{grid-template-columns:minmax(0,1fr)}.s-cols{grid-template-columns:1fr}}
/* phones: the sidebar becomes a top bar */
@media (max-width:760px){
.app{flex-direction:column}
.side{position:sticky;top:0;z-index:20;width:100%;height:auto;flex-direction:row;align-items:center;
border-right:0;border-bottom:1px solid var(--line);min-width:0}
.side-top{padding:10px 12px 10px 16px;flex:none}
.side-top small,.nav-label,.side-foot{display:none}
.brand{font-size:16px}
.side nav{flex-direction:row;overflow-x:auto;padding:6px 16px 6px 4px;gap:2px;scrollbar-width:none;min-width:0}
.side nav::-webkit-scrollbar{display:none}
.side nav a{padding:7px 10px;font-size:13px}
.side nav svg{display:none}
.topbar{padding:14px 16px 0}
.topbar h1{font-size:19px}
.content{padding:14px 16px 40px}
.seg{flex-wrap:nowrap;overflow-x:auto;max-width:100%;scrollbar-width:none}
.seg::-webkit-scrollbar{display:none}
.kpi .value{font-size:22px}
.kpis,.kpis.three{gap:10px}
.kpis>.card{padding:14px}
.bt-grid{grid-template-columns:minmax(0,1fr);gap:8px}
.wk-grid.counts{grid-template-columns:repeat(3,minmax(0,1fr))}.wk-v{font-size:22px}
.wk-row{grid-template-columns:minmax(0,1fr) auto;gap:4px 8px;padding:8px 4px}
.wk-row .wk-f{font-size:11px}.wk-money{font-size:15px}
#bots{padding:14px 12px}
.bt{padding:11px 12px;gap:7px}
.s-bot{grid-template-columns:1fr}
.s-nums{justify-content:flex-start;text-align:left}
.s-sim{text-align:left}
input,select{font-size:16px}
}
"""


# ------------------------------------------------ the shell, like the AI hub's (12 Oct) --
# Aaron, 12 Oct 2026: "Put the layout similar to the ai hub, i don't like this it's just messy and i can't figure out
# what's going on there's too much writing still". Every page sits in the same shell: a left sidebar (a top bar on
# phones) with grouped links, a title bar, then white cards on a light grey page - the hub's look, inline only.
THEME_JS = """<script>
(function(){var t='light';try{t=localStorage.getItem('mintel-theme')||'light'}catch(e){}
document.documentElement.setAttribute('data-theme',t);})();
function toggleTheme(){var t=document.documentElement.getAttribute('data-theme')==='dark'?'light':'dark';
try{localStorage.setItem('mintel-theme',t)}catch(e){}document.documentElement.setAttribute('data-theme',t);
var b=document.getElementById('theme-btn');if(b)b.textContent=t==='dark'?'Light':'Dark';}
</script>"""

_ICONS = {
    "home": '<path d="M3 11l9-7 9 7"/><path d="M5 10v10h14V10"/>',
    "live": '<circle cx="12" cy="12" r="9"/><path d="M14.8 9.2c-.6-.8-1.6-1.2-2.8-1.2-1.7 0-3 .9-3 2.2 0 3 6 1.5 6 '
            '4.5 0 1.3-1.3 2.3-3 2.3-1.3 0-2.4-.5-3-1.3M12 6v2M12 16.8V18"/>',
    "practice": '<path d="M4 20V10M10 20V4M16 20v-7M21 20H3"/>',
    "lab": '<path d="M9 3h6M10 3v6l-5 9a2 2 0 0 0 1.7 3h10.6a2 2 0 0 0 1.7-3l-5-9V3"/><path d="M7.5 14h9"/>',
    "coach": '<circle cx="12" cy="12" r="9"/><path d="M9.5 9.5a2.5 2.5 0 1 1 3.6 2.2c-.7.4-1.1.9-1.1 1.8M12 17h.01"/>',
    "details": '<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.9 4.9l2.1 2.1M17 17l2.1 '
               '2.1M4.9 19.1 7 17M17 7l2.1-2.1"/>',
    "health": '<path d="M3 12h4l2-6 4 12 2-6h6"/>',
}
NAV_GROUPS = (("Overview", (("home", "Home", "/"),)),
              ("Bots", (("live", "Real money", "/?show=live"), ("practice", "Practice", "/?show=practice"),
                        ("lab", "The lab", "/lab"))),
              ("Behind the scenes", (("coach", "The coach", "/coach"), ("details", "Technical detail", "/details"),
                                     ("health", "Health", "/health"))))


def _period_q(view: Optional[dict]) -> str:
    """'period=week' (and a custom period's dates) - what every link keeps; '' for Today."""
    v = view or {}
    if str(v.get("key") or "today") == "today":
        return ""
    return _keep(v, "all", "overall")


def _with_q(href: str, q: str) -> str:
    if not q or href in ("/health", "/coach", "/lab"):
        return href
    return f"{href}{'&' if '?' in href else '?'}{q}"


def _nav_html(active: str, q: str = "") -> str:
    """The sidebar's grouped links, the chosen period kept on the pages that show money."""
    out = []
    for label, links in NAV_GROUPS:
        out.append(f'<div class="nav-label">{html.escape(label)}</div>')
        for key, words, href in links:
            on = ' class="on"' if key == active else ""
            out.append(f'<a href="{_with_q(href, q)}"{on}><svg viewBox="0 0 24 24">{_ICONS[key]}</svg>'
                       f'{html.escape(words)}</a>')
    return f'<nav class="nav">{"".join(out)}</nav>'


def _shell(title: str, body: str, active: str = "", q: str = "", stamp: str = "", back: str = "",
           old_css: bool = False, refresh: str = "js", heading: Optional[str] = None) -> str:
    """A whole page in the shell. ``refresh``: "js" (every 10 s, never while a date is half typed; KEEP_OPEN_JS
    too), "meta" (a plain 10-second refresh) or "" (none). ``heading``: the title bar's words (the page's
    title by default); ``back``: a ready-made link shown before them."""
    e = html.escape
    css = (CSS if old_css else "") + HOME_CSS
    meta = ('<noscript><meta http-equiv="refresh" content="10"></noscript>' if refresh == "js" else
            '<meta http-equiv="refresh" content="10">' if refresh == "meta" else "")
    head_txt = e(title) if heading is None else heading
    head_h = f'<h1>{head_txt}</h1>' if head_txt else '<span class="grow"></span>'
    stamp_html = f'<span class="stamp">{e(stamp)}</span>' if stamp else ""
    scripts = ("<script>(function(){var b=document.getElementById('theme-btn');b.addEventListener('click',toggleTheme);"
               "if(document.documentElement.getAttribute('data-theme')==='dark')b.textContent='Light';})();</script>"
               + ((KEEP_OPEN_JS + REFRESH_JS) if refresh == "js" else ""))
    return (f'<!doctype html><html lang="en-GB"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">{meta}<title>{e(title)}</title>'
            f'{THEME_JS}<style>{css}</style></head><body><div class="app">'
            f'<aside class="side"><div class="side-top"><a class="brand" href="{_with_q("/", q)}"><i>Trading</i> '
            f'bots</a><small>Demo account</small></div>{_nav_html(active, q)}'
            f'<div class="side-foot"><span>Refreshes every 10 s</span>'
            f'<button class="linkbtn" id="theme-btn" type="button">Dark</button></div></aside>'
            f'<div class="main"><div class="topbar">{back}{head_h}{stamp_html}</div>'
            f'<div class="content">{body}</div></div></div>{scripts}</body></html>')


def _gbp_html(v, cur: str = "GBP", cls: bool = True, signed: bool = True) -> str:
    """A money figure, escaped, coloured green or red only when ``cls``."""
    if v is None:
        return "-"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "-"
    txt = _fmt_money(f, cur) if signed else _fmt_money(abs(f), cur).lstrip("+")
    c = _cls(f) if cls else ""
    c = {"ok": "pos", "bad": "neg"}.get(c, "")
    return f'<span class="{c}">{html.escape(txt)}</span>' if c else html.escape(txt)


def _home_query(view: Optional[dict], bot: str = "all") -> str:
    """?period=... (and its dates), and the chosen bot - for every link that keeps them."""
    q = _keep(view or {"key": "today"}, _bot_choice(bot), "overall")
    return f"?{q}" if q else ""


def render_home_periods(view: Optional[dict], bot: str = "all", show: str = "") -> str:
    """The period buttons, the same eight choices as the full page (Today ...
    Total, and Custom from/to), as the hub's segmented control. The chosen bot
    (or the chosen filter, ``show``) stays chosen. The custom form is a "dates"
    form, so the 10-second refresh never wipes a date half typed."""
    from .standing import PERIOD_BUTTONS
    bot = _bot_choice(bot)
    show = show if show in SHOWS else ""
    v = view or {}
    key = v.get("key") or "today"
    keep = (f"&bot={html.escape(bot)}" if bot != "all" else "") + (f"&show={show}" if show else "")
    btns = "".join(f'<a class="s-p{" on" if k == key else ""}" href="/?period={k}{keep}">{html.escape(lbl)}</a>'
                   for k, lbl in PERIOD_BUTTONS)
    f_val = html.escape(str(v.get("from") or ""))
    t_val = html.escape(str(v.get("to") or ""))
    hidden = (f'<input type="hidden" name="bot" value="{html.escape(bot)}">' if bot != "all" else "") + (
        f'<input type="hidden" name="show" value="{show}">' if show else "")
    custom = (f'<form class="dates" method="get" action="/"><input type="hidden" name="period" value="custom">{hidden}'
              f'<input type="date" name="from" value="{f_val}" aria-label="From">'
              f'<span>to</span><input type="date" name="to" value="{t_val}" aria-label="To">'
              f'<button type="submit"{" class=on" if key == "custom" else ""}>Custom</button></form>')
    return (f'<div class="periodbar s-periods" title="{html.escape(UK_DAYS_NOTE)}"><div class="seg">{btns}</div>'
            f'{custom}</div>')


def _kpi(label: str, value: str, foot: str = "", key: str = "") -> str:
    """One KPI card: its label, its figure (already escaped html), one short grey line."""
    e = html.escape
    k = f' data-kpi="{e(key)}"' if key else ""
    return (f'<div class="card kpi"{k}><div class="label">{e(label)}</div><div class="value">{value}</div>'
            + (f'<div class="foot">{e(foot)}</div>' if foot else "") + '</div>')


def _home_account(snap: dict, top: dict, cur: str) -> str:
    """The account as the hub's KPI cards: the balance, the chosen period, Overall
    since the reset and today's trades - MetaTrader's own figures
    (``standing.period_view``), never worked out another way. Practice never
    enters them."""
    e = html.escape
    sd = snap.get("standing") or {}
    sel, tot = top.get("sel"), top.get("tot")
    if not sel or not tot or tot.get("made") is None:
        why = str(sd.get("error") or "the page has not heard from MetaTrader yet")
        return (f'<div class="kpis three s-acct">{_kpi("Balance", "-", "", "balance")}'
                f'{_kpi("Overall", "-", "", "overall")}{_kpi("Trades today", "-", "", "trades")}</div>'
                f'<p class="kpi-note">MetaTrader\'s records could not be read just now ({e(why)}) - no figure rather '
                f'than a wrong one.</p>')
    total = sel.get("key") == "total"
    label = str(sel.get("label") or "Today")

    def closed(v: dict) -> str:
        n, w = v.get("trades"), v.get("wins")
        return "no trades closed" if not n else f"{n} closed, {w} won"
    cards = [_kpi("Balance", _gbp_html(sd.get("balance"), cur, cls=False, signed=False), f"{cur} demo account",
                  "balance")]
    if not total:
        cards.append(_kpi(label, _gbp_html(sel.get("made"), cur), closed(sel), "period"))
    began = _uk(sd.get("start") or "", "%d %b")
    since = f"since the reset on {began}" if began else "since the account started"
    cards.append(_kpi("Total (Overall)" if total else "Overall", _gbp_html(tot.get("made"), cur),
                      since + (f" · {closed(sel)}" if total else ""), "overall"))
    today = top.get("today") if isinstance(top.get("today"), dict) else (sel if sel.get("key") == "today" else None)
    n_today = (today or {}).get("trades")
    open_live = snap.get("open_live") or []
    foot = ""
    if open_live:
        opened = round(sum(float(r.get("profit") or 0.0) for r in open_live), 2)
        foot = f"{len(open_live)} open now, {_fmt_money(opened, cur)} not banked"
    elif today is not None:
        foot = closed(today)
    cards.append(_kpi("Trades today", e("-" if n_today is None else str(n_today)), foot, "trades"))
    return f'<div class="kpis{" three" if total else ""} s-acct">{"".join(cards)}</div>'


def formula1_line(v: Optional[dict], focus: Optional[dict] = None) -> str:
    """Formula 1's LIVE progress in one plain line, from Trend & Breakout's
    verdict (``verdicts.formula1_live``: MetaTrader's deal records only -
    never its journal, never practice): trades closed since it went live,
    won, money after all costs, days up and down, and how many of the
    plan's ``judge_after`` (20) it has. Says plainly when it cannot count."""
    from .verdicts import FORMULA1_JUDGE_AFTER
    v = v or {}
    try:
        judge = int((focus or {}).get("judge_after") or FORMULA1_JUDGE_AFTER)
    except (TypeError, ValueError):
        judge = FORMULA1_JUDGE_AFTER
    f1 = v.get("formula1")
    if v.get("error"):
        return "its records could not be read just now - no live figure is shown rather than a wrong one."
    if not isinstance(f1, dict):
        if str(v.get("mode") or "").upper() == "LIVE":
            return ("Trend & Breakout is fully LIVE on this computer, so Formula 1's trades are not counted apart "
                    "from the rest.")
        return ("not live on this computer yet - Trend & Breakout's minor currency pairs are still on PAPER here, so "
                "there are no live trades to count.")
    if not f1.get("readable", True):
        return "MetaTrader's records could not be read just now - no live figure is shown rather than a wrong one."
    since = f1.get("since")
    if since is None:
        return "its live start time is not recorded, so its live trades are not counted yet."
    when = _uk(since.isoformat(), "%d %b %H:%M") + " UK"
    s = f1.get("stats") or {}
    n, k = int(s.get("trades") or 0), int(f1.get("open") or 0)
    still = (f" {k} open now, not counted until {'it closes' if k == 1 else 'they close'}." if k else "")
    if not n:
        return f"no live trades yet (live since {when}) - 0 of {judge} toward the judgement.{still}"
    up, down = int(s.get("up_days") or 0), int(s.get("down_days") or 0)
    toward = (f"{n} of {judge} toward the judgement" if n < judge else
              f"its {judge} live trades are in: time to judge it")
    return (f"{n} trade{'' if n == 1 else 's'} closed since {when}, {s.get('wins', 0)} won, "
            f"{_fmt_money(float(s.get('net') or 0.0), 'GBP')} after all costs "
            f"({_fmt_money(float(s.get('costs') or 0.0), 'GBP').lstrip('+')} of commission); "
            f"{up} day{'' if up == 1 else 's'} up, {down} down - {toward}. From MetaTrader's records.{still}")


def _plan_html(plan: Optional[dict], plan_error: str, verdicts: dict) -> str:
    """The plan: the aim, the focus, the next three steps, BIN / CHANGE / KEEP
    (one short line each; the reason folds open under it), any bot whose
    record now disagrees with it, and - folded - where its figures come
    from. Open folds stay open through the 10-second refresh (each has an id)."""
    from .verdicts import next_steps, plan_disagreements, plan_items
    e = html.escape
    if not plan:
        return (f'<section class="s-card"><h2>The plan</h2><p class="s-small">No plan to show: {e(plan_error or "none")}.'
                f'</p></section>')
    out = [f'<section class="s-card"><h2>The plan</h2><p class="s-aim">{e(str(plan.get("aim") or ""))}</p>']
    f = plan.get("focus") if isinstance(plan.get("focus"), dict) else None
    if f:
        # 9 Oct evening: Formula 1 is LIVE - its progress is its live trades from MetaTrader's records only
        # (verdicts.formula1_live), never the journal and never practice
        fwd = f'<div class="s-small"><b>Live so far:</b> {e(formula1_line(verdicts.get(str(f.get("bot") or "")), f))}</div>'
        # 10 Oct review: what config.json now holds that the plan's words do not (Aaron's answers, by config)
        force = str((verdicts.get(str(f.get("bot") or "")) or {}).get("in_force") or "")
        if force:
            fwd += f'<div class="s-small"><b>In force now (config.json):</b> {e(force)}.</div>'
        out.append(f'<div class="s-focus"><div class="s-k">The focus now</div><b>{e(str(f.get("name") or ""))}</b>: '
                   f'{e(str(f.get("what") or ""))}<div class="s-small">{e(str(f.get("evidence") or ""))} '
                   f'{e(str(f.get("caution") or ""))}</div>{fwd}</div>')
    steps = next_steps(plan, 3)
    if steps:
        out.append('<div class="s-k">The next steps</div><ol class="s-steps">'
                   + "".join(f"<li>{e(x)}</li>" for x in steps) + "</ol>")
    cols = []
    for bucket, title in (("bin", "Bin"), ("change", "Change"), ("keep", "Keep")):
        lis = []
        for k, i in enumerate(plan_items(plan, bucket)):
            txt = f'<b>{e(str(i.get("text") or ""))}</b>'
            if i.get("why"):          # the reason folds open under the item, so the box stays one short line each
                lis.append(f'<li><details id="plan-{bucket}-{k}"><summary>{txt}</summary>'
                           f'<span class="s-why">{e(str(i.get("why")))}</span></details></li>')
            else:
                lis.append(f"<li>{txt}</li>")
        cols.append(f'<div><h3>{title}</h3><ul>{"".join(lis) or "<li>nothing</li>"}</ul></div>')
    out.append(f'<div class="s-cols">{"".join(cols)}</div>')
    for d in plan_disagreements(plan, verdicts):
        out.append(f'<div class="s-flag"><b>Check:</b> {e(d["text"])}</div>')
    upd = str(plan.get("updated") or "")
    try:
        upd = dt.date.fromisoformat(upd[:10]).strftime("%d %b %Y")
    except ValueError:
        pass
    sources = [str(x) for x in plan.get("sources") or [] if x]
    out.append(f'<details class="s-more" id="plan-sources"><summary>Where the plan\'s figures come from</summary>'
               f'<p class="s-src">The plan is docs/plan.json, updated {e(upd or "-")}. '
               f'{e(str(plan.get("written_by") or ""))}'
               + (f' Its figures come from: {e("; ".join(sources))}.' if sources else "") + '</p></details></section>')
    return "".join(out)


def _real_practice_note(b: dict, v: dict, top: dict, view: dict, cur: str) -> str:
    """On a bot's own page: which of its money is real and which is practice -
    Trend & Breakout part LIVE (Formula 1), a switched-off bot whose real money
    still counts, a PAPER bot's practice and any real money left from its LIVE
    days. The same words the main page's rows carried until 12 Oct."""
    e = html.escape
    bid, mode = b.get("id"), str(b.get("mode") or v.get("mode") or "")
    mixed = bool(v.get("live_groups")) and mode != "LIVE"
    off = mode == "OFF"
    live = mode == "LIVE" or mixed or off
    sel, tot = top.get("sel") or {}, top.get("tot") or {}
    total = (view or {}).get("key") == "total"
    plabel = str((view or {}).get("label") or "Today")
    vs = (sel.get("bots") or {}).get(bid) or {}
    vt = (tot.get("bots") or {}).get(bid) or {}
    if mixed:
        p_sel, p_tot = top.get("p_sel") or {}, top.get("p_tot") or {}
        pn = int(((v.get("period") or {}).get("practice") or {}).get("trades") or 0)
        bits = []
        if not total and p_sel.get(bid) is not None:
            bits.append(f"{plabel.lower()} {_fmt_money(float(p_sel[bid]), cur)} ({pn} trade{'' if pn == 1 else 's'})")
        if p_tot.get(bid) is not None:
            bits.append(f"overall {_fmt_money(float(p_tot[bid]), cur)}")
        return (f'<p class="s-extra">Its money on the main page is real (MetaTrader\'s): its '
                f'{e(str(v.get("live_kinds") or "chosen markets"))} trade LIVE. The rest is practice (PAPER), not real '
                f'money' + (f": {e(', '.join(bits))}." if bits else ".") + '</p>')
    if off:
        why = f"; binned: {v.get('why')}" if v.get("code") == "NO EDGE" and v.get("why") else ""
        return (f'<p class="s-extra">Switched off - it places no trades{e(why)}. Its real money from its LIVE days '
                f'still counts in the account\'s figures.</p>')
    if not live:
        real_t = vt.get("made")
        if real_t is not None and abs(float(real_t)) >= 0.005:
            real_p = vs.get("made") if not total else None
            n_p = int(vs.get("trades") or 0) if not total else 0
            return ('<p class="s-extra">Practice only, not real money. Real money from its LIVE days: '
                    + (f"{e(plabel.lower())} {e(_fmt_money(float(real_p), cur))}"
                       f" ({n_p} trade{'' if n_p == 1 else 's'}), "
                       if real_p is not None and (abs(float(real_p)) >= 0.005 or n_p) else "")
                    + f"overall {e(_fmt_money(float(real_t), cur))}.</p>")
        return '<p class="s-extra">Practice only (PAPER), not real money.</p>'
    return ""


# ------------------------------------------------ the lab's showcase (12 Oct) --
# Aaron, 10 Oct 2026: "Make sure everything is presented in the bot showcase really simply for me and not over the
# top" and "each day the winners stand out and we watch them closely like scouts". The words and the rules are
# mintel/ops/showcase.py's; these only draw them, every string escaped. The page reads data/lab/summary.json and
# data/lab-status.json and nothing else of the lab's.
def _checks_html(checks, synthetic: bool = False) -> str:
    """The five check pills, in order and wording (Enough trades, Steady, Fresh
    prices, Costs, Luck check), each with its state word and, in its title, the
    sentence. A made-up summary keeps the words and shows TEST DATA in place of
    the sentence and the counts."""
    from . import showcase as S
    e = html.escape
    pills = []
    for c in S.ordered_checks(checks):
        title = S.TEST_DATA if synthetic else S.pounds(c.get("text") or "")
        pills.append(f'<span class="s-ck {S.CHECK_CLASS.get(c["state"], "ny")}" title="{e(title)}">'
                     f'{e(c["label"])}: {e(S.check_word(c, synthetic))}</span>')
    return f'<div class="s-checks">{"".join(pills)}</div>'


def _lab_href(cid) -> str:
    """/lab?id=<the id>, quoted and escaped."""
    from urllib.parse import quote
    return html.escape(f"/lab?id={quote(str(cid), safe='')}")


def _lab_chip(c: dict) -> str:
    chip = c.get("chip") if isinstance(c.get("chip"), dict) else {}
    colour = str(chip.get("colour") or "grey")
    from .showcase import CHIP_CLASSES
    colour = colour if colour in CHIP_CLASSES else "grey"
    code = str(chip.get("code") or "")
    return f'<span class="s-chip {colour}">{html.escape(code)}</span>' if code else ""


def _lab_sims(c: dict) -> str:
    """A candidate's computer tests, as 'On old prices (computer test): +£312.00 over
    143 trades' - simulated, never called money made."""
    from . import showcase as S
    e = html.escape
    tests = c.get("tests") if isinstance(c.get("tests"), dict) else {}
    out = []
    for kind, lead in (("past", "On old prices (computer test)"), ("fresh", "Once on prices it had never seen")):
        t = tests.get(kind) if isinstance(tests.get(kind), dict) else {}
        n, net = S._int(t.get("trades")), S._num(t.get("net_gbp"))
        if n is not None and net is not None:
            out.append(f'<div class="s-sim">{e(lead)}: {e(S.money(net))} over {e(S._plural(n, "trade"))}</div>')
    return "".join(out)


def _cand_row(r: dict) -> str:
    """One lab candidate on the ladder, in the same grid as a bot's row: its name,
    chip and stage, its headline, What to do, its practice figures in grey (or
    its computer tests, or "Not yet" when it has no record), and its five checks.
    The whole row opens its page (/lab?id=...)."""
    from . import showcase as S
    e = html.escape
    c = r.get("candidate") or {}
    syn = bool(r.get("synthetic"))
    stage = str(r.get("stage") or "")
    head = S.TEST_DATA if syn else S.pounds(c.get("headline") or "")
    f = c.get("figures") if isinstance(c.get("figures"), dict) else {}
    n = S._int(f.get("trades"))

    def fig(k, val):
        return f'<div class=s-prac><div class="s-k">{e(k)}</div><div class="s-m">{_gbp_html(val, "GBP")}</div></div>'
    if syn:
        nums = f'<div class="s-sim">{e(S.TEST_DATA)}</div>'
    elif stage in (S.PRACTICE, S.READY_FOR_LIVE, S.LIVE_TRIAL, S.PROVEN):
        # the lab's money is always practice (it has no live path): grey and labelled, whatever the stage
        if not n:
            nums = '<div class=s-prac><div class="s-k">Practice</div><div class="s-m0">Not yet</div></div>'
        else:
            w = S._int(f.get("wins")) or 0
            nums = (fig("This week (practice)", f.get("week_gbp")) + fig("Overall (practice)", f.get("overall_gbp"))
                    + f'<div class=s-prac><div class="s-k">Trades</div><div class="s-m0">'
                    f'{e(S._plural(n, "practice trade"))}, {round(100.0 * w / n):.0f}% won</div></div>')
    else:
        nums = _lab_sims(c) or '<div class="s-sim">Not yet</div>'
    return (f'<a class="s-bot" href="{_lab_href(r.get("id"))}"><div>'
            f'<span class="s-cname">{e(str(r.get("name")))}</span>{_lab_chip(c)}'
            f'<span class="s-stage">{e(stage)}</span><div class="s-hl">{e(head)}</div>'
            f'<div class="s-rec"><b>What to do:</b> {e(str(r.get("what_to_do") or ""))}</div></div>'
            f'<div class="s-nums">{nums}</div>{_checks_html(r.get("checks"), syn)}</a>')


def _changes_html(moved: list) -> str:
    """What changed since yesterday (``showcase.changes``: at most 5 lines) - on
    /coach since 12 Oct, off the main page."""
    from . import showcase as S
    e = html.escape
    if moved:
        lines = "".join(f'<li>{e(S.uk_time(m.get("when"), "%a %d %b %H:%M") + " UK: " if m.get("when") else "")}'
                        f'{e(m["text"])}</li>' for m in moved)
        inner = f'<ul class="s-changes">{lines}</ul>'
    else:
        inner = f'<p class="s-none">{e(S.NOTHING_MOVED)}</p>'
    return f'<section class="s-card" id="changed"><h2>What changed since yesterday</h2>{inner}</section>'


# ------------------------------------------------- the leaderboard on "/" (12 Oct) --
# Aaron, 12 Oct 2026: "The ui is confusing, it should be combined bots but still talks about and breaks down only the
# old ones, we need to clean it up, its just a load of writing". One list: the bots and the lab's ideas treated the
# same, ONE line each - a short name, LIVE / PRACTICE / TESTING / OFF, the period and Overall in £ (practice in grey;
# computer-test money never), trades, one verdict word. Everything else is on each one's own page.
LB_LIVE, LB_PRACTICE, LB_TESTING, LB_OFF, LB_UNKNOWN = "LIVE", "PRACTICE", "TESTING", "OFF", "-"
_BOT_WORDS = {"PROVEN": ("Winning", "green"), "PROMISING": ("Promising", "green"), "BREAK-EVEN": ("Flat", "amber"),
              "NO EDGE": ("Losing", "red"), "WAITING": ("Waiting", "grey"), "NOT ENOUGH DATA": ("Too early", "grey")}


def _bot_entry(b: dict, v: dict, top: dict, view: dict, info: dict) -> dict:
    """A current bot on the leaderboard: its real money (LIVE, part LIVE or
    switched off - period_view's figures) or its practice (PAPER -
    practice_figures, grey), its trades in the period and one verdict word."""
    bid, label = b.get("id"), str(b.get("label") or v.get("label") or "")
    mode = str(b.get("mode") or v.get("mode") or "")
    mixed = bool(v.get("live_groups")) and mode != "LIVE"     # Trend & Breakout's minor pairs LIVE (Formula 1)
    off = mode == "OFF"
    live = mode == "LIVE" or mixed or off
    sel, tot = top.get("sel") or {}, top.get("tot") or {}
    if live:
        vs = (sel.get("bots") or {}).get(bid) or {}
        vt = (tot.get("bots") or {}).get(bid) or {}
        period, overall, trades = vs.get("made"), vt.get("made"), vs.get("trades")
    else:
        period, overall = (top.get("p_sel") or {}).get(bid), (top.get("p_tot") or {}).get(bid)
        trades = ((v.get("period") or {}).get("practice") or {}).get("trades")
    status = (LB_LIVE if (mode == "LIVE" or mixed) else LB_OFF if off else
              LB_PRACTICE if mode == "PAPER" else LB_UNKNOWN)
    if off:
        word, dot = "Off", "grey"
    else:
        word, dot = _BOT_WORDS.get(str(v.get("code") or ""), ("No verdict", "grey"))
    st_title = (str(v.get("mode_words") or "") if mixed else
                {LB_LIVE: "Real money", LB_OFF: "Switched off - it places no trades",
                 LB_PRACTICE: "Practice (PAPER) - not real money"}.get(status, "Mode not known yet"))
    return {"kind": "bot", "id": bid, "name": label, "full": label, "status": status, "status_title": st_title,
            "practice": not live, "period": period, "overall": overall, "trades": trades, "word": word, "dot": dot,
            "why": str(v.get("headline") or ""), "synthetic": False, "info": info}


def _cand_entry(r: dict, short: str, key: str, written, now: dt.datetime) -> dict:
    """A lab idea on the leaderboard: its PRACTICE money only (the lab has no
    live path), grey; an idea still being tested shows no money (its computer
    tests are never a result); TEST DATA for a made-up summary."""
    from . import showcase as S
    c = r.get("candidate") or {}
    syn = bool(r.get("synthetic"))
    stage = str(r.get("stage") or "")
    if stage in (S.PRACTICE, S.READY_FOR_LIVE, S.LIVE_TRIAL, S.PROVEN):
        status = LB_PRACTICE
    elif stage == S.BINNED:
        status = LB_OFF
    else:
        status = LB_TESTING
    period = overall = trades = None
    if not syn and status != LB_TESTING:
        period, trades = S.lab_period_figures(c, key, written, now)
        f = c.get("figures") if isinstance(c.get("figures"), dict) else {}
        overall = S._num(f.get("overall_gbp"))
    checks = r.get("checks") or []
    if syn:
        word, dot = "Test data", "grey"
    elif status == LB_OFF:
        word, dot = "Binned", "grey"
    elif status == LB_TESTING:
        word, dot = "Waiting", "grey"
    elif stage == S.READY_FOR_LIVE:
        word, dot = "Your call", "blue"
    elif any(k.get("state") == S.FAILED for k in checks):
        word, dot = "Failing", "red"
    elif checks and all(k.get("state") == S.PASSED for k in checks):
        word, dot = "Passed", "green"
    else:
        word, dot = "Too early", "grey"
    st_title = {LB_PRACTICE: "Practice - live prices, no money", LB_OFF: "Binned - out, not retried",
                LB_TESTING: "Tested on old prices only - not practising yet, no money"}[status]
    why = S.TEST_DATA if syn else S.pounds(c.get("headline") or "")
    return {"kind": "candidate", "id": r.get("id"), "name": short, "full": str(r.get("name") or ""),
            "status": status,
            "status_title": f"{st_title} - {stage.lower()}" if stage not in ("", S.PRACTICE, S.BINNED) else st_title,
            "practice": True, "period": period, "overall": overall, "trades": trades, "word": word, "dot": dot,
            "why": why, "synthetic": syn, "info": r}


def _lb_sort_key(x: dict) -> tuple:
    """LIVE first, then by Overall (highest first); switched off and binned last; no figure after a figure."""
    rank = 0 if x["status"] == LB_LIVE else 2 if x["status"] == LB_OFF else 1
    o = x.get("overall")
    try:
        o = None if o is None else float(o)
    except (TypeError, ValueError):
        o = None
    return (rank, o is None, -(o or 0.0), str(x.get("name") or ""), str(x.get("id") or ""))


def _lb_row(x: dict, view: dict, cur: str, star: str = "") -> str:
    """One tile: its name (a star for a standout), a coloured status pill, the
    period and Overall in £ (practice grey), its trades, one coloured word. No
    sentences - the word's headline is in its title. The whole tile opens the
    bot's page (/?bot=...) or the idea's (/lab?id=...)."""
    e = html.escape
    total = (view or {}).get("key") == "total"
    plabel = str((view or {}).get("label") or "Today")
    prac = x["practice"]
    tag = " (practice)" if prac else ""
    href = f"/{_home_query(view, x['id'])}" if x["kind"] == "bot" else _lab_href(x["id"])
    star_html = (f'<span class="bt-star" title="{e("Standout: " + star)}" aria-label="standout">&#9733;</span>'
                 if star else "")
    st = x["status"].lower() if x["status"] != LB_UNKNOWN else "unk"
    head = (f'<div class="bt-head"><span class="bt-name">{e(x["name"])}</span>{star_html}'
            f'<span class="bt-st {st}" title="{e(x["status_title"])}">{e(x["status"])}</span></div>')
    if x["synthetic"]:
        figs = f'<div class="bt-test">{e(_test_data_words())}</div>'
    else:
        pc = " prac" if prac else ""
        cells = []
        if not total:
            cells.append(f'<div class="bt-m bt-p{pc}" title="{e(plabel + tag)}" data-k="{e(plabel)}">'
                         f'{_gbp_html(x["period"], cur)}</div>')
        cells.append(f'<div class="bt-m bt-o{pc}" title="{e("Overall" + tag)}" data-k="Overall">'
                     f'{_gbp_html(x["overall"], cur)}</div>')
        n = x.get("trades")
        cells.append(f'<div class="bt-t{pc}" title="{e("Trades " + _in_period(view or {}) + tag)}" data-k="Trades">'
                     f'{e("-" if n is None else str(n))}</div>')
        figs = f'<div class="bt-figs{" two" if total else ""}">{"".join(cells)}</div>'
    word = (f'<div class="bt-v {e(x["dot"])}" title="{e(x["why"])}"><i class="dot {e(x["dot"])}"></i>'
            f'{e(x["word"])}</div>')
    title = x["full"] if x["full"] != x["name"] else ""
    return (f'<a class="bt{" prac" if prac else ""}" href="{href}"' + (f' title="{e(title)}"' if title else "")
            + f'>{head}{figs}{word}</a>')


def _test_data_words() -> str:
    from .showcase import TEST_DATA
    return TEST_DATA


SHOWS = {"live": "Real money", "practice": "Practice"}


def _shown(entries: list, show: str) -> list:
    """The tiles a page shows: every one on Home; Real money - the bots whose money is real (LIVE, part LIVE,
    or switched off with real money from their LIVE days); Practice - every PRACTICE tile, bots and ideas."""
    if show == "live":
        return [x for x in entries if x["kind"] == "bot" and not x["practice"]]
    if show == "practice":
        return [x for x in entries if x["status"] == LB_PRACTICE]
    if show == "lab":
        return [x for x in entries if x["kind"] == "candidate"]
    return entries


def _leaderboard_html(entries: list, stars: dict, view: dict, cur: str, title: str = "Bots") -> str:
    """ONE card of tiles: the bots and the lab's ideas together, LIVE first then by Overall."""
    tiles = "".join(_lb_row(x, view, cur, stars.get((x["kind"], str(x["id"])), "")) for x in entries)
    legend = " · ".join(w for w, on in (
        ("Grey = practice, not real money", any(x["practice"] for x in entries)),
        ("&#9733; standout", any(stars.get((x["kind"], str(x["id"]))) for x in entries))) if on)
    return (f'<section class="card" id="bots"><div class="card-head"><h2>{html.escape(title)}</h2>'
            + (f'<span class="sub">{legend}</span>' if legend else "") + '</div>'
            f'<div class="bt-grid">{tiles or "<p class=bt-none>Nothing here yet.</p>"}</div></section>')


def _next_lab_run(now: dt.datetime) -> str:
    """When the lab's first nightly run is due: 23:10 UK, Monday to Saturday (mintel/lab/nightly.py)."""
    uk = to_utc(now).astimezone(TZ_LONDON)
    before = (uk.hour, uk.minute) < (23, 10)
    if uk.hour < 6 and (uk.date() - dt.timedelta(days=1)).weekday() < 6:
        return "first test due by 06:00"                     # last night's run (23:10 to 06:00) is due by now
    if uk.weekday() < 6 and before:
        return "first test tonight 23:10"
    if uk.weekday() < 6 and uk.hour >= 23:
        return "first test due tonight from 23:10"
    nxt = uk.date() + dt.timedelta(days=1)
    while nxt.weekday() == 6:
        nxt += dt.timedelta(days=1)
    return f"first test {nxt.strftime('%A')} 23:10"


def _lab_ideas_registered() -> Optional[int]:
    """The ideas the lab's first night will test: its registered grid (mintel.lab.families), never a guess."""
    try:
        from ..lab.families import all_families
        return sum(int(f.GRID_SIZE) for f in all_families())
    except Exception as exc:                                 # never a reason to fail the page
        log.warning("lab grid: %s", exc)
        return None


def _lab_card(lab: dict, now: dt.datetime, link: bool = True) -> str:
    """The lab, small: before it has run, how many ideas wait and when the first test is; while it collects
    prices, how many days of the 40; then its funnel's numbers. TEST DATA for a made-up run; one grey line."""
    from . import showcase as S
    e = html.escape
    state = lab.get("state")
    sm = lab.get("summary") or {}
    syn = bool(lab.get("synthetic") or sm.get("synthetic"))
    grey = ""
    if state == S.LAB_NONE:
        n = _lab_ideas_registered()
        inner = (f'<div class="lab-big">{e(S._count(n)) if n is not None else "-"}<small>ideas waiting</small></div>'
                 f'<p class="grey-line">{e(_next_lab_run(now))}</p>')
    elif state == S.LAB_BROKEN:
        inner = f'<div class="lab-one">{e(str(lab.get("line") or ""))}</div>'
    elif state == S.LAB_COLLECTING:
        st = sm.get("store") if isinstance(sm.get("store"), dict) else {}
        have, need = S._int(st.get("complete_days")), S._int(st.get("needed_days")) or S.NEEDED_DAYS
        if syn:
            inner = f'<div class="lab-one">Collecting prices: {e(S.TEST_DATA)}</div>'
        else:
            pct = max(0, min(100, round(100.0 * (have or 0) / need))) if need else 0
            inner = (f'<div class="lab-one">Collecting prices: {e(S._count(have))} of {e(S._count(need))} days</div>'
                     f'<div class="prog"><span style="width:{pct}%"></span></div>')
    else:
        f = sm.get("funnel") if isinstance(sm.get("funnel"), dict) else {}
        if syn:
            inner = f'<div class="lab-one">{e(S.TEST_DATA)}</div>'
        elif not f:
            inner = '<div class="lab-one">Nothing tried yet</div>'
        else:
            tried = S._int(f.get("past_tested"))
            tried = tried if tried is not None else S._int(f.get("ideas"))
            cells = [("Tested", tried), ("Passed", f.get("past_passed")), ("Practising", f.get("practice"))]
            for key, words in (("ready", "Ready"), ("live", "Live trial"), ("proven", "Proven")):
                if (S._int(f.get(key)) or 0) > 0:
                    cells.append((words, f.get(key)))
            cells.append(("Binned", f.get("binned")))
            inner = ('<div class="funnel">' + "".join(f'<div><b>{e(S._count(v))}</b><span>{e(k)}</span></div>'
                                                      for k, v in cells) + '</div>')
        if S.run_missed(sm.get("nightly"), now):
            grey = "Last night's run missed"
    tail = (f'<p class="grey-line">{e(grey)}</p>' if grey else "")
    if not link:
        return f'<section class="card lab-card" id="lab"><div class="mini-title">In numbers</div>{inner}{tail}</section>'
    return (f'<a class="card lab-card" href="/lab" id="lab"><div class="mini-title">The lab<span class="go">'
            f'Open &rarr;</span></div>{inner}{tail}</a>')


def _coach_card(line: str) -> str:
    """The coach in its one line (coach_page.one_line), under its own title."""
    words = line[len("Coach: "):] if line.startswith("Coach: ") else line
    words = words[:1].upper() + words[1:]
    return (f'<a class="card coach-card" href="/coach" id="coach"><div class="mini-title">The coach<span class="go">'
            f'Open &rarr;</span></div><div class="lab-one">{html.escape(words)}</div></a>')


def _home_problem(snap: dict) -> str:
    """One line at the top, only when the trader itself says something is wrong
    (not running, waiting for MetaTrader, safe mode); the detail is on /details."""
    st = snap.get("status") or {}
    health = snap.get("health") or {}
    state = str(st.get("bot") or "")
    if state == "RUNNING" and not health.get("safe_mode"):
        return ""
    if state == "WAITING FOR METATRADER":
        what = f"waiting for MetaTrader ({st.get('waiting') or 'not connected yet'})"
    elif state in ("", "STARTING"):
        what = "starting - it has not reported yet"
    else:
        what = str(health.get("summary") or state.lower() or "a problem")
    return (f'<div class="flag s-flag"><span class="light"></span><span><b>The trader:</b> {html.escape(what)} - '
            f'<a href="/details">see the technical detail</a>.</span></div>')


def _stamp(updated) -> str:
    return _uk(str(updated or ""), "%a %d %b, %H:%M UK") if updated else ""


def _home_page(title: str, body: str, old_css: bool = False, active: str = "", q: str = "", stamp: str = "",
               back: str = "", heading: Optional[str] = None) -> str:
    return _shell(title, body, active=active, q=q, stamp=stamp, back=back, old_css=old_css, heading=heading)


def _ladder_infos(sd: dict, top: dict, verdicts: dict, deals, plan: Optional[dict], now: dt.datetime,
                  data_dir: str, only: Optional[str] = None) -> list:
    """[(bot, verdict, its ladder info)] for every active bot - its stage and
    five checks (``showcase.bot_ladder``), never a reason to fail the page."""
    from . import showcase as S
    from .standing import ACTIVE_BOTS
    focus = (plan or {}).get("focus") if isinstance((plan or {}).get("focus"), dict) else {}
    try:
        from .verdicts import FORMULA1_JUDGE_AFTER
        judge = int(focus.get("judge_after") or FORMULA1_JUDGE_AFTER)
    except (TypeError, ValueError):
        judge = 20
    by_id = {b.get("id"): b for b in sd.get("bots") or () if isinstance(b, dict)}
    out = []
    for bid, label in ACTIVE_BOTS:
        if only is not None and bid != only:
            continue
        v = verdicts.get(bid) or {"label": label, "code": "", "headline": ""}
        b = by_id.get(bid) or {"id": bid, "label": label, "mode": v.get("mode") or ""}
        try:
            info = S.bot_ladder(bid, v, sd=sd, top=top, data_dir=data_dir, now=now, deals=deals, judge_after=judge)
        except Exception as exc:                             # never a reason to fail the page
            log.warning("ladder of %s: %s", bid, exc)
            info = {"kind": "bot", "id": bid, "name": label, "stage": S.bot_stage(v), "checks": [],
                    "what_to_do": "Nothing until it can be worked out - it comes back on its own.",
                    "verdict": str(v.get("code") or ""), "trades": None, "synthetic": False}
        out.append((b, v, info))
    return out


def _tiles_of(snap: dict, top: dict, verdicts: dict, plan: Optional[dict], deals, lab: dict, view: dict,
              now: dt.datetime, data_dir: str) -> tuple[list, dict]:
    """(every tile's entry, sorted; the standouts' stars) - the bots and the lab's ideas together."""
    from . import showcase as S
    sd = snap.get("standing") or {}
    entries, infos = [], []
    for b, v, info in _ladder_infos(sd, top, verdicts, deals, plan, now, data_dir):
        infos.append(info)
        try:
            entries.append(_bot_entry(b, v, top, view, info))
        except Exception as exc:                             # never a reason to fail the page
            log.warning("row of %s: %s", b.get("id"), exc)
            entries.append({"kind": "bot", "id": b.get("id"), "name": str(b.get("label") or b.get("id")),
                            "full": str(b.get("label") or ""), "status": LB_UNKNOWN,
                            "status_title": "could not be shown just now", "practice": False, "period": None,
                            "overall": None, "trades": None, "word": "No verdict", "dot": "grey",
                            "why": f"It could not be shown just now ({exc}).", "synthetic": False, "info": info})
    cands = S.ladder_candidates(lab, now)
    shorts = S.short_names([r.get("candidate") or {"id": r.get("id"), "name": r.get("name")} for r in cands])
    written = S._when((lab.get("summary") or {}).get("written_utc"))
    for r in cands:
        infos.append(r)
        try:
            entries.append(_cand_entry(r, shorts.get(str(r.get("id"))) or str(r.get("name")),
                                       str(view.get("key") or "today"), written, now))
        except Exception as exc:                             # never a reason to fail the page
            log.warning("lab row of %s: %s", r.get("id"), exc)
    try:
        stars = {("bot" if p["row"].get("kind") == "bot" else "candidate", str(p["row"].get("id"))): p["why"]
                 for p in S.standouts(infos, now)}
    except Exception as exc:                                 # never a reason to fail the page
        log.warning("standouts: %s", exc)
        stars = {}
    entries.sort(key=_lb_sort_key)
    return entries, stars


WK_CHIPS = {"MAKING MONEY": "making", "LOSING": "losing", "TOO EARLY": "", "READY": "ready"}


def _pct(v) -> str:
    return "-" if v is None else f"{float(v):.0f}%"


def _wk_chip(word: str) -> str:
    return f'<span class="wk-c {WK_CHIPS.get(word, "")}">{html.escape(word)}</span>'


def _wk_href(x: dict) -> str:
    """A row's own detail page: a bot's (or Trend & Breakout's) /?bot=..., a lab idea's /lab?id=...; '' when unknown."""
    kind, ref = str(x.get("kind") or ""), str(x.get("ref") or "")
    if kind in ("bot", "tnb") and ref in BOT_IDS:
        return html.escape(f"/?bot={ref}")
    if kind == "lab" and ref:
        return _lab_href(ref)
    return ""


def _wk_row(x: dict, cur: str, link: bool = False) -> str:
    """One test, one light row (the .wk-row grid, never a table): its name ("early" under 10 trades), £ after costs
    over 28 days, trades and win %, and one chip - READY, MAKING MONEY, LOSING or TOO EARLY. ``link``: the name opens
    its own page."""
    from . import weekly as W
    e = html.escape
    n = int(x.get("trades") or 0)
    name = e(str(x.get("name") or ""))
    href = _wk_href(x) if link else ""
    if href:
        name = f'<a href="{href}">{name}</a>'
    chip = _wk_chip("READY") if x.get("ready") else _wk_chip(str(x.get("chip") or W.TOO_EARLY))
    return (f'<div class="wk-row" data-test="{e(str(x.get("id")))}"><div class="wk-name">{name}'
            f'{" <span class=wk-early>early</span>" if x.get("early") else ""}</div>'
            f'<div class="wk-money">{_gbp_html(x.get("net"), cur)}</div>'
            f'<div class="wk-f">{n} trade{"" if n == 1 else "s"} · {_pct(x.get("win_rate"))} won</div>'
            f'<div class="wk-chip">{chip}</div></div>')


def _wk_counts(b: dict, cur: str) -> str:
    """The pool's total and counts: every test together, then how many run, make money, lose, wait for a trade and are
    ready for real money."""
    e = html.escape
    c = b.get("counts") or {}
    total = b.get("total") or {}
    return (f'<div class="wk-t prac wk-total" data-count="total"><div class="wk-n">All tests together</div>'
            f'<div class="wk-v">{_gbp_html(total.get("net") or 0.0, cur) if total.get("trades") else "-"}</div>'
            f'<div class="wk-f">{int(total.get("trades") or 0)} trades · {_pct(total.get("win_rate"))} won</div></div>'
            + "".join(f'<div class="wk-t prac" data-count="{k}"><div class="wk-v">{int(c.get(k) or 0)}</div>'
                      f'<div class="wk-n">{e(w)}</div></div>'
                      for k, w in (("running", "tests running"), ("making", "making money"), ("losing", "losing"),
                                   ("waiting", "waiting for a trade"), ("ready", "ready for real money"))))


def _wk_tests(b: dict) -> int:
    """How many tests in all (those waiting for a trade included)."""
    got = b.get("tests")
    if got is None:
        got = len(b.get("all") or b.get("top") or []) + len(b.get("waiting") or [])
    return int(got or 0)


def render_weekly_home(board: Optional[dict], cur: str = "GBP") -> str:
    """Home's headline numbers (Aaron, 13 Oct: "headline numbers on home ... not
    a load of writing"; "they just become part of all of the other tests"):
    every test as ONE pool. On real money: a tile per test the weekly pick
    put there - real £ today, since it went live, open now, N of 20 live
    trades (MetaTrader's figures). Testing: every test together, how many run,
    make money, lose, wait for their first trade, are ready for real money;
    then, with ~640 tests (every lab idea practises since 13 Oct), only the ten
    best and the three worst by practice £ after costs over 28 days, and "See all
    N tests" for the full breakdown (the Practice page). A test with no trade
    yet is counted, not listed. This week's pick. Numbers first, almost no
    words; renders with no weekly data at all."""
    from . import weekly as W
    e = html.escape
    b = board or {}
    live = b.get("live") or []
    pick = b.get("pick") or W.pick_tile(None, utcnow())
    names = [str(n) for n in (pick.get("names") or [pick.get("name") or ""])]    # every pick, one a line
    if live:
        tiles = "".join(
            f'<div class="wk-t" data-test="{e(str(x.get("id")))}"><div class="wk-n">{e(str(x.get("name") or ""))}</div>'
            f'<div class="wk-nums"><div data-k="today">{_gbp_html(x.get("today"), cur)}</div>'
            f'<div data-k="since live">{_gbp_html(x.get("since"), cur)}</div>'
            f'<div data-k="open now">{_gbp_html(x.get("open") or 0.0, cur)}</div>'
            f'<div data-k="live trades">{int(x.get("trades") or 0)} of {int(x.get("of") or W.JUDGE_AFTER)}</div>'
            f'</div>{_wk_chip(str(x.get("chip") or W.TOO_EARLY))}</div>' for x in live)
    else:
        tiles = (f'<div class="wk-t prac" data-test="none"><div class="wk-v" style="font-size:17px;white-space:normal">Nothing on real '
                 f'money &mdash; all testing</div><div class="wk-f">{e(str(pick.get("when") or ""))}</div></div>')
    if b.get("top") is not None and (b.get("top") or b.get("bottom")):
        top, bottom = list(b.get("top") or []), list(b.get("bottom") or [])
    else:                                                     # an older board: its full list, cut the same way
        rows = [x for x in (b.get("all") or []) if int(x.get("trades") or 0)]
        cut = W.HOME_TOP + W.HOME_BOTTOM
        top, bottom = (rows, []) if len(rows) <= cut else (rows[:W.HOME_TOP], rows[-W.HOME_BOTTOM:])
    lines = "".join(_wk_row(x, cur) for x in top)
    if bottom:
        lines += '<div class="wk-gap">&middot;&middot;&middot; the three at the bottom</div>' + "".join(
            _wk_row(x, cur) for x in bottom)
    table = (f'<div class="wk-list">{lines}</div>' if lines
             else '<div class="wk-f">No practice trades in the last 28 days yet.</div>')
    n = _wk_tests(b)
    more = f'<a class="wk-more" href="/?show=practice">See all {n} tests &rarr;</a>' if n else ""
    pick_html = (f'<div class="wk-t{" prac" if pick.get("none") else ""}" data-test="pick">'
                 f'<div class="wk-n">This week\'s pick{"s" if len(names) > 1 else ""}</div>'
                 f'<div class="wk-v" style="font-size:17px;white-space:normal">{"<br>".join(e(n) for n in names)}</div>'
                 f'<div class="wk-f">{e(str(pick.get("when") or ""))}</div></div>')
    return (f'<section class="card" id="weekly"><div class="wk-h">On real money</div><div class="wk-grid">{tiles}'
            f'{pick_html}</div><div class="wk-h">Testing &middot; last 28 days, practice money</div>'
            f'<div class="wk-grid counts">{_wk_counts(b, cur)}</div>{table}{more}</section>')


def render_practice_breakdown(board: Optional[dict], cur: str = "GBP") -> str:
    """The Practice page's breakdown (Aaron, 13 Oct: "a breakdown of everything
    (profit) and winners at the top"): every test together at the top, then
    every test with a trade, one light row each, best practice £ after costs
    over 28 days first - each name opens its own page - and the tests with no
    trade yet as ONE folded line at the bottom. No table: the .wk-row grid
    reads on a phone."""
    from . import weekly as W
    e = html.escape
    b = board or {}
    rows = [x for x in (b.get("all") or []) if int(x.get("trades") or 0)]
    waiting = list(b.get("waiting") or [])
    lines = "".join(_wk_row(x, cur, link=True) for x in rows)
    table = (f'<div class="wk-list">{lines}</div>' if lines
             else '<div class="wk-f">No practice trades in the last 28 days yet.</div>')
    wait = ""
    if waiting:
        k = len(waiting)
        names = " · ".join(e(str(x.get("name") or "")) for x in waiting)
        wait = (f'<details class="wk-wait" data-count="waiting"><summary>{k} test{"" if k == 1 else "s"} waiting for '
                f'their first trade</summary><div class="wk-f">{names}</div></details>')
    return (f'<section class="card" id="breakdown"><div class="wk-h">Every test &middot; last {W.LOOKBACK_DAYS} days, '
            f'practice money after costs, winners at the top</div>'
            f'<div class="wk-grid counts">{_wk_counts(b, cur)}</div>{table}{wait}</section>')


def _weekly_board(snap: dict, top: dict, data_dir: str, now: dt.datetime, deals) -> Optional[dict]:
    """``weekly.home_board`` from the page's own refresh; None (rendered as empty) when it fails."""
    try:
        from . import weekly as W
        sd = snap.get("standing") or {}
        return W.home_board(data_dir, now=now, deals=deals if sd.get("start") else None,
                            open_tickets=sd.get("open_tickets"), open_live=snap.get("open_live") or [])
    except Exception as exc:                                 # never a reason to fail the page
        log.warning("weekly board: %s", exc)
        return None


def render_home(snap: dict, top: Optional[dict], verdicts: dict, plan: Optional[dict] = None,
                plan_error: str = "", deals=None, lab: Optional[dict] = None, show: str = "") -> str:
    """The main page, in the AI hub's layout (Aaron, 12 Oct: "Put the layout
    similar to the ai hub ... there's too much writing still"): the period
    buttons; the account as four KPI cards (balance, the chosen period,
    Overall since the reset, trades today - ``standing.period_view``); ONE
    card "Bots" of tiles - every bot and every lab idea, LIVE first then by
    Overall, each with its status, its period and Overall £ (practice grey),
    its trades and one coloured word; then the lab and the coach, small.
    ``show``: "live" (Real money) or "practice" - the same tiles filtered, no
    KPI cards. What happened, what to do, the five checks and the sources are
    on each bot's page (/?bot=...) and each idea's (/lab?id=...).

    The lab never enters the account; the lab's computer tests are never
    shown as money here; a made-up lab summary shows TEST DATA. ``deals``:
    the account's deal rows (Formula 1's trades for its checks); ``lab``:
    ``showcase.load_lab`` (read from the data folder when not given).
    Renders whatever is missing as missing; everything is escaped.

    13 Oct (Aaron: "they just become part of all of the other tests"): under
    the account, the weekly pick's numbers as one pool (``render_weekly_home``:
    on real money, testing, this week's pick); the tiles bot by bot are
    folded under "Every test, one by one", one tap away."""
    from . import showcase as S
    top = top or {}
    show = show if show in SHOWS else ""
    sd = snap.get("standing") or {}
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")
    view = top.get("sel") or top.get("period") or {"key": "today"}
    now = to_utc(top["now"]) if isinstance(top.get("now"), dt.datetime) else utcnow()
    data_dir = str(top.get("data_dir") or "")
    if lab is None:
        lab = S.load_lab(data_dir, now)
    entries, stars = _tiles_of(snap, top, verdicts, plan, deals, lab, view, now, data_dir)
    title = SHOWS.get(show, "Home")
    body = _home_problem(snap) + render_home_periods(view, "all", show)
    if show == "practice":
        # 13 Oct: the full breakdown first, every test winners at the top; each one's own page stays one tap away,
        # by its name in the breakdown or its tile folded underneath
        board = _weekly_board(snap, top, data_dir, now, deals)
        body += (render_practice_breakdown(board, cur)
                 + '<details class="wk-all"><summary>Each test\'s own page</summary>'
                 + _leaderboard_html(_shown(entries, show), stars, view, cur, title) + '</details>')
    elif show:
        body += _leaderboard_html(_shown(entries, show), stars, view, cur, title)
    else:
        coach = _coach_card(coach_page.one_line(data_dir, now))
        # 13 Oct: the pool's numbers first; the bot-by-bot tiles folded away, one tap from Home
        board = _weekly_board(snap, top, data_dir, now, deals)
        body += (_home_account(snap, top, cur) + render_weekly_home(board, cur)
                 + '<details class="wk-all"><summary>Every test, one by one</summary>'
                 + _leaderboard_html(entries, stars, view, cur) + '</details>'
                 + f'<div class="g2">{_lab_card(lab, now)}{coach}</div>')
    return _home_page("Trading bots" if not show else title, body, active=show or "home", q=_period_q(view),
                      stamp=_stamp(snap.get("updated")), heading=title)


def render_bot_detail(snap: dict, top: Optional[dict], bot: str, verdict: dict, plan: Optional[dict] = None,
                      verdicts: Optional[dict] = None, deals=None) -> str:
    """One bot: its verdict, What happened in the chosen period, the
    Recommendation, its place on the ladder (stage, which of its money is real
    and which practice, why it stands out today, its five checks - moved here
    from the main page on 12 Oct) and the evidence (each with its source and
    dates), then its existing detail - its card with the "Now:" line, its
    trades in the period, its open trades and its own live view
    (``render_bot_view``). A LIVE bot also gets the quiet banner when the live
    bots have gone quiet."""
    from .verdicts import plan_disagreements
    e = html.escape
    top = top or {}
    sd = snap.get("standing") or {}
    view = top.get("sel") or top.get("period") or {"key": "today"}
    cur = str(sd.get("currency") or (snap.get("status") or {}).get("currency") or "GBP")
    now = to_utc(top["now"]) if isinstance(top.get("now"), dt.datetime) else utcnow()
    v = verdict or {}
    label = str(v.get("label") or bot)
    mode = str(v.get("mode") or "")
    mixed = bool(v.get("live_groups")) and mode != "LIVE"     # 9 Oct: Trend & Breakout's minor pairs LIVE
    live = mode == "LIVE" or mixed
    back = f'<a class="back" href="/{_home_query(view, "all")}">&larr; All bots</a>'
    chip = f'<span class="s-chip {e(str(v.get("colour") or "grey"))}">{e(str(v.get("code") or ""))}</span>'
    pill = f'<span class="s-mode{" live" if live else ""}">{e((v.get("mode_words") if mixed else mode) or "-")}</span>'
    flags = "".join(f'<div class="s-flag"><b>Check:</b> {e(d["text"])}</div>'
                    for d in plan_disagreements(plan, verdicts or {bot: v}) if d.get("bot") == bot)
    ev = "".join(f'<li><b>{e(str(x.get("label") or ""))}</b><div>{e(str(x.get("facts") or ""))}</div>'
                 f'<div class="s-src" style="margin-top:2px">Source: {e(str(x.get("source") or "-"))}'
                 + (f"; dates: {e(str(x.get('period')))}" if x.get("period") else "") + '</div></li>'
                 for x in v.get("evidence") or [])
    plabel = str(view.get("label") or "Today")
    # its place on the ladder and its five checks (on "/" until 12 Oct)
    ladder = ""
    try:
        b = next((x for x in sd.get("bots") or () if isinstance(x, dict) and x.get("id") == bot),
                 {"id": bot, "label": label, "mode": mode})
        info = next((i for _b, _v, i in _ladder_infos(sd, top, {bot: v}, deals, plan, now,
                                                      str(top.get("data_dir") or ""), only=bot)), None)
        if info is not None:
            ladder = ('<h2>On the ladder</h2><p>'
                      + (f'<span class="s-stage">{e(str(info.get("stage")))}</span>' if info.get("stage")
                         else "Its mode is not known yet.") + '</p>' + _real_practice_note(b, v, top, view, cur)
                      + '<h2>The five checks</h2>' + _checks_html(info.get("checks")))
    except Exception as exc:                                 # never a reason to fail the page
        log.warning("ladder of %s: %s", bot, exc)
    box = (f'<section class="s-card s-v"><h1>{e(label)}{pill}{chip}</h1>'
           f'<p class="s-hl" style="font-size:16px">{e(str(v.get("headline") or ""))}</p>'
           f'<div class="s-k">Verdict: {e(str(v.get("code") or "-"))}'
           + (f' - {e(str(v.get("action")))}' if v.get("action") and v.get("action") != v.get("code") else "")
           + '</div>'
           f'<h2>What happened - {e(plabel)}</h2><p>{e(str(v.get("what_happened") or "Nothing to say yet."))}</p>'
           f'<h2>Recommendation</h2><p>{e(str(v.get("recommendation") or "-"))}</p>{flags}{ladder}'
           f'<h2>The evidence</h2><ul class="s-ev">{ev or "<li>No trades on record yet.</li>"}</ul>'
           f'<p class="s-src">The rule: {e(str(v.get("rule") or ""))}</p></section>')
    quiet = ""
    if live:
        try:
            quiet = render_quiet_banner(snap, top, bot_now_lines(snap, top))
        except Exception as exc:                             # never a reason to fail the page
            log.warning("quiet banner: %s", exc)
    try:
        old = render_bot_view(snap, top, bot)
    except Exception as exc:
        log.warning("bot view of %s: %s", bot, exc)
        old = f'<div class="card small">Its detail could not be shown just now ({e(str(exc))}).</div>'
    if not top.get("sel") or not top.get("tot"):
        why = e(str(sd.get("error") or "the page has not heard from MetaTrader yet"))
        old = (f'<div class="card small bad">MetaTrader\'s records could not be read just now ({why}); its real trades '
               f'are not listed rather than listed wrong. They come back on their own.</div>' + old)
    body = (_home_problem(snap) + render_home_periods(view, bot) + box
            + f'<div class="legacy"><div class="s-old"><h2>Its detail</h2>{quiet}{old}</div></div>')
    real = live or mode == "OFF"
    return _home_page(label, body, old_css=True, active="live" if real else "practice", q=_period_q(view),
                      stamp=_stamp(snap.get("updated")), back=back, heading="")


def _lab_back(back: str = "/", back_words: str = "All bots") -> str:
    """The title bar's way back (the shell's)."""
    return f'<a class="back" href="{back}">&larr; {html.escape(back_words)}</a>'


def _now_stamp(now: dt.datetime) -> str:
    return _uk(now.isoformat(), "%a %d %b, %H:%M UK")


def _lab_test_banner(lab: dict) -> str:
    from . import showcase as S
    if not lab.get("synthetic"):
        return ""
    return (f'<p class="s-test"><b>{html.escape(S.TEST_DATA)}.</b> This summary was built from made-up prices to '
            f'check that the lab works; nothing on this page is a result.</p>')


def render_lab_board(data_dir, now: Optional[dt.datetime] = None, lab: Optional[dict] = None) -> str:
    """/lab - the lab in full, read-only from data/lab/summary.json and
    data/lab-status.json: the prices it holds, the stretch it keeps back, the
    funnel, every family (ideas, passes, whether it looks to be fitting
    noise), the clock-trade control's pass rate, the one honest line about how
    many ideas were tried, the ideas on the ladder (the bench and the binned
    folded), and last night's run. Tables only here, inside .s-old."""
    from . import showcase as S
    e = html.escape
    now = to_utc(now) if now is not None else utcnow()
    lab = lab or S.load_lab(data_dir, now)
    state = lab.get("state")
    sm = lab.get("summary") or {}
    syn = bool(lab.get("synthetic"))
    parts = [_lab_test_banner(lab)]

    def page(parts: list) -> str:
        return _home_page("The lab", "".join(parts), active="lab", stamp=_now_stamp(now))
    if state in (S.LAB_NONE, S.LAB_BROKEN):
        parts.append(f'<div class="g2 top">{_lab_card(lab, now, link=False)}'
                     f'<section class="s-card"><p class="s-aim">{e(str(lab.get("line")))}</p>'
                     f'<p class="s-small">{e(S.practice_now_line(lab, now))}</p></section></div>')
        return page(parts)
    line = str(lab.get("line") or "") if state != S.LAB_READY else S.funnel_line(sm, now, prices=False)
    if syn:     # made-up prices: no research figure is shown, only that it is test data
        where = [line, S.coverage_line(sm), f"Ideas tried and fresh looks: {S.TEST_DATA}.", S.SMALL_EDGE,
                 S.practice_now_line(lab, now)]
    else:
        where = [line, S.coverage_line(sm), S.fresh_line(sm), S.honest_line(sm), S.SMALL_EDGE,
                 S.practice_now_line(lab, now)]
    f = sm.get("funnel") if isinstance(sm.get("funnel"), dict) else {}
    if S._int(f.get("waiting_for_calendar")):
        where.append(f"{S._count(f.get('waiting_for_calendar'))} ideas wait for enough news history to test.")
    parts.append(f'<div class="g2 top">{_lab_card(lab, now, link=False)}<section class="s-card"><h2>Where it stands</h2>'
                 + "".join(f'<p class="s-small">{e(x)}</p>' for x in where if x) + '</section></div>')
    if state == S.LAB_READY:
        parts.append(_lab_tiles(lab, now))
    if state == S.LAB_READY:
        rows = [S.candidate_ladder(c, syn) for c in S.candidates_of(lab)]
        live = [r for r in rows if r["stage"] in (S.PROVEN, S.LIVE_TRIAL, S.READY_FOR_LIVE, S.PRACTICE)]
        bench = [r for r in rows if r["stage"] == S.ON_THE_BENCH]
        binned = [r for r in rows if r["stage"] == S.BINNED]
        ladder = []
        for name, rs in S.grouped(live):
            if rs:
                ladder.append(f'<h3 class="s-group">{e(name)}</h3><div class="s-rows">'
                              + "".join(_cand_row(r) for r in rs) + '</div>')
        ladder.append(f'<details class="s-more" id="lab-bench"><summary>On the bench ({len(bench)})</summary>'
                      f'<div class="s-rows">{"".join(_cand_row(r) for r in bench) or "<p class=s-none>None.</p>"}</div>'
                      f'</details>')
        seen = {r["id"] for r in binned}
        extra = [b for b in sm.get("binned_recent") or () if isinstance(b, dict) and str(b.get("id")) not in seen]
        lis = "".join(
            f'<li><a href="{_lab_href(r["id"])}">{e(r["name"])}</a> - '
            f'{e(S.TEST_DATA if r["synthetic"] else S.pounds((r.get("candidate") or {}).get("headline") or "binned"))}'
            f'</li>' for r in binned)
        lis += "".join(f'<li><a href="{_lab_href(b.get("id"))}">{e(str(b.get("name") or b.get("id")))}</a> - '
                       f'binned {e(S.day_words(b.get("when")))}: '
                       f'{e(S.TEST_DATA if syn else S.pounds(b.get("reason") or ""))}</li>' for b in extra)
        ladder.append(f'<details class="s-more" id="lab-binned"><summary>Binned lately ({len(binned) + len(extra)})'
                      f'</summary><ul class="s-ev">{lis or "<li>None.</li>"}</ul></details>')
        parts.append(f'<section class="s-card"><h2>Its ideas on the ladder</h2>{"".join(ladder)}'
                     f'<p class="s-src">{e(S.STAGE_NOTE)}</p></section>')
        fams = [x for x in sm.get("families") or () if isinstance(x, dict)]

        def fam_cells(x: dict) -> tuple:
            """(passed, PBO, fitting noise?) - TEST DATA for made-up prices; a family whose grid is
            not finished on this stretch is not graded yet (its pass count and PBO come at the end)."""
            if syn:
                return S.TEST_DATA, S.TEST_DATA, S.TEST_DATA
            if not x.get("complete"):
                tried = f"not graded yet ({S._count(x.get('tested'))} of {S._count(x.get('n'))} tried)"
                return tried, "-", "-"
            return (S._count(x.get("past_passed")),
                    "n/a" if S._num(x.get("pbo")) is None else format(float(x.get("pbo")), ".2f"),
                    "Yes - nothing forwarded" if x.get("fits_noise") else "No")

        trs = "".join(
            f'<tr><td>{e(str(x.get("label") or x.get("code") or ""))}</td><td>{e(S._count(x.get("n")))}</td>'
            + "".join(f'<td>{e(c)}</td>' for c in fam_cells(x))
            + f'<td>{e(S.family_note(x))}</td></tr>' for x in fams)
        parts.append('<section class="s-card"><h2>The families of ideas</h2><div class="s-old"><table class="s-lt">'
                     '<tr><th>Family</th><th>Ideas</th><th>Passed the past test</th><th>Chance it is fitting noise '
                     '(PBO; over 0.35 holds it back)</th><th>Fitting noise?</th><th>Note</th></tr>'
                     f'{trs or "<tr><td colspan=6>None yet.</td></tr>"}</table></div>'
                     f'<p class="s-small">{e(S.TEST_DATA if syn else S.control_line(sm))}</p></section>')
    nightly = sm.get("nightly") if isinstance(sm.get("nightly"), dict) else {}
    if nightly:
        kv = [("Run of", S.day_words(nightly.get("date"), True)),
              ("Started", S.uk_time(S._when(nightly.get("started")), "%a %d %b %H:%M UK")),
              ("Finished", S.uk_time(S._when(nightly.get("finished")), "%a %d %b %H:%M UK") or "not yet"),
              ("Last step", str(nightly.get("step") or "-")),
              ("Ideas tested", S.TEST_DATA if syn else S._count(nightly.get("tested"))),
              ("Fresh looks", S.TEST_DATA if syn else S._count(nightly.get("fresh"))),
              ("Took", S.TEST_DATA if syn else (S.took_words(nightly.get("seconds")) or "-")),
              ("Seconds an idea", S.TEST_DATA if syn else S._count(nightly.get("seconds_per_candidate"))),
              ("Ran out of time", "yes" if nightly.get("deadline_hit") else "no"),
              ("Note", str(nightly.get("note") or "-"))]
        tail = nightly.get("log_tail") if isinstance(nightly.get("log_tail"), list) else []
        missed = S.run_missed(nightly, now)
        parts.append('<section class="s-card"><h2>Last night\'s run</h2>'
                     + ('<p class="s-flag" style="margin-top:0"><b>Last night\'s run: missed.</b> The next run catches '
                        'up.</p>' if missed else "")
                     + '<div class="s-old"><table class="s-lt">'
                     + "".join(f'<tr><th>{e(k)}</th><td>{e(v or "-")}</td></tr>' for k, v in kv)
                     + (f'<tr><th>Its log</th><td><pre>{e(chr(10).join(str(x) for x in tail[-20:]))}</pre></td></tr>'
                        if tail and not syn else "")
                     + '</table></div></section>')
    return page(parts)


def _lab_tiles(lab: dict, now: dt.datetime) -> str:
    """/lab's tiles: the lab's ideas on "/" (the same tiles, filtered), their practice in the UK day."""
    from . import showcase as S
    view = {"key": "today", "label": "Today"}
    cands = S.ladder_candidates(lab, now)
    shorts = S.short_names([r.get("candidate") or {"id": r.get("id"), "name": r.get("name")} for r in cands])
    written = S._when((lab.get("summary") or {}).get("written_utc"))
    entries = []
    for r in cands:
        try:
            entries.append(_cand_entry(r, shorts.get(str(r.get("id"))) or str(r.get("name")), "today", written, now))
        except Exception as exc:                             # never a reason to fail the page
            log.warning("lab tile of %s: %s", r.get("id"), exc)
    entries.sort(key=_lb_sort_key)
    return _leaderboard_html(entries, {}, view, "GBP", "Ideas")


def render_candidate(data_dir, cid: str, now: Optional[dt.datetime] = None, lab: Optional[dict] = None) -> str:
    """/lab?id=<id> - one idea: its name, stage and chip, its headline, What
    happened, What to do, "Is it luck? The five checks" (each sentence with its
    numbers and its line; a cross says why), its rules in plain words, its
    story, its practice trades and the source of every figure. An id the
    summary does not hold: "No such idea." """
    from . import showcase as S
    e = html.escape
    now = to_utc(now) if now is not None else utcnow()
    lab = lab or S.load_lab(data_dir, now)
    back = _lab_back("/lab", "The lab")

    def page(title: str, body: str) -> str:
        return _home_page(title, body, active="lab", stamp=_now_stamp(now), back=back, heading="")
    c = S.find_candidate(lab, str(cid or "")) if lab.get("state") in (S.LAB_READY, S.LAB_COLLECTING) else None
    if c is None:
        why = "" if lab.get("state") in (S.LAB_READY, S.LAB_COLLECTING) else f' {e(str(lab.get("line") or ""))}'
        return page("The lab", f'<section class="s-card"><p class="s-aim">{e(S.NO_SUCH_IDEA)}</p>'
                               f'<p class="s-small">{why.strip()}</p></section>')
    syn = bool(lab.get("synthetic"))
    r = S.candidate_ladder(c, syn)
    name = str(r["name"])
    headline = S.TEST_DATA if syn else S.pounds(c.get("headline") or "")
    marks = {"Passed": "&#10003;", "Failed": "&#10007;", "Running": "&hellip;"}
    checks = "".join(
        f'<li><b>{marks.get(k["state"], "&ndash;")} {e(k["label"])}: {e(S.check_word(k, syn))}</b>'
        f'<div>{e(S.TEST_DATA if syn else S.pounds(k.get("text") or "Not worked out yet."))}</div></li>'
        for k in r["checks"])
    words = "".join(f"<li>{e(str(w))}</li>" for w in (c.get("words") if isinstance(c.get("words"), list) else [])
                    if w)
    story = "".join(
        f'<li><b>{e(S.day_words(s.get("when"), True) or "-")}: {e(str(s.get("from") or "-"))} &rarr; '
        f'{e(str(s.get("to") or "-"))}</b><div>{e(S.TEST_DATA if syn else S.pounds(s.get("why") or ""))}</div></li>'
        for s in (c.get("story") if isinstance(c.get("story"), list) else []) if isinstance(s, dict))
    rows = [t for t in (c.get("practice_trades") if isinstance(c.get("practice_trades"), list) else [])
            if isinstance(t, dict)]

    def side(v) -> str:
        return {1: "buy", -1: "sell"}.get(S._int(v), str(v or ""))
    rows.sort(key=lambda t: str(t.get("closed_utc") or t.get("closed") or ""), reverse=True)
    st = lab.get("status") if isinstance(lab.get("status"), dict) else {}
    open_now = [p for p in (st.get("open_positions") if isinstance(st.get("open_positions"), list) else [])
                if isinstance(p, dict) and str(p.get("candidate_id") or p.get("id") or "") == str(c.get("id"))]
    if syn:
        trades = f'<p>{e(S.TEST_DATA)}</p>'
    elif rows:
        trades = ('<div class="s-old"><table class="s-lt"><tr><th>Closed (UK)</th><th>Market</th><th>Side</th>'
                  '<th>After costs (practice)</th><th>Exit</th></tr>'
                  + "".join(f'<tr><td>{e(S.uk_time(S._when(t.get("closed_utc") or t.get("closed")), "%a %d %b %H:%M"))}'
                            f'</td><td>{e(str(t.get("symbol") or ""))}</td>'
                            f'<td>{e(side(t.get("side")))}</td>'
                            f'<td>{_gbp_html(t.get("net_gbp"), "GBP")}</td><td>{e(str(t.get("exit_reason") or ""))}'
                            f'</td></tr>' for t in rows[:50])
                  + '</table></div>')
    elif S._int((c.get("figures") or {}).get("trades")):
        trades = (f'<p>Its {e(S._plural(S._int(c["figures"]["trades"]), "practice trade"))} are in the lab\'s own '
                  f'record (data/lab/lab.sqlite, practice_trades); this summary does not list them.</p>')
    else:
        trades = "<p>No practice trades on record yet.</p>"
    if open_now and not syn:
        trades += "<p>Open now (practice): " + "; ".join(
            f'{e(str(p.get("symbol") or ""))} {e(side(p.get("side")))} since '
            f'{e(S.uk_time(S._when(p.get("opened_utc")), "%H:%M UK"))}' for p in open_now) + "</p>"
    sources = [str(x) for x in (c.get("sources") if isinstance(c.get("sources"), list) else []) if x]
    written = S.uk_time(S._when((lab.get("summary") or {}).get("written_utc")), "%a %d %b %H:%M UK")
    sources.append(f"data/lab/summary.json, written {written or '-'}")
    body = (_lab_test_banner(lab)
            + f'<section class="s-card s-v"><h1>{e(name)}<span class="s-stage">{e(r["stage"] or "-")}</span>'
              f'{_lab_chip(c)}</h1><p class="s-hl" style="font-size:16px">{e(headline)}</p>'
            + "<h2>What happened</h2>" + "".join(f"<p>{e(x)}</p>" for x in S.what_happened(c, syn))
            + f'<h2>What to do</h2><p>{e(r["what_to_do"])}</p>'
            + f'<h2>Is it luck? The five checks</h2><ul class="s-ev">{checks}</ul>'
            + f'<h2>Its rules in plain words</h2><ul>{words or "<li>Not given.</li>"}</ul>'
            + f'<h2>Its story</h2><ul class="s-ev">{story or "<li>Nothing on record yet.</li>"}</ul>'
            + f'<h2>Its trades</h2>{trades}'
            + '<h2>Sources</h2><ul class="s-ev">' + "".join(f"<li>{e(x)}</li>" for x in sources) + "</ul></section>")
    return page(name, body)


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
    """The Rider's and Financial Ian's own pages, in the same shell."""
    return _shell(title, f'<div class="legacy">{body}<p class="small">This page only reads the bot\'s own files; '
                         f'it changes nothing. It refreshes itself every 10 seconds.</p></div>',
                  active="live", old_css=True, refresh="meta" if refresh else "", heading="")


def _own_file(data, name: str) -> dict:
    raw, _why = _read_status_file(data / name)
    return raw or {}


# ------------------------------------------- each bot's trades, MetaTrader's way --
OWN_RECORDS = {  # bot: (its own sqlite, its money column, its entry column, its exit column)
    "market_intelligence": ("journal.sqlite", "pnl_money", "entry", "exit_price"),
    "momentum_rider": ("rider.sqlite", "net_money", "entry", "exit_price"),
    "momentum_runner": ("runner.sqlite", "net_pnl", "entry", "exit_price"),
    "band_breaker": ("bandbreaker.sqlite", "net_pnl", "entry", "exit_price"),
    "crowd_fader": ("crowd.sqlite", "net_pnl", "entry", "exit_price"),
    "financial_ian": ("ian.sqlite", "net_pnl", "entry", "exit_price"),
    "rapid_scalper": ("scalper.sqlite", "net_pnl", "entry_filled", "exit_filled"),
}


def _own_db(data: Path, bot: str) -> Optional[Path]:
    """The bot's own sqlite (the Rider's may be renamed in rider.json)."""
    rec = OWN_RECORDS.get(bot)
    if rec is None:
        return None
    name = rec[0]
    if bot == "momentum_rider":
        jf = _own_file(data, "rider.json").get("journal_file")
        if isinstance(jf, str) and jf.strip():
            name = jf
    return data / name


def _when(v) -> Optional[dt.datetime]:
    try:
        t = v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return to_utc(t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc))
    except Exception:
        return None


def bot_trade_rows(data_dir, bot: str, view: dict, magic: int) -> list[dict]:
    """The bot's trades that closed in a ``period_view``'s period (UK days),
    newest first. LIVE: MetaTrader's positions opened with its magic, each
    once and in full (``view["closed"]``), with the side, prices and exit
    reason from its own record of the same ticket when it has one. Practice:
    its own PAPER rows that closed in the period (Trend & Breakout's from
    its paper record), as the card's practice figure counts them - never
    money. Read-only; a missing file is simply no rows."""
    from .attribution import _rows_ro, is_test_data, row_mode
    from .standing import TnbPaperRecord, bot_view
    start, end = _when(view.get("start")), _when(view.get("end"))
    _db, money_col, entry_col, exit_col = OWN_RECORDS.get(bot, (None, "net_pnl", "entry", "exit_price"))
    data = Path(data_dir) if data_dir else None
    path = _own_db(data, bot) if data is not None else None
    live = bot_view(view, magic)["closed"] if magic else []

    def by_ticket(p: Optional[Path], tickets) -> dict:
        out: dict = {}
        tickets = sorted({int(t) for t in tickets if t})
        if p is None or not tickets or not p.exists():
            return out
        for i in range(0, len(tickets), 400):
            chunk = tickets[i:i + 400]
            for r in _rows_ro(p, f"SELECT * FROM trades WHERE ticket IN ({','.join('?' * len(chunk))})", tuple(chunk)):
                t = int(r.get("ticket") or 0)
                # a PAPER shadow can share a real ticket (the Momentum Runner's): the LIVE row is this trade's
                if t not in out or (bot != "market_intelligence" and row_mode(r) == "LIVE"):
                    out[t] = r
        return out

    def symbol_of(r: dict) -> str:
        return str(r.get("symbol") or r.get("spot_symbol") or r.get("instrument") or "")
    rows: list[dict] = []
    own = by_ticket(path, [p.get("position") for p in live])
    for p in live:
        r = own.get(int(p.get("position") or 0)) or {}
        rows.append({"closed": p.get("closed"), "symbol": p.get("symbol") or symbol_of(r), "side": r.get("side"),
                     "volume": p.get("volume") or r.get("volume"), "entry": r.get(entry_col), "exit": r.get(exit_col),
                     "net": p.get("net"), "reason": r.get("exit_reason"), "mode": "LIVE", "ticket": p.get("position"),
                     "pips": r.get("realised_pips"), "in_records": bool(r)})
    if start is not None and end is not None and data is not None:
        if bot == "market_intelligence":
            record = TnbPaperRecord(data)
            paper = [] if record.error else record.positions(start, end)
            jr = by_ticket(data / "journal.sqlite", [p["ticket"] for p in paper])
            for p in paper:
                r = jr.get(int(p["ticket"])) or {}
                rows.append({"closed": p.get("closed"), "symbol": p.get("symbol") or symbol_of(r), "side": r.get("side"),
                             "volume": p.get("volume") or r.get("volume"), "entry": r.get("entry"),
                             "exit": r.get("exit_price"), "net": p.get("net"), "reason": r.get("exit_reason"),
                             "mode": "PAPER", "ticket": p.get("ticket"), "pips": r.get("pnl_pips"), "in_records": True})
        elif path is not None and path.exists():
            for r in _rows_ro(path, "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND "
                                    "closed_utc < ? ORDER BY closed_utc ASC LIMIT 5000",
                              (start.isoformat(), end.isoformat())):
                if row_mode(r) == "LIVE":
                    continue                                # LIVE money is MetaTrader's, above
                if bot == "momentum_rider" and str(r.get("exit_reason") or "").startswith("SWITCHED"):
                    continue                                # closed in its records at a switch: never a trade
                if bot == "financial_ian" and is_test_data(r):
                    continue                                # a synthetic or replayed feed: a test, not a result
                rows.append({"closed": _when(r.get("closed_utc")), "symbol": symbol_of(r), "side": r.get("side"),
                             "volume": r.get("volume"), "entry": r.get(entry_col), "exit": r.get(exit_col),
                             "net": r.get(money_col), "reason": r.get("exit_reason"), "mode": row_mode(r),
                             "ticket": r.get("ticket"), "pips": r.get("realised_pips"), "in_records": True})
    rows.sort(key=lambda x: x.get("closed") or dt.datetime.min.replace(tzinfo=dt.timezone.utc), reverse=True)
    return rows


def bot_today(sd: Optional[dict], deals, bot: str, data_dir, now: Optional[dt.datetime] = None) -> Optional[dict]:
    """A bot's TODAY for its own page, from the same position records and UK
    day as its card on the status page: {live, practice, magic, mode,
    figure}. None when the page has no figures from MetaTrader yet, or its
    magic's rows could not be read (never a false "no trades")."""
    if not sd or not sd.get("start") or sd.get("made") is None:
        return None
    from .standing import period_view
    b = next((x for x in sd.get("bots") or () if x.get("id") == bot), {}) or {}
    magic = int(b.get("magic") or DEFAULT_MAGICS.get(bot, 0))
    if magic in set(sd.get("unknown_magics") or ()):
        return None                                   # its rows could not be read: never a false "no trades"
    view = period_view(sd, list(deals or ()), "today", now=now)
    rows = bot_trade_rows(data_dir, bot, view, magic)
    return {"live": [r for r in rows if r["mode"] == "LIVE"], "practice": [r for r in rows if r["mode"] != "LIVE"],
            "magic": magic, "mode": str(b.get("mode") or ""), "figure": (view.get("bots") or {}).get(bot) or {}}


def _day_tiles(rows: list, ccy: str = "GBP") -> dict:
    """Trades, won, lost, win rate, money, pips and the best trade of some trade rows."""
    nets = [float(r.get("net") or 0.0) for r in rows]
    won = sum(1 for v in nets if v > 0)
    pips = [float(r["pips"]) for r in rows if r.get("pips") is not None]
    best = max(rows, key=lambda r: float(r.get("net") or 0.0), default=None)
    best_txt = None
    if best is not None:
        money = _money_txt(best.get("net"), ccy)
        best_txt = (f"{best.get('symbol')} {float(best['pips']):+.1f} pips ({money})" if best.get("pips") is not None
                    else f"{best.get('symbol')} ({money})")
    return {"trades": len(rows), "won": won, "lost": len(rows) - won,
            "win_rate": round(100.0 * won / len(rows), 1) if rows else None,
            "net_money": round(sum(nets), 2), "net_pips": round(sum(pips), 1) if pips else None,
            "pips_missing": len(rows) - len(pips), "best_trade": best_txt}


def _rider_state_html(st: dict, mode_set: str, now: dt.datetime) -> str:
    """Is the Rider healthy, paused or not running, and its exposure."""
    e = html.escape
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
    return (f'<div class="card">{state} {e(str(h.get("summary") or ""))}{restart}'
            f'<div class="small">{e(str(st.get("tagline") or ""))} Exposure: '
            f'GBP {_num(st.get("user_pip_value_gbp"), "{:.2f}")} per pip (Aaron\'s setting). '
            f'Order flow: {e(str(st.get("order_flow") or "-"))}.</div></div>')


def _rider_scanner_html(st: dict) -> str:
    """The Rider's live scanner: MARKET, DIRECTION, SCORE, STATE, REASON, best score first."""
    e = html.escape
    scan = [r for r in (st.get("scanner") or []) if isinstance(r, dict)]
    if not scan:
        return '<div class="card"><h2>The scanner right now</h2><div class="small">No scan reported yet.</div></div>'
    scan.sort(key=lambda r: -float(r.get("score") or 0.0))
    body = "".join(
        f'<tr><td><b>{e(str(r.get("market")))}</b></td><td>{e(str(r.get("direction")))}</td>'
        f'<td class="mono">{_num(r.get("score"), "{:.0f}")}</td><td>{e(str(r.get("state")))}</td>'
        f'<td class="small">{e(str(r.get("reason") or ""))}</td></tr>' for r in scan)
    return (f'<div class="card"><h2>The scanner right now - {len(scan)} markets</h2><table><tr><th>Market</th>'
            f'<th>Direction</th><th>Score</th><th>State</th><th>Reason</th></tr>{body}</table></div>')


def _rider_checks_html(st: dict) -> str:
    """The Rider's health checks, as its status file gives them."""
    e = html.escape
    checks = (st.get("health") or {}).get("checks") or []
    if not checks:
        return ""
    body = "".join(
        f'<tr><td><span class="pill {"ok" if c.get("ok") else ("bad" if c.get("critical") else "warn")}">'
        f'{"OK" if c.get("ok") else ("FAIL" if c.get("critical") else "WARN")}</span></td>'
        f'<td>{e(str(c.get("name") or ""))}</td><td>{e(str(c.get("message") or ""))}</td></tr>'
        for c in checks if isinstance(c, dict))
    return f'<div class="card"><h2>Health checks</h2><table>{body}</table></div>'


def _rider_files(data: Path) -> tuple[str, str, str]:
    """(mode in rider.json, its status file, its own sqlite)."""
    from .watchdog import own_file_mode
    settings = _own_file(data, "rider.json")
    status_name = settings.get("status_file") if isinstance(settings.get("status_file"), str) else "rider-status.json"
    db_name = settings.get("journal_file") if isinstance(settings.get("journal_file"), str) else "rider.sqlite"
    return own_file_mode(data / "rider.json"), status_name or "rider-status.json", db_name or "rider.sqlite"


def _rider_live_html(data_dir: str, now: dt.datetime) -> str:
    """The Rider's live view on the status page: its state, the scanner and
    its health checks, from its status file; a link to its own page."""
    e = html.escape
    link = '<div class="small"><a href="/rider">Everything it is doing, trade by trade</a></div>'
    if not data_dir:
        return '<div class="card small">The status page has not been told where the bots keep their files.</div>'
    data = Path(data_dir)
    mode_set, status_name, _db = _rider_files(data)
    st, why = _read_status_file(data / status_name)
    if st is None:
        what = "not started yet" if why == "missing" else f"its status file {why}"
        return (f'<div class="card"><b>Rapid Momentum Rider: {e(what)}.</b> <span class="small">It is set to '
                f'{e(mode_set)} in data/rider.json; this fills in once it reports.</span>{link}</div>')
    return _rider_state_html(st, mode_set, now) + _rider_scanner_html(st) + _rider_checks_html(st) + link


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


def render_rider_page(data_dir: str, now: Optional[dt.datetime] = None, sd: Optional[dict] = None,
                      deals=None) -> str:
    """The Rapid Momentum Rider's own page: TODAY, every trade's story, the
    live scanner, open positions with their FlowLock X state, and the
    health checks. Read-only, from data/rider-status.json and
    data/rider.sqlite. Renders (saying "not started yet") when they are
    missing.

    TODAY is the UK day. Given the account's standing and deal rows (``sd``,
    ``deals``: the status page hands over its own), it is worked out here
    from MetaTrader's positions for the Rider's magic - the same records and
    the same day as its card, so the two can never disagree; without them
    (MetaTrader not read yet), from its status file or its own records."""
    e = html.escape
    now = to_utc(now or utcnow())
    title = "RAPID MOMENTUM RIDER"
    if not data_dir:
        return _page("Rapid Momentum Rider", f'<h1>{title}</h1><div class="card">Not started yet: the status page has '
                                             f'not been told where the bots keep their files.</div>')
    data = Path(data_dir)
    mode_set, status_name, db_name = _rider_files(data)
    st, why = _read_status_file(data / status_name)
    day0 = uk_day_start(now)
    from .attribution import _rows_ro
    rows = _rows_ro(data / db_name,
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
        parts.append(_rider_state_html(st, mode_set, now))
    # ---- TODAY
    day = None
    try:
        day = bot_today(sd, deals, "momentum_rider", data_dir, now)
    except Exception as exc:                             # never a reason to fail the page: say it, fall back
        log.warning("rider today from MetaTrader: %s", exc)
    by_ticket: dict = {}
    if day is not None:
        live_now = bool(day["live"]) or "LIVE" in (mode_set, str(st.get("mode") or ""), day["mode"])
        shown = day["live"] if live_now else day["practice"]
        today = _day_tiles(shown)
        by_ticket = {int(r.get("ticket") or 0): r for r in day["live"]}
        if live_now:
            source = (f"MetaTrader's own records for its magic number {day['magic']}: every trade it opened that "
                      f"closed since 00:00 UK today, each counted once and in full (both halves of the commission) - "
                      f"the same figures as its card on the status page")
            money_src = "MetaTrader's figures"
        else:
            source = "its own records: every PAPER trade that closed since 00:00 UK today"
            money_src = "practice (paper) figures, not money"
        extra = []
        if today["pips_missing"] and shown:
            extra.append(f"pips are from its own records, which do not have {today['pips_missing']} of these trades")
        if live_now and day["practice"]:
            p = _day_tiles(day["practice"])
            extra.append(f"practice today (PAPER, not money): {p['trades']} trade{'' if p['trades'] == 1 else 's'}, "
                         f"{_money_txt(p['net_money'])}")
        est = ("; " + "; ".join(extra)) if extra else ""
    else:
        today = st.get("today") if isinstance(st.get("today"), dict) else None
        source = "the Rider's status file (MetaTrader's records have not reached this page yet)"
        if today is None:
            today = _rider_today_from_rows(rows)
            source = "the Rider's own records since 00:00 UK"
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
    parts.append(f'<div class="card"><h2>Today (UK day, since 00:00 UK)</h2><div class="row">{tiles}</div>'
                 f'<div class="small">From {e(source)}; money: {e(money_src or "-")}{e(est)}.</div></div>')
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
    parts.append(_rider_scanner_html(st))
    # ---- every trade's story
    rows = [r for r in rows if not str(r.get("exit_reason") or "").startswith("SWITCHED")]
    if rows:
        def story(r):
            mine = by_ticket.get(int(r.get("ticket") or 0)) if str(r.get("mode") or "") == "LIVE" else None
            if mine is not None:                           # MetaTrader's figure for the whole position
                money = f'{_money_txt(mine.get("net"))} (MetaTrader, in full)'
                cls = _cls_of(mine.get("net"))
            else:
                money = (_money_txt(r.get("net_money"))
                         + (" (estimate)" if r.get("money_source") == "estimate" else ""))
                cls = _cls_of(r.get("net_money"))
            return (f'<div class="story"><b>{e(str(r.get("symbol")))}</b> {e(str(r.get("side") or ""))} '
                    f'<span class="pill">{e(str(r.get("mode") or "PAPER"))}</span> '
                    f'<span class="mono">{e(_uk(r.get("closed_utc") or "", "%H:%M:%S"))} UK</span> '
                    f'<span class="mono {_cls_of(r.get("realised_pips"))}">{_num(r.get("realised_pips"), "{:+.1f}")} pips</span> '
                    f'<span class="mono {cls}">{e(money)}</span>'
                    f'<div class="small">{e(str(r.get("explanation") or r.get("exit_detail") or ""))}</div></div>')
        parts.append(f'<div class="card"><h2>Every trade today, explained - {len(rows)}</h2>'
                     f'{"".join(story(r) for r in rows)}</div>')
    elif st.get("stories"):
        stories = "".join(f'<div class="story small">{e(str(x))}</div>' for x in st["stories"])
        parts.append(f'<div class="card"><h2>Recent trades, explained</h2>{stories}</div>')
    else:
        parts.append('<div class="card"><h2>Every trade today, explained</h2><div class="small">No trades today yet.'
                     '</div></div>')
    # ---- health checks
    parts.append(_rider_checks_html(st))
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


def _ian_feed(st: dict) -> tuple[str, str]:
    """(the feed's state, its pill class)."""
    feed = st.get("feed") or {}
    fstate = str(feed.get("state") or "UNKNOWN")
    return fstate, {"LIVE": "ok", "DEGRADED": "warn", "NOT CONFIGURED": "warn", "CONNECTING": "warn"}.get(fstate, "bad")


def _ian_top_html(st: dict) -> str:
    """Financial Ian's top opportunity and its plain-English reasoning."""
    e = html.escape
    top = st.get("top_opportunity") or {}
    if not top:
        return ('<div class="card"><h2>Top opportunity</h2><div class="small">None yet: it needs the institutional '
                'order book to form a view.</div>'
                + (f'<p>{e(str(st.get("reasoning")))}</p>' if st.get("reasoning") else "") + '</div>')
    reg = top.get("regime") or {}
    return (f'<div class="card"><h2>Top opportunity</h2><b>{e(str(top.get("spot_symbol")))} '
            f'{e(str(top.get("spot_side") or "no side"))}</b> from {e(str(top.get("instrument") or top.get("root")))} - '
            f'score {_num(top.get("score"), "{:.0f}")}, {e(str(reg.get("regime") if isinstance(reg, dict) else reg))}, '
            f'{"tradeable" if top.get("tradeable") else "not tradeable: " + e(str(top.get("why_not") or ""))}'
            f'<p>{e(str(st.get("reasoning") or top.get("reasoning") or ""))}</p></div>')


def _ian_live_html(data_dir: str, now: dt.datetime) -> str:
    """Financial Ian's live view on the status page: its feed and its
    reasoning, from its status file; the order-book ladder is on /ian."""
    e = html.escape
    link = '<div class="small"><a href="/ian">The order book ladder, the reasoning and the trades</a></div>'
    if not data_dir:
        return '<div class="card small">The status page has not been told where the bots keep their files.</div>'
    from .watchdog import own_file_mode
    data = Path(data_dir)
    mode_set = own_file_mode(data / "ian.json")
    st, why = _read_status_file(data / "ian-status.json")
    if st is None:
        what = "not started yet" if why == "missing" else f"its status file {why}"
        return (f'<div class="card"><b>Financial Ian: {e(what)}.</b> <span class="small">It is set to {e(mode_set)} '
                f'in data/ian.json. With no institutional data feed it runs DATA-DEGRADED: no signals, no trades, and '
                f'says so here once it reports.</span>{link}</div>')
    age = _age_of(st.get("updated") or st.get("updated_utc"), now)
    running = age is not None and age <= BOT_STALE_SECONDS
    feed = st.get("feed") or {}
    fstate, fcls = _ian_feed(st)
    label = str(st.get("data_label") or "LIVE")
    return (f'<div class="card"><h2>Financial Ian right now</h2><b>{e(str(st.get("status") or ""))}</b>'
            + (f' <span class="pill bad">{e(label)}</span>' if label != "LIVE" else "")
            + f'<div>Running: {"yes" if running else "no" + ("" if age is None else f" (last report {age / 60:.0f} min ago)")}'
            f' &middot; Feed: <span class="pill {fcls}">{e(fstate)}</span> {e(str(feed.get("vendor") or "none"))} - '
            f'{e(str(feed.get("reason") or ""))}</div>{link}</div>' + _ian_top_html(st))


def render_ian_page(data_dir: str, now: Optional[dt.datetime] = None, sd: Optional[dict] = None,
                    deals=None) -> str:
    """Financial Ian's own page, read-only from data/ian-status.json:
    running, the feed (NOT CONFIGURED / LIVE / DEGRADED / DOWN), MetaTrader,
    the top opportunity and its plain-English reasoning, open positions,
    today, the last trade and the order-book ladder. Renders (saying "not
    started yet") when the file is missing. Today is the UK day; given the
    account's standing and deal rows (``sd``, ``deals``) its figures are
    MetaTrader's for Ian's magic, the same as its card."""
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
    fstate, fcls = _ian_feed(st)
    mt5 = (st.get("mt5") or {}).get("connected")
    today = st.get("today") or {}
    wr = today.get("win_rate")
    today_src = (f"Financial Ian's own records ({e(str(today.get('data_source') or '-'))}, "
                 f"{e(str(st.get('mode') or mode_set))}); PAPER is practice, not money")
    day = None
    try:
        day = bot_today(sd, deals, "financial_ian", data_dir, now)
    except Exception as exc:                             # never a reason to fail the page
        log.warning("ian today from MetaTrader: %s", exc)
    if day is not None:
        live_now = bool(day["live"]) or "LIVE" in (mode_set, str(st.get("mode") or ""), day["mode"])
        t = _day_tiles(day["live"] if live_now else day["practice"])
        today = {"net": t["net_money"], "trades": t["trades"]}
        wr = None if t["win_rate"] is None else t["win_rate"] / 100.0
        today_src = (f"MetaTrader's own records for its magic number {day['magic']}: every trade it opened that closed "
                     f"since 00:00 UK today, each in full after both halves of its commission - the same figures as its "
                     f"card on the status page" if live_now else
                     "its own records: every PAPER trade that closed since 00:00 UK today - practice, not money")
        today_src = e(today_src)
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
                 f'<div class="small">Today (UK day, since 00:00 UK) is from {today_src}.</div></div>')
    parts.append(_ian_top_html(st))
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

    def _query(self) -> tuple[str, str, str, str, str]:
        """(strategy, period, from, to, bot) from the address."""
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(self.path).query)
        return ((q.get("strategy") or ["overall"])[0], (q.get("period") or ["today"])[0], (q.get("from") or [""])[0],
                (q.get("to") or [""])[0], _bot_choice((q.get("bot") or ["all"])[0]))

    def _figures(self, snap: dict, period: str, date_from: str, date_to: str, bot: str) -> tuple[dict, dict, dict, list]:
        """(snap, headline, standing, deals): the chosen period and Total from
        MetaTrader's deal rows (``standing.period_view``) and each bot's
        practice (``standing.practice_figures``), all from the same refresh -
        the one way both pages work out money."""
        headline = None
        now = self.state.now()
        with self.state.lock:                    # the figures and their deal rows from the same refresh
            sd = dict(self.state.standing or {})
            deals = list(self.state.deals or ())
            data_dir = self.state.data_dir
        snap = dict(snap)
        snap["standing"] = sd
        snap["bot_selected"] = bot
        try:                                     # 9 Oct: which of Trend & Breakout's markets trade for real
            from .verdicts import tnb_live_setting
            snap["tnb_live"] = tnb_live_setting(data_dir)
        except Exception as exc:                 # never a reason to fail the page
            log.warning("Trend & Breakout's live markets: %s", exc)
        if sd.get("start") and sd.get("made") is not None:
            try:
                from .standing import bot_view, open_charged, period_view, practice_figures
                sel = period_view(sd, deals, period, date_from, date_to, now=now)
                sel["from"], sel["to"] = date_from, date_to
                tot = period_view(sd, deals, "total", now=now)
                headline = {"sel": sel, "tot": tot, "p_sel": {}, "p_tot": {}, "data_dir": data_dir, "now": now}
                snap["open_charged"] = open_charged(deals, sd.get("open_tickets"))
                today_v = sel if sel["key"] == "today" else period_view(sd, deals, "today", now=now)
                headline["today"] = today_v              # "Trades today" on "/"
                rider = next((b for b in sd.get("bots") or () if b.get("id") == "momentum_rider"), None)
                if rider is not None and rider.get("made") is not None:
                    snap["bot_today"] = {"momentum_rider": bot_view(today_v, int(rider.get("magic") or 0))}
                if data_dir:
                    ccy = str(sd.get("currency") or "GBP")
                    for k, v in (("p_sel", sel), ("p_tot", tot)):
                        headline[k] = practice_figures(data_dir, dt.datetime.fromisoformat(v["start"]), now,
                                                       ccy, end=dt.datetime.fromisoformat(v["end"]))
            except Exception as exc:
                log.warning("period %s: %s", period, exc)
                headline = None
        if headline is None:                     # the chosen period still shows on its button
            headline = {"period": {"key": period, "from": date_from, "to": date_to}, "data_dir": data_dir, "now": now}
            try:                                 # and its UK days, for each bot's practice on the simple page
                from .standing import period_bounds
                a, b, label, _key = period_bounds(sd, period, date_from, date_to, now)
                headline["period"].update(label=label, start=a.isoformat(), end=b.isoformat())
            except Exception as exc:
                log.debug("period bounds %s: %s", period, exc)
        return snap, headline, sd, deals

    def _details_page(self, snap: dict) -> str:
        """/details: the full page, as it was the main page until 9 Oct - every
        figure, the folded tabs, the scanner, the connections."""
        strategy, period, date_from, date_to, bot = self._query()
        snap, headline, sd, _deals = self._figures(snap, period, date_from, date_to, bot)
        now = headline["now"]
        # the folded tabs follow the same filter (from the bots' own records), on the same UK days
        tab_period, tf, tt = period, date_from, date_to
        if period == "total" and sd.get("start"):
            try:
                tab_period, tf, tt = ("custom", uk_date(dt.datetime.fromisoformat(str(sd["start"]))).isoformat(),
                                      uk_date(now).isoformat())
            except Exception:
                tab_period, tf, tt = "custom", str(sd["start"])[:10], uk_date(now).isoformat()
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
        return render_status(snap, strategy, headline, bot, base="/details")

    def _home_page(self, snap: dict) -> str:
        """/: the simple page; /?bot=<id>: that bot's verdict above its detail."""
        from .verdicts import all_verdicts, load_plan
        _strategy, period, date_from, date_to, bot = self._query()
        snap, headline, sd, deals = self._figures(snap, period, date_from, date_to, bot)
        plan, plan_error = load_plan()
        try:
            verdicts = all_verdicts(sd, headline.get("sel"), headline.get("tot"), headline.get("data_dir") or "",
                                    headline["now"], plan, period=headline.get("period"), deals=deals)
        except Exception as exc:                 # never a reason to fail the page
            log.warning("verdicts: %s", exc)
            verdicts = {}
        if bot != "all":
            from .standing import BOTS
            mode = next((str(b.get("mode") or "") for b in sd.get("bots") or () if b.get("id") == bot), "")
            return render_bot_detail(snap, headline, bot,
                                     verdicts.get(bot) or {"label": dict(BOTS).get(bot, bot), "mode": mode},
                                     plan, verdicts, deals=deals)
        from urllib.parse import parse_qs, urlparse
        show = (parse_qs(urlparse(self.path).query).get("show") or [""])[0]
        return render_home(snap, headline, verdicts, plan, plan_error, deals=deals, show=show)

    def _lab_page(self) -> str:
        """/lab: the lab in full; /lab?id=<id>: one candidate. Read-only, from
        data/lab/summary.json and data/lab-status.json."""
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        with self.state.lock:
            data_dir = self.state.data_dir
        now = self.state.now()
        if "id" in q:
            return render_candidate(data_dir, (q.get("id") or [""])[0], now)
        return render_lab_board(data_dir, now)

    def _coach_page(self, snap: dict) -> str:
        """/coach: the coach's full record, read only (data/coach.json, data/coach/status.json and the
        account's own figures for the one real number); then what changed since yesterday and the plan
        (moved off "/" on 12 Oct)."""
        from . import showcase as S
        from .verdicts import all_verdicts, load_plan
        with self.state.lock:
            data_dir = self.state.data_dir
            sd, deals = dict(self.state.standing or {}), list(self.state.deals or ())
        now = self.state.now()
        extra = ""
        try:
            lab = S.load_lab(data_dir, now)
            extra += _changes_html(S.changes(lab, data_dir, now))
        except Exception as exc:                 # never a reason to fail the page
            log.warning("what changed: %s", exc)
        plan, plan_error = load_plan()
        try:
            _snap, headline, sd2, deals2 = self._figures(snap, "today", "", "", "all")
            verdicts = all_verdicts(sd2, headline.get("sel"), headline.get("tot"), headline.get("data_dir") or "",
                                    headline["now"], plan, period=headline.get("period"), deals=deals2)
        except Exception as exc:                 # never a reason to fail the page
            log.warning("verdicts: %s", exc)
            verdicts = {}
        extra += f'<div id="plan">{_plan_html(plan, plan_error, verdicts)}</div>'
        body = coach_page.render_page_body(data_dir, sd, deals, now) + extra
        return _home_page("The coach", body, old_css=True, active="coach", stamp=_now_stamp(now))

    def _coach_progress(self, snap: dict) -> dict:
        """/api's coach_progress: real money since 12 Oct, the page's own arithmetic (never a projection)."""
        with self.state.lock:
            sd, deals = dict(self.state.standing or {}), list(self.state.deals or ())
        try:
            return coach_page.progress(sd, deals, self.state.now())
        except Exception as exc:                 # never a reason to fail the page
            return {"ok": False, "why": str(exc)}

    def do_GET(self):
        try:
            snap = self.state.snapshot()
            if self.path.startswith("/results"):
                self._send(render_results(snap.get("results") or {}))
            elif self.path.startswith("/rider"):
                with self.state.lock:                    # the same records the cards are drawn from
                    data_dir = self.state.data_dir
                    sd, deals = dict(self.state.standing or {}), list(self.state.deals or ())
                self._send(render_rider_page(data_dir, self.state.now(), sd=sd, deals=deals))
            elif self.path.startswith("/ian"):
                with self.state.lock:
                    data_dir = self.state.data_dir
                    sd, deals = dict(self.state.standing or {}), list(self.state.deals or ())
                self._send(render_ian_page(data_dir, self.state.now(), sd=sd, deals=deals))
            elif self.path.startswith("/health"):
                code = 200 if (snap.get("health") or {}).get(
                    "entries_allowed", False) else 503
                self._send(json.dumps(snap, indent=2, default=str),
                           "application/json", code)
            elif self.path.startswith("/weekly-live"):
                # 13 Oct: MetaTrader's closed positions of the bots' magics, for the weekly pick's live judgement
                with self.state.lock:
                    sd, deals = dict(self.state.standing or {}), list(self.state.deals or ())
                from .weekly import live_positions_json
                body = (live_positions_json(deals, sd.get("open_tickets")) if sd.get("start") and sd.get("made")
                        is not None else {"ok": False, "why": "MetaTrader's records have not been read yet"})
                self._send(json.dumps(body, default=str), "application/json")
            elif self.path.startswith("/api"):
                snap["coach_progress"] = self._coach_progress(snap)
                self._send(json.dumps(snap, indent=2, default=str),
                           "application/json")
            elif self.path.startswith("/details"):
                self._send(self._details_page(snap))
            elif self.path.split("?", 1)[0].rstrip("/") == "/lab":
                self._send(self._lab_page())
            elif self.path.split("?", 1)[0].rstrip("/") == "/coach":
                self._send(self._coach_page(snap))
            else:
                self._send(self._home_page(snap))
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
