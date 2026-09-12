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
margin:0;background:#f5f6f8;color:#1b1c1e}
.wrap{max-width:1000px;margin:0 auto;padding:18px}
h1{font-size:22px;margin:6px 0 14px}
h2{font-size:17px;margin:22px 0 8px}
.card{background:#fff;border:1px solid #e2e4e8;border-radius:10px;
padding:14px 16px;margin-bottom:12px}
.row{display:flex;flex-wrap:wrap;gap:10px}
.tile{flex:1 1 150px;background:#fff;border:1px solid #e2e4e8;border-radius:10px;
padding:12px 14px}
.tile .k{font-size:12px;text-transform:uppercase;letter-spacing:.4px;color:#6b7280}
.tile .v{font-size:20px;font-weight:600;margin-top:4px}
.ok{color:#0a7c35}.bad{color:#b91c1c}.warn{color:#b45309}
.pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:12px;
font-weight:600}
.pill.ok{background:#e7f6ec;color:#0a7c35}
.pill.bad{background:#fdeaea;color:#b91c1c}
.pill.warn{background:#fef4e6;color:#b45309}
table{width:100%;border-collapse:collapse;font-size:14px}
th,td{text-align:left;padding:7px 8px;border-bottom:1px solid #eef0f3;
vertical-align:top}
th{font-size:12px;text-transform:uppercase;color:#6b7280;letter-spacing:.4px}
.mono{font-variant-numeric:tabular-nums}
.small{font-size:13px;color:#4b5563}
a{color:#1d4ed8;text-decoration:none}
.banner{padding:12px 16px;border-radius:10px;font-weight:600;margin-bottom:12px}
.banner.ok{background:#e7f6ec;color:#0a7c35;border:1px solid #b7e3c6}
.banner.bad{background:#fdeaea;color:#b91c1c;border:1px solid #f3c0c0}
nav{margin-bottom:10px;font-size:14px}
nav a{margin-right:14px}
"""


def render_status(snap: dict) -> str:
    st = snap.get("status") or {}
    health = snap.get("health") or {}
    checks = health.get("checks") or []
    problems = [c for c in checks if c["severity"] != "OK"]
    running = st.get("bot") == "RUNNING" and not health.get("safe_mode")
    banner_cls = "ok" if running and not problems else "bad"
    banner_txt = ("BOT: RUNNING - everything is healthy" if running and not problems
                  else f"BOT: PROBLEM - {health.get('summary', 'see below')}")

    def tile(k, v, cls=""):
        return (f'<div class="tile"><div class="k">{html.escape(k)}</div>'
                f'<div class="v {cls}">{v}</div></div>')

    def yesno(flag, good="CONNECTED", bad="NOT CONNECTED"):
        return (f'<span class="pill ok">{good}</span>' if flag
                else f'<span class="pill bad">{bad}</span>')

    tiles = "".join([
        tile("MT5", yesno(st.get("mt5_connected"))),
        tile("Broker", yesno(st.get("broker_connected"))),
        tile("Market data", yesno(st.get("data_live"), "LIVE", "STALE")),
        tile("News", yesno(st.get("news_live"), "LIVE", "DEGRADED")),
        tile("Watchdog", yesno(st.get("watchdog_ok"), "HEALTHY", "NOT SEEN")),
        tile("Mode", f'<span class="pill {"bad" if st.get("mode")=="LIVE" else "ok"}">'
                     f'{html.escape(str(st.get("mode", "?")))}</span>'),
        tile("Open trades", st.get("open_positions", 0)),
        tile("Today", _fmt_money(st.get("today_pnl", 0.0),
                                 st.get("currency", "")),
             "ok" if (st.get("today_pnl") or 0) >= 0 else "bad"),
        tile("Win rate today",
             "-" if st.get("win_rate_today") is None
             else f"{st['win_rate_today']:.0f}%"),
        tile("Last scan", html.escape(str(st.get("last_scan", "never")))),
        tile("Last trade", html.escape(str(st.get("last_trade", "none yet")))),
        tile("Equity", _fmt_money(st.get("equity", 0.0),
                                  st.get("currency", ""))),
    ])

    prob_html = ""
    if problems:
        rows = "".join(
            f'<tr><td><span class="pill '
            f'{"bad" if c["severity"] == "FAIL" else "warn"}">'
            f'{c["severity"]}</span></td><td>{html.escape(c["message"])}</td></tr>'
            for c in problems)
        prob_html = (f'<h2>What is wrong</h2><div class="card">'
                     f'<table>{rows}</table></div>')

    think = snap.get("thinking") or []
    if think:
        rows = "".join(
            f'<tr><td class="mono">{t["rank"]}</td><td><b>{html.escape(t["symbol"])}</b></td>'
            f'<td>{t["direction"]}</td><td class="mono">{t["score"]:.0f}</td>'
            f'<td>{html.escape(t["tier"])}</td>'
            f'<td>{html.escape(t["tactic"].replace("_", " ").title())}</td>'
            f'<td class="small">{html.escape(t["regime"])}'
            + (f'<br><i>waiting: {html.escape("; ".join(t["blockers"]))}</i>'
               if t.get("blockers") else "")
            + '</td></tr>'
            for t in think)
        think_html = (
            '<h2>What the bot is looking at right now</h2><div class="card">'
            '<table><tr><th>#</th><th>Market</th><th>Side</th><th>Score</th>'
            '<th>Tier</th><th>Approach</th><th>Why</th></tr>'
            + rows + '</table></div>')
    else:
        think_html = ('<h2>What the bot is looking at right now</h2>'
                      '<div class="card small">No scan has completed yet.</div>')

    pos = snap.get("positions") or []
    if pos:
        rows = "".join(
            f'<tr><td><b>{html.escape(p["symbol"])}</b></td><td>{p["side"]}</td>'
            f'<td class="mono">{p["volume"]}</td>'
            f'<td class="mono">{p["entry"]}</td><td class="mono">{p["stop"]}</td>'
            f'<td class="mono {"ok" if p["profit"] >= 0 else "bad"}">'
            f'{_fmt_money(p["profit"], st.get("currency", ""))}</td>'
            f'<td class="small">{html.escape(str(p.get("flow_state", "")))} '
            f'{html.escape(str(p.get("note", "")))}</td></tr>' for p in pos)
        pos_html = ('<h2>Open trades</h2><div class="card"><table>'
                    '<tr><th>Market</th><th>Side</th><th>Lots</th><th>Entry</th>'
                    '<th>Stop</th><th>P&amp;L</th><th>State</th></tr>'
                    + rows + '</table></div>')
    else:
        pos_html = ('<h2>Open trades</h2><div class="card small">'
                    'No open trades. The bot is watching and waiting for a '
                    'strong enough opportunity.</div>')

    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="10"><title>Trading bot status</title>
<style>{CSS}</style></head><body><div class="wrap">
<nav><a href="/">Status</a><a href="/results">Results</a>
<a href="/health">Health (JSON)</a></nav>
<div class="banner {banner_cls}">{html.escape(banner_txt)}</div>
<div class="row">{tiles}</div>
{prob_html}{pos_html}{think_html}
<p class="small">Updated {html.escape(str(snap.get('updated')))} (UTC).
This page refreshes itself every 10 seconds.</p>
</div></body></html>"""


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
                self._send(render_status(snap))
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
