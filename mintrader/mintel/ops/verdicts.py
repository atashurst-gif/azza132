"""What each bot's record says, and what to do about it - in plain English.

Aaron, 9 Oct 16:45 UK: "the bots' overall logic on what's happened and its
recommendations for each separate bot and a nice simple but intelligent plan
on where we're going, what we're looking to bin off, change or keep.
Remember the sole aim is to find one winning formula and build them one by
one."

Everything here is worked out from the records, read-only, and never
invents a number:

* REAL money: MetaTrader's own closed positions for the bot's magic number,
  each in full after commission - the very rows the page's figures come
  from (``standing.period_view`` / ``standing.bot_view``).
* PRACTICE: the bot's own PAPER record (Trend & Breakout's
  data/tnb_paper.sqlite; every other bot's own sqlite, PAPER rows), counted
  the way the page's practice figures count it. Practice is never money and
  is never added to money; it is evidence about the method.
* For Trend & Breakout, also its own journal from before the account reset
  (data/journal.sqlite), and its strongest part - the "Formula 1"
  candidate - read from the same journal. The journal books only a trade's
  closing deals, so the commission charged when each trade opened is not
  in it: those trades are shown as evidence, labelled so, and never judged.
* Where present, the bot's own research on real past prices:
  data/rider-research/<newest>/summary.json for the Rapid Momentum Rider,
  data/ian-research/<newest>/summary.json for Financial Ian - the newest
  run on real prices (a SYNTHETIC run is never evidence, and never hides a
  real one). Financial Ian's research counts only when it was run on the
  market Ian now trades (ian.json "feed" -> "vendor").

THE RULE (one verdict per bot, decided in ``decide`` from the constants
below, in this order - the first that fits wins):

1. NO EDGE -> BIN (or back to PAPER): its own research on real past prices
   says NO EDGE (over at least MIN_TRADES trades), or its own record has at
   least MIN_TRADES trades and a profit factor under NO_EDGE_PROFIT_FACTOR
   (it won back less than 90p for every pound it lost, after costs).
2. PROVEN -> KEEP AND GROW: at least MIN_TRADES trades over at least
   MIN_DAYS UK days, positive after costs per trade, a profit factor of at
   least PROVEN_PROFIT_FACTOR and more up days than down days.
3. PROMISING -> KEEP TESTING: positive after costs over at least
   MIN_TRADES_PROMISING trades, but short of PROVEN.
4. BREAK-EVEN -> CHANGE OR KEEP TESTING: enough trades to judge
   (MIN_TRADES over MIN_DAYS days) and not positive, but not clearly losing
   either (profit factor NO_EDGE_PROFIT_FACTOR or more): as a whole it does
   not pay; a part of it might.
5. WAITING: it cannot trade at all just now (switched OFF, Financial Ian
   with no data feed, the Crowd Fader with no Coinversa key), so there is
   nothing new to judge.
6. NOT ENOUGH DATA -> TESTING: anything else. Never a verdict from fewer
   trades than the rule allows.

The verdict is decided on every trade whose full costs are on record (real
and practice trades together, each after its own costs; the headline says
how many of each); the page shows each part apart, with its source and its
dates. Money since the reset is always ``standing.period_view``'s own
figure (the balance-based Overall), never the closed trades added up.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
from pathlib import Path
from typing import Optional, Sequence

from ..clock import TZ_LONDON as UK_TZ, to_utc
from .standing import _money, uk_date, uk_midnight

log = logging.getLogger("mintel.verdicts")

# ------------------------------------------------------------ THE RULE --
MIN_TRADES = 30                 # PROVEN, NO EDGE and BREAK-EVEN need at least this many closed trades
MIN_DAYS = 5                    # PROVEN and BREAK-EVEN need them spread over at least this many UK days
PROVEN_PROFIT_FACTOR = 1.2      # PROVEN: won at least £1.20 for every £1 lost, after costs
NO_EDGE_PROFIT_FACTOR = 0.9     # NO EDGE: won back less than 90p for every £1 lost, after costs
MIN_TRADES_PROMISING = 10       # below this a positive result is luck as far as anyone can tell

PROVEN, PROMISING, BREAK_EVEN, NO_EDGE, WAITING, NOT_ENOUGH = (
    "PROVEN", "PROMISING", "BREAK-EVEN", "NO EDGE", "WAITING", "NOT ENOUGH DATA")

# code: (what to do, chip colour). Green and red are kept for money on the page, so the chips use other colours.
VERDICTS = {
    PROVEN: ("KEEP AND GROW", "blue"),
    PROMISING: ("KEEP TESTING", "sky"),
    BREAK_EVEN: ("CHANGE OR KEEP TESTING", "sand"),
    NO_EDGE: ("BIN (OR BACK TO PAPER)", "amber"),
    WAITING: ("WAITING", "grey"),
    NOT_ENOUGH: ("TESTING", "grey"),
}

RULE_TEXT = (f"NO EDGE: its own test on real past prices says no edge, or {MIN_TRADES}+ trades won back under "
             f"£{NO_EDGE_PROFIT_FACTOR:.2f} for every £1 lost. PROVEN: {MIN_TRADES}+ trades over {MIN_DAYS}+ UK days, "
             f"positive after costs, £{PROVEN_PROFIT_FACTOR:.2f}+ won for every £1 lost and more up days than down. "
             f"PROMISING: positive after costs over {MIN_TRADES_PROMISING}+ trades. BREAK-EVEN: {MIN_TRADES}+ trades "
             f"over {MIN_DAYS}+ days, neither. WAITING: it cannot trade. Anything else: NOT ENOUGH DATA.")

# The research each bot has on real past prices: (folder under the data folder, how to read it)
RESEARCH_DIRS = {"momentum_rider": "rider-research", "financial_ian": "ian-research"}

# The bots' own records of PAPER trades: (sqlite file, its money column). The Rider's file may be renamed in
# rider.json ("journal_file"), as the pages read it.
OWN_PRACTICE = {"momentum_rider": ("rider.sqlite", "net_money"), "momentum_runner": ("runner.sqlite", "net_pnl"),
                "band_breaker": ("bandbreaker.sqlite", "net_pnl"), "crowd_fader": ("crowd.sqlite", "net_pnl"),
                "financial_ian": ("ian.sqlite", "net_pnl")}

TNB = "market_intelligence"
_DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}")

# What Trend & Breakout's journal leaves out of a trade's money (it books only the closing deals)
OPEN_FEE = "the commission charged when each trade opened"


# ------------------------------------------------------------- helpers --
def _when(v) -> Optional[dt.datetime]:
    try:
        t = v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return to_utc(t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc))
    except Exception:
        return None


def _day(d: Optional[dt.date]) -> str:
    """'5 Oct' (no platform-only strftime flags: the VPS is Windows)."""
    return "" if d is None else f"{d.day} {d.strftime('%b')}"


def _gbp(v, signed: bool = True) -> str:
    return "-" if v is None else _money(float(v), "GBP", signed=signed)


def _n(n: int, one: str, many: str = "") -> str:
    return f"{n:,} {one if n == 1 else (many or one + 's')}"


def _pf_words(pf: Optional[float]) -> str:
    """How a profit factor reads: 'won back 61p for every £1 it lost'."""
    if pf is None:
        return ""
    if pf < 1:
        return f"won back {round(pf * 100):.0f}p for every £1 it lost"
    return f"won £{pf:.2f} for every £1 it lost"


def _read_json(path: Path) -> Optional[dict]:
    try:
        raw = json.loads(Path(path).read_text())
        return raw if isinstance(raw, dict) else None
    except Exception:
        return None


# ------------------------------------------------------------- trades --
# A trade, whatever its source: {"closed": UTC datetime, "net": money after its costs, "costs": its costs
# (a positive number) or None when the record does not say, "symbol", "ticket", "kind": real | practice | journal}

def real_trades(view: Optional[dict], magic: int) -> list[dict]:
    """The bot's REAL trades in a ``standing.period_view``: MetaTrader's
    positions opened with its magic that closed in the period, each in full
    after commission (the same positions the page's figures count)."""
    if not view or not magic:
        return []
    from .standing import bot_view
    out = []
    for p in bot_view(view, int(magic))["closed"]:
        t = _when(p.get("closed"))
        if t is None:
            continue
        out.append({"closed": t, "net": round(float(p.get("net") or 0.0), 2),
                    "costs": round(abs(float(p.get("commission") or 0.0)), 2), "symbol": str(p.get("symbol") or ""),
                    "ticket": int(p.get("position") or 0), "kind": "real"})
    return out


def _own_practice_db(data: Path, bot: str) -> Optional[Path]:
    rec = OWN_PRACTICE.get(bot)
    if rec is None:
        return None
    name = rec[0]
    if bot == "momentum_rider":
        jf = (_read_json(data / "rider.json") or {}).get("journal_file")
        if isinstance(jf, str) and jf.strip():
            name = jf
    return data / name


def practice_trades(data_dir, bot: str, start: dt.datetime, end: dt.datetime, record=None) -> list[dict]:
    """The bot's PRACTICE (PAPER) trades that closed in [start, end), from
    its own record - counted exactly as the page's practice figures count
    them (``standing.practice_figures``): Trend & Breakout's paper positions
    in full; every other bot's PAPER rows (the Rider's switch-closes and
    Financial Ian's synthetic or replayed rows left out). ``record``: a
    ``TnbPaperRecord`` already read. Read-only; a missing file is no trades."""
    if not data_dir:
        return []
    data = Path(data_dir)
    a, b = to_utc(start), to_utc(end)
    if bot == TNB:
        from .standing import TnbPaperRecord
        record = record if record is not None else TnbPaperRecord(data)
        if record.error:
            return []
        return [{"closed": to_utc(p["closed"]), "net": round(float(p["net"]), 2),
                 "costs": round(float(p.get("commission") or 0.0), 2), "symbol": p.get("symbol") or "",
                 "ticket": int(p.get("ticket") or 0), "kind": "practice"} for p in record.positions(a, b)]
    path = _own_practice_db(data, bot)
    if path is None or not path.exists():
        return []
    from .attribution import _rows_ro, is_test_data, row_mode
    money = OWN_PRACTICE[bot][1]
    rows = _rows_ro(path, "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND closed_utc < ? "
                          "ORDER BY closed_utc ASC LIMIT 5000", (a.isoformat(), b.isoformat()))
    out = []
    for r in rows:
        if row_mode(r) == "LIVE":
            continue                                    # real money is MetaTrader's, never the bot's own row
        if bot == "momentum_rider" and str(r.get("exit_reason") or "").startswith("SWITCHED"):
            continue                                    # closed in its records at a switch: never a trade
        if bot == "financial_ian" and is_test_data(r):
            continue                                    # a synthetic or replayed feed: a test, not a result
        t = _when(r.get("closed_utc"))
        if t is None:
            continue
        c = r.get("commission")
        out.append({"closed": t, "net": round(float(r.get(money) or 0.0), 2),
                    "costs": None if c is None else round(abs(float(c)), 2),
                    "symbol": str(r.get("symbol") or r.get("spot_symbol") or r.get("instrument") or ""),
                    "ticket": int(r.get("ticket") or 0), "kind": "practice"})
    return out


# -------------------------------------------------------------- stats --
def stats(trades: Sequence[dict]) -> dict:
    """The numbers a verdict is made of, from trades after their costs:
    trades, wins (made money after costs; every other trade is a loss, as
    the page counts them), UK days traded and how many were up or down,
    net, expectancy (net a trade), profit factor (money won / money lost),
    average win and average loss (both positive numbers), costs (None when
    a record does not say), what it made before costs, and costs as a share
    of that; ``journal``: how many trades come from Trend & Breakout's
    journal, whose money leaves out ``OPEN_FEE``. Everything rounded to
    pennies, and decided on what is shown."""
    trades = [t for t in trades if t.get("closed") is not None]
    nets = [round(float(t["net"]), 2) for t in trades]
    n = len(nets)
    won = [v for v in nets if v > 0]
    lost = [v for v in nets if v < 0]
    by_day: dict = {}
    for t in trades:
        d = uk_date(t["closed"])
        by_day[d] = round(by_day.get(d, 0.0) + float(t["net"]), 2)
    gw, gl = round(sum(won), 2), round(-sum(lost), 2)
    net = round(sum(nets), 2)
    known = all(t.get("costs") is not None for t in trades)
    costs = round(sum(float(t["costs"]) for t in trades), 2) if known else None
    before = round(net + costs, 2) if costs is not None else None
    days = sorted(by_day)
    return {"trades": n, "wins": len(won), "losses": n - len(won),
            "win_rate": round(100.0 * len(won) / n, 1) if n else None,
            "days": len(by_day), "up_days": sum(1 for v in by_day.values() if v > 0),
            "down_days": sum(1 for v in by_day.values() if v < 0),
            "net": net, "expectancy": round(net / n, 2) if n else None,
            "profit_factor": round(gw / gl, 2) if gl > 0 else None,
            "money_won": gw, "money_lost": gl,
            "avg_win": round(gw / len(won), 2) if won else None,
            "avg_loss": round(gl / len(lost), 2) if lost else None,
            "costs": costs, "before_costs": before,
            "costs_share": round(costs / before, 2) if costs is not None and before is not None and before > 0 else None,
            "journal": sum(1 for t in trades if t.get("kind") == "journal"),
            "first_day": days[0] if days else None, "last_day": days[-1] if days else None}


# ----------------------------------------------------------- research --
def read_research(data_dir, bot: str) -> Optional[dict]:
    """The bot's newest research on real past prices, from
    data/<rider-research|ian-research>/<newest date>/summary.json, in one
    shape: {code, text, trades, days, unit ("pips" or "£"), expectancy,
    profit_factor, avg_win, avg_loss, best_move (pips, the Rider's average
    best move after entry), versions, versions_lost, synthetic, source,
    period, data, market}. None when there is none. The newest run on real
    prices is the one read: a SYNTHETIC run (its folder name, or its
    summary, says so) is read only when there is no real run, comes back
    with synthetic=True and is never used for a verdict."""
    folder = RESEARCH_DIRS.get(bot)
    if not data_dir or folder is None:
        return None
    root = Path(data_dir) / folder
    try:
        runs = sorted((p for p in root.iterdir() if p.is_dir() and _DATE_DIR.match(p.name)
                       and (p / "summary.json").exists()), key=lambda p: p.name)
    except Exception:
        return None
    if not runs:
        return None
    chosen = runs[-1]
    for run in reversed(runs):                       # newest first: the newest run on real prices wins
        if "SYNTHETIC" in run.name.upper():
            continue
        raw = _read_json(run / "summary.json")
        if raw is not None and _summary_synthetic(raw):
            continue
        chosen = run
        break
    sm = _read_json(chosen / "summary.json")
    rel = f"data/{folder}/{chosen.name}/summary.json"
    if sm is None:
        return {"code": "UNREADABLE", "text": "", "trades": 0, "synthetic": False, "source": rel,
                "error": "the summary could not be read"}
    v = sm.get("verdict")
    raw_code = str(v.get("code") if isinstance(v, dict) else v or "")
    text = str(v.get("text") if isinstance(v, dict) else "; ".join(str(x) for x in sm.get("verdict_reasons") or []))
    code = raw_code.replace("_", " ").upper().strip()
    if code.startswith("NO EDGE"):
        code = NO_EDGE
    synthetic = _summary_synthetic(sm) or "SYNTHETIC" in chosen.name.upper()
    out = {"code": code, "text": text, "synthetic": synthetic, "source": rel,
           "generated": str(sm.get("generated_utc") or "")[:10]}
    if bot == "momentum_rider":
        h = ((sm.get("headline") or {}).get("normal") or {})
        days = list(sm.get("days_replayed") or [])
        variants = [x for x in sm.get("variants") or [] if isinstance(x, dict)]
        nets = [((x.get("normal") or {}).get("net_pips")) for x in variants]
        nets = [float(x) for x in nets if isinstance(x, (int, float))]
        loss = h.get("avg_loss_pips")
        out.update({"trades": int(h.get("trades") or 0), "days": len(days) or h.get("days"), "unit": "pips",
                    "expectancy": h.get("expectancy_pips"), "profit_factor": h.get("profit_factor"),
                    "avg_win": h.get("avg_win_pips"), "avg_loss": None if loss is None else abs(float(loss)),
                    "best_move": h.get("avg_mfe_pips"), "net": h.get("net_pips"),
                    "versions": len(nets), "versions_lost": sum(1 for x in nets if x <= 0),
                    "period": (f"{days[0]} to {days[-1]}" if days else ""),
                    "data": str(sm.get("data_source") or "real past ticks")})
    else:
        st = sm.get("stats") or {}
        loss = st.get("avg_loss_gbp")
        per = sm.get("period") or {}
        if isinstance(per, dict):                    # Ian's research writes {first, last}
            a = str(per.get("first") or per.get("start") or "")
            b = str(per.get("last") or per.get("end") or "")
            period = f"{a} to {b}" if a and b and a != b else (a or b)
        else:
            period = str(per)
        source = str(sm.get("source") or "real past prices")
        out.update({"trades": int(st.get("trades") or 0), "days": sm.get("days"), "unit": "£",
                    "expectancy": st.get("expectancy_gbp"), "profit_factor": st.get("profit_factor"),
                    "avg_win": st.get("avg_win_gbp"), "avg_loss": None if loss is None else abs(float(loss)),
                    "best_move": None, "net": st.get("net_gbp"), "versions": 0, "versions_lost": 0,
                    "period": period, "data": source, "market": _market_of(source)})
    return out


def _summary_synthetic(sm: dict) -> bool:
    """Does a research summary say it ran on made-up prices?"""
    v = sm.get("verdict")
    code = str(v.get("code") if isinstance(v, dict) else v or "")
    return bool(sm.get("synthetic")) or "SYNTHETIC" in code.upper()


# The market a research run or a feed is about, in plain words: (words in its source or vendor, the name)
_MARKETS = ((("databento", "cme", "glbx"), "CME futures (Databento)"), (("binance",), "Binance's public crypto book"))


def _market_of(text: str) -> str:
    low = str(text or "").lower()
    return next((name for keys, name in _MARKETS if any(k in low for k in keys)), "")


def ian_market(data_dir) -> str:
    """The market Financial Ian trades now, from its own file (data/ian.json
    "feed" -> "vendor", else its status file's feed); '' when it does not say."""
    if not data_dir:
        return ""
    data = Path(data_dir)
    for name in ("ian.json", "ian-status.json"):
        feed = (_read_json(data / name) or {}).get("feed")
        vendor = str(feed.get("vendor") or "") if isinstance(feed, dict) else ""
        if vendor.strip():
            return _market_of(vendor)
    return ""


def research_elsewhere(r: Optional[dict], market_now: str) -> str:
    """Why research was run on a different market from the one the bot trades
    now ('' when it was not, or either side does not say): such research is
    shown as evidence about the method, never used for the verdict."""
    ran_on = str((r or {}).get("market") or "")
    if ran_on and market_now and ran_on != market_now:
        return f"this test was on {ran_on}; it now trades {market_now}"
    return ""


def research_counts(r: Optional[dict]) -> bool:
    """Is this research evidence a verdict may rest on? Real data (never
    synthetic), and at least MIN_TRADES trades."""
    return bool(r) and not r.get("synthetic") and int(r.get("trades") or 0) >= MIN_TRADES


# ---------------------------------------------------------- can it trade --
SWITCHED_OFF = "it is switched OFF"


def cannot_trade(data_dir, bot: str, mode: str) -> str:
    """Why the bot cannot trade at all just now, from its own files; '' when
    nothing says it cannot (a missing status file says nothing)."""
    if str(mode or "").upper() == "OFF":
        return SWITCHED_OFF
    if not data_dir:
        return ""
    data = Path(data_dir)
    if bot == "financial_ian":
        st = _read_json(data / "ian-status.json") or {}
        feed = st.get("feed") if isinstance(st.get("feed"), dict) else {}
        if str(feed.get("state") or "").upper() == "NOT CONFIGURED":
            return "it has no market data feed"
    if bot == "crowd_fader":
        st = _read_json(data / "crowd-status.json") or {}
        if str(st.get("status") or "").upper().startswith("NO KEY"):
            return "no Coinversa key is saved"
    return ""


# ------------------------------------------------------------ the rule --
def decide(s: dict, research: Optional[dict] = None, blocked: str = "") -> tuple[str, str]:
    """THE RULE (see the top of this file): (verdict code, why in a few
    words). ``s`` is ``stats`` of everything the bot has done; ``research``
    its own research on real past prices (``read_research``); ``blocked``
    why it cannot trade (``cannot_trade``)."""
    n, days = int(s.get("trades") or 0), int(s.get("days") or 0)
    pf, net = s.get("profit_factor"), float(s.get("net") or 0.0)
    if research_counts(research) and research.get("code") == NO_EDGE:
        return NO_EDGE, "its own test on real past prices found no edge"
    if n >= MIN_TRADES and pf is not None and pf < NO_EDGE_PROFIT_FACTOR:
        return NO_EDGE, f"over {n} trades it {_pf_words(pf)}"
    enough = n >= MIN_TRADES and days >= MIN_DAYS
    strong = pf is None or pf >= PROVEN_PROFIT_FACTOR           # None here: no losing trade at all
    if enough and net > 0 and strong and s.get("up_days", 0) > s.get("down_days", 0):
        return PROVEN, "enough trades, positive after costs, and more up days than down"
    if n >= MIN_TRADES_PROMISING and net > 0:
        return PROMISING, "positive after costs, but not proven yet"
    if enough:
        return BREAK_EVEN, "enough trades to judge, and as a whole it does not pay"
    if blocked:
        return WAITING, blocked
    return NOT_ENOUGH, f"only {n} trade{'' if n == 1 else 's'}"


def _missing_for_proven(s: dict) -> list[str]:
    n, days = int(s.get("trades") or 0), int(s.get("days") or 0)
    out = []
    if n < MIN_TRADES:
        out.append(_n(MIN_TRADES - n, "more trade"))
    if days < MIN_DAYS:
        out.append(_n(MIN_DAYS - days, "more UK day"))
    pf = s.get("profit_factor")
    if pf is not None and pf < PROVEN_PROFIT_FACTOR:
        out.append(f"to win £{PROVEN_PROFIT_FACTOR:.2f} for every £1 lost (now £{pf:.2f})")
    if s.get("up_days", 0) <= s.get("down_days", 0) and n:
        out.append(f"more up days than down (now {s.get('up_days', 0)} up, {s.get('down_days', 0)} down)")
    return out


def _join(parts: list[str]) -> str:
    """'a', 'a and b', 'a, b and c'."""
    parts = [p for p in parts if p]
    if len(parts) <= 1:
        return parts[0] if parts else ""
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def _research_line(r: dict) -> str:
    """The research in one plain sentence, from its own numbers."""
    unit = r.get("unit") or "pips"

    def amt(v, signed=True):
        if v is None:
            return "-"
        return _gbp(v, signed) if unit == "£" else (f"{float(v):+.2f} pips" if signed else f"{float(v):.1f} pips")
    days = r.get("days")
    lead = f"Its own {days}-day test on real past prices" if days else "Its own test on real past prices"
    bits = [f"{lead} ({_n(int(r.get('trades') or 0), 'trade')})"]
    tail = []
    if r.get("expectancy") is not None:
        tail.append(f"{amt(r['expectancy'])} a trade after costs")
    if r.get("profit_factor") is not None:
        tail.append(f"profit factor {float(r['profit_factor']):.2f}")
    if r.get("avg_win") is not None and r.get("avg_loss") is not None:
        tail.append(f"average win {amt(r['avg_win'], False)} against average loss {amt(r['avg_loss'], False)}")
    if r.get("best_move") is not None:
        tail.append(f"the best move after entry only {float(r['best_move']):.1f} pips on average")
    said = "found no edge" if r.get("code") == NO_EDGE else f"says {str(r.get('code') or '-').lower()}"
    out = f"{bits[0]} {said}" + (": " + "; ".join(tail) if tail else "") + "."
    if r.get("versions"):
        out += (" Every version tested lost." if r.get("versions_lost") == r.get("versions")
                else f" {r.get('versions_lost')} of {r.get('versions')} versions tested lost.")
    return out


# ----------------------------------------------------- Trend & Breakout --
def _segment_filter():
    """Trend & Breakout's top segment as its own rules define it
    (``risk.top_segments``: momentum continuation on FX minor pairs), in
    the conditions and markets it can still trade (the fast and news
    markets: momentum continuation in a trend is off; excluded markets
    left out) - the "Formula 1" candidate."""
    from ..config import Config
    from ..contracts import infer_group
    cfg = Config()
    segs = tuple(getattr(cfg.risk, "top_segments", ()) or ())
    off = {(str(t).upper(), str(g).upper()) for t, g in (cfg.scan.disabled_tactic_regimes or ())}
    excluded = {str(x).upper() for x in (getattr(cfg.universe, "excluded_symbols", ()) or ())}

    def keep(row: dict) -> bool:
        tactic = str(row.get("tactic") or "").upper().removesuffix("_2X")
        sym = str(row.get("symbol") or "").upper()
        if sym in excluded or (tactic, str(row.get("regime") or "").upper()) in off:
            return False
        return any(tactic == str(t).upper() and infer_group(sym) == str(g).upper() for t, g in segs)
    return keep


SEGMENT_NAME = "momentum continuation on FX minor pairs, in fast and news markets"


# ------------------------------------------------- Formula 1, LIVE (9 Oct) --
# Aaron, 9 Oct about 19:30 UK: "Just Formula 1" - Trend & Breakout stays on
# PAPER except its minor currency pair trades (config.json "tnb" ->
# "live_groups": ["FX_MINOR"]), which go to the account with real money from
# "live_since_utc". It is judged after 20 live trades (docs/plan.json "focus"
# -> "judge_after"), from MetaTrader's records only - never the journal,
# never practice.
FORMULA1_GROUP = "FX_MINOR"
FORMULA1_JUDGE_AFTER = 20
TNB_MAGIC = 990_311
# the kinds of market in plain words, for "LIVE: minor pairs (Formula 1) - rest PAPER"
_KIND_WORDS = {"FX_MINOR": "minor pairs", "FX_MAJOR": "major pairs", "FX_EXOTIC": "exotic pairs", "INDEX": "indices",
               "GOLD": "gold", "SILVER": "silver", "METAL": "metals", "ENERGY": "energy"}


def tnb_live_setting(data_dir) -> dict:
    """Which kinds of Trend & Breakout's market trade for REAL while it is on
    PAPER, from config.json in the data folder (the file the trader reads):
    {groups: ("FX_MINOR",) - empty when it is LIVE, or all on paper; since:
    tnb.live_since_utc as a UTC datetime, None when not recorded}. Spelled as
    the PaperBroker reads them ("INDICES" is INDEX). Never raises."""
    out = {"groups": (), "since": None}
    raw = _read_json(Path(data_dir) / "config.json") if data_dir else None
    b = (raw or {}).get("tnb")
    if not isinstance(b, dict) or str(b.get("mode") or "").strip().upper() != "PAPER":
        return out
    g = b.get("live_groups") or []
    g = [g] if isinstance(g, str) else (list(g) if isinstance(g, (list, tuple)) else [])
    try:
        from ..broker.paper import GROUP_ALIASES
    except Exception:                                    # never a reason to fail the page
        GROUP_ALIASES = {}
    groups: list = []
    for x in g:
        key = str(x or "").strip().upper()
        if key:
            groups.extend(GROUP_ALIASES.get(key, (key,)))
    out["groups"] = tuple(dict.fromkeys(groups))
    # 10 Oct: tnb.live_tactics - the approaches that go real there, read as
    # the trader reads it (a name it does not know is left out; a list that
    # names none keeps every market on paper). Only in the answer when set.
    lt = b.get("live_tactics")
    lt = [lt] if isinstance(lt, str) else (list(lt) if isinstance(lt, (list, tuple)) else ([] if lt is None else [None]))
    if lt and out["groups"]:
        try:
            from ..engine.tactics import APPROACH_NAMES, approach_of
            known = tuple(dict.fromkeys(a for a in (approach_of(x) for x in lt if isinstance(x, str))
                                        if a in APPROACH_NAMES))
        except Exception:                                # never a reason to fail the page
            known = ()
        if known:
            out["tactics"] = known
        else:
            out["groups"] = ()
    since = str(b.get("live_since_utc") or "").strip()
    out["since"] = _when(since) if since and out["groups"] else None
    return out


def live_kinds(groups, tactics=()) -> str:
    """The kinds of market that trade for real, in plain words: 'minor pairs
    (Formula 1)'; '' when none. 10 Oct: with tnb.live_tactics set, only those
    approaches trade for real there: 'minor pairs (Formula 1, momentum
    continuation only)'."""
    groups, tactics = tuple(groups or ()), tuple(tactics or ())
    if not tactics:
        return _join([_KIND_WORDS.get(g, str(g).lower().replace("_", " "))
                      + (" (Formula 1)" if g == FORMULA1_GROUP else "") for g in groups])
    kinds = _join([_KIND_WORDS.get(g, str(g).lower().replace("_", " ")) for g in groups])
    only = _join([str(t).lower().replace("_", " ") for t in tactics]) + " only"
    f1 = "Formula 1, " if groups == (FORMULA1_GROUP,) and tactics == ("MOMENTUM_CONTINUATION",) else ""
    return f"{kinds} ({f1}{only})" if kinds else ""


def live_words(groups, tactics=()) -> str:
    """Trend & Breakout's mode when part of it trades for real: 'LIVE: minor
    pairs (Formula 1) - rest PAPER'; '' when nothing does."""
    kinds = live_kinds(groups, tactics)
    return f"LIVE: {kinds} - rest PAPER" if kinds else ""


def formula1_live(deals, open_tickets, since: Optional[dt.datetime], magic: int = TNB_MAGIC) -> dict:
    """Formula 1's LIVE record, from MetaTrader's deal rows only: Trend &
    Breakout's REAL positions (its magic) on minor currency pairs
    (``contracts.infer_group``, as the PaperBroker routes them) OPENED at or
    after ``since`` (tnb.live_since_utc). A closed one counts in full - every
    one of its deals, commission and swap - on the UK day it closed
    (``stats``); one still open is counted only as open. {stats, open, since}."""
    from ..contracts import infer_group
    from .standing import positions_of
    trades, n_open = [], 0
    if since is not None:
        since = to_utc(since)
        for p in positions_of(deals or (), open_tickets):
            if p.get("bot_magic") != magic or p.get("opened") is None or p["opened"] < since:
                continue
            if infer_group(str(p.get("symbol") or "")) != FORMULA1_GROUP:
                continue
            if not p.get("is_closed"):
                n_open += 1
                continue
            trades.append({"closed": p["closed"], "net": round(float(p["net"]), 2),
                           "costs": round(float(p.get("commission") or 0.0), 2), "symbol": str(p.get("symbol") or ""),
                           "ticket": int(p.get("position") or 0), "kind": "real"})
    return {"stats": stats(trades), "open": n_open, "since": since}


def formula1_trades(tactics=()) -> str:
    """Which of Trend & Breakout's trades are Formula 1's real ones, in plain
    words: "its minor-pair trades"; with tnb.live_tactics set "its momentum
    continuation trades on minor pairs" (10 Oct review: the row's badge
    already said so, the line beside it did not)."""
    tactics = tuple(tactics or ())
    if not tactics:
        return "its minor-pair trades"
    return f"its {_join([str(t).lower().replace('_', ' ') for t in tactics])} trades on minor pairs"


def _window_words(seconds: float) -> str:
    """900 -> "15 minutes", 90 -> "90 seconds" (as the modes tool says it)."""
    m = float(seconds) / 60.0
    if seconds < 60 or abs(m - round(m)) > 1e-9:
        return f"{float(seconds):g} seconds"
    return f"{m:g} minute{'' if m == 1 else 's'}"


def formula1_in_force(data_dir, wanted: Optional[dict] = None) -> str:
    """What config.json now holds for Formula 1 beyond 9 Oct's settings, in
    one line: only some approaches real (tnb.live_tactics), the
    top-opportunity size off (risk.top_size_enabled), a news window other
    than the 90 seconds of 9 Oct (news.blackout_before_seconds), the
    daily-loss stop on real money only (risk.daily_loss_counts_practice
    false). '' when none of these - and nothing the plan asks for is
    missing - so the page shows nothing new. 10 Oct review: Aaron's answers
    are applied by config alone, and the plan box printed docs/plan.json
    word for word beside them.

    ``wanted``: the plan's own settings for Formula 1 (docs/plan.json
    "focus" -> "settings": live_tactics, top_size_enabled,
    news_before_minutes, daily_loss_counts_practice). 10 Oct, Aaron's
    answers: the plan now says them, so a setting the plan names that
    config.json here does not hold is said plainly after the rest (a
    one-time step that failed, or a choice changed by hand since) - the box
    never shows the plan's words as if they were in force. Never raises."""
    try:
        live = tnb_live_setting(data_dir)
        if FORMULA1_GROUP not in live["groups"]:
            return ""
        raw = _read_json(Path(data_dir) / "config.json") or {}
        from ..config import NewsConfig, RiskConfig, _build
        rc = _build(RiskConfig, raw.get("risk") if isinstance(raw.get("risk"), dict) else {})
        nc = _build(NewsConfig, raw.get("news") if isinstance(raw.get("news"), dict) else {})
        counts_practice = not (isinstance(raw.get("risk"), dict)
                               and raw["risk"].get("daily_loss_counts_practice") is False)  # as Config.load reads it
        window = float(nc.blackout_before_seconds)
        bits = []
        if live.get("tactics"):
            bits.append(f"only {formula1_trades(live['tactics'])} use real money (tnb.live_tactics)")
        if not rc.top_size_enabled:
            # (the money a trade itself is not read here: only Trend & Breakout's sizing reads it)
            bits.append("the top-opportunity size is OFF, so every trade risks the normal amount, never the "
                        "bigger size (risk.top_size_enabled)")
        if abs(window - float(NewsConfig().blackout_before_seconds)) > 1e-9:
            bits.append(f"no new Trend & Breakout entry in the {_window_words(window)} before a high-importance "
                        f"release on either currency of the pair (news.blackout_before_seconds)")
        if not counts_practice:
            bits.append("the daily-loss stop counts real money only, its limit unchanged "
                        "(risk.daily_loss_counts_practice)")
        missing = []
        w = wanted if isinstance(wanted, dict) else {}
        if w.get("live_tactics"):
            from ..engine.tactics import approach_of
            want_t = tuple(dict.fromkeys(approach_of(x) for x in w["live_tactics"] if isinstance(x, str)))
            if want_t and set(want_t) != set(live.get("tactics") or ()):
                missing.append(f"only {formula1_trades(want_t)} with real money (tnb.live_tactics)")
        if w.get("top_size_enabled") is False and rc.top_size_enabled:
            missing.append("the top-opportunity size OFF (risk.top_size_enabled)")
        if isinstance(w.get("news_before_minutes"), (int, float)) and not isinstance(w["news_before_minutes"], bool) \
                and abs(float(w["news_before_minutes"]) * 60.0 - window) > 1e-6:
            missing.append(f"no new entry in the {_window_words(float(w['news_before_minutes']) * 60.0)} before a "
                           f"high-importance release (news.blackout_before_seconds is {window:g} here)")
        if w.get("daily_loss_counts_practice") is False and counts_practice:
            missing.append("the daily-loss stop on real money only (risk.daily_loss_counts_practice)")
        out = "; ".join(bits)
        if missing:
            gap = "not in config.json here yet, though the plan says so: " + "; ".join(missing)
            out = f"{out}. {gap[0].upper()}{gap[1:]}" if out else f"none of the plan's choices - {gap}"
        return out
    except Exception:                                    # never a reason to fail the page
        return ""


def formula1_judgement(f1: dict, judge_after: int = FORMULA1_JUDGE_AFTER, live_tactics=()) -> str:
    """What the plan's rule says of Formula 1's live trades so far, in one
    sentence: keep and grow it after ``judge_after`` live trades if it is
    positive after costs with more up days than down, otherwise back to PAPER.
    ``live_tactics``: tnb.live_tactics, so it names the trades that are real."""
    s = (f1 or {}).get("stats") or stats([])
    n = int(s.get("trades") or 0)
    # 10 Oct: never a count that was not made - MetaTrader unreadable, or no live start time on record
    if not (f1 or {}).get("readable", True):
        so_far = "they cannot be counted just now: MetaTrader's records could not be read"
    elif (f1 or {}).get("since") is None:
        so_far = "they are not counted yet: its live start time is not recorded"
    else:
        so_far = f"{n} so far"
    if n < judge_after or so_far != f"{n} so far":
        return (f"Formula 1 - {formula1_trades(live_tactics)} - is LIVE: judge it after {judge_after} live trades "
                f"({so_far}). Keep it and grow it only if it is positive after costs with more up days than down; "
                f"otherwise back to PAPER and test the next candidate. The rest stays on PAPER.")
    passes = float(s.get("net") or 0.0) > 0 and int(s.get("up_days") or 0) > int(s.get("down_days") or 0)
    facts = (f"{_gbp(s['net'])} after costs over {_n(n, 'live trade')}, {s['up_days']} days up and "
             f"{s['down_days']} down")
    return (f"Formula 1 has its {judge_after} live trades - time to judge it: {facts}. "
            + ("By the plan's rule that is a keep: keep it and grow it." if passes else
               "By the plan's rule it has not earned its place: back to PAPER, and test the next candidate."))


def tnb_history(data_dir, reset: dt.datetime, real_by_ticket: dict, record=None,
                since_plan: Optional[dt.datetime] = None) -> dict:
    """Trend & Breakout's own journal (data/journal.sqlite), read-only:
    {before_reset: its REAL trades that closed before the account reset
    (the journal's booked money - the commission charged on the way in is
    not in it), segment: its real trades in the Formula 1 segment, all
    dates, forward: the segment's trades since the plan (real and practice
    apart), first_day}. Each trade's money is MetaTrader's full figure
    where the account's records have its position (``real_by_ticket``),
    its paper record's for a practice trade, else the journal's."""
    out = {"before_reset": [], "segment": [], "forward_real": [], "forward_practice": [], "first_day": None}
    if not data_dir:
        return out
    data = Path(data_dir)
    from .attribution import _rows_ro
    rows = _rows_ro(data / "journal.sqlite", "SELECT ticket, symbol, tactic, regime, closed_utc, pnl_money FROM trades "
                                             "WHERE closed_utc IS NOT NULL ORDER BY closed_utc ASC LIMIT 50000", ())
    if not rows:
        return out
    if record is None:
        from .standing import TnbPaperRecord
        record = TnbPaperRecord(data)
    paper = {int(p["ticket"]): p for p in ([] if record.error else record.positions())}
    try:
        in_segment = _segment_filter()
    except Exception as exc:                              # never a reason to fail the page
        log.warning("Formula 1 segment: %s", exc)
        in_segment = None
    reset = to_utc(reset)
    for r in rows:
        t = _when(r.get("closed_utc"))
        if t is None:
            continue
        ticket = int(r.get("ticket") or 0)
        if ticket in paper:
            p = paper[ticket]
            trade = {"closed": to_utc(p["closed"]), "net": round(float(p["net"]), 2),
                     "costs": round(float(p.get("commission") or 0.0), 2), "symbol": p.get("symbol") or "",
                     "ticket": ticket, "kind": "practice"}
        elif ticket in real_by_ticket:
            trade = dict(real_by_ticket[ticket])
        else:
            trade = {"closed": t, "net": round(float(r.get("pnl_money") or 0.0), 2), "costs": None,
                     "symbol": str(r.get("symbol") or ""), "ticket": ticket, "kind": "journal"}
        if out["first_day"] is None and trade["kind"] != "practice":
            out["first_day"] = uk_date(trade["closed"])
        if trade["kind"] == "journal" and trade["closed"] < reset:
            out["before_reset"].append(trade)
        if in_segment is not None and in_segment(r):
            if trade["kind"] != "practice":
                out["segment"].append(trade)
            if since_plan is not None and trade["closed"] >= to_utc(since_plan):
                out["forward_practice" if trade["kind"] == "practice" else "forward_real"].append(trade)
    return out


# ------------------------------------------------------- one bot's verdict --
def _without_open_fee(s: dict) -> str:
    """How a sample with journal trades in it reads: 'without the commission
    charged when each trade opened' ('' when it has none)."""
    j, n = int(s.get("journal") or 0), int(s.get("trades") or 0)
    if not j:
        return ""
    return f"without {OPEN_FEE}" if j >= n else f"without the commission charged when {j} of them opened"


def _facts(s: dict, money: bool = True) -> str:
    """One line of a sample's numbers: trades, days, net, a trade, profit factor, wins and losses, costs.
    Trend & Breakout's journal trades never read "after costs": their money leaves out ``OPEN_FEE``."""
    if not s.get("trades"):
        return "no trades"
    partial = _without_open_fee(s)
    bits = [f"{_n(s['trades'], 'trade')} over {_n(s['days'], 'UK day')} ({s['up_days']} up, {s['down_days']} down)"]
    if money:
        bits.append(f"{_gbp(s['net'])} {partial}" if partial else f"{_gbp(s['net'])} after costs")
    bits.append(f"{_gbp(s['expectancy'])} a trade")
    if s.get("profit_factor") is not None:
        bits.append(f"profit factor {s['profit_factor']:.2f}")
    elif s.get("wins"):
        bits.append("no losing trade")
    w = [f"{s['wins']} won"]
    if s.get("avg_win") is not None:
        w.append(f"wins averaged {_gbp(s['avg_win'], False)}")
    if s.get("avg_loss") is not None:
        w.append(f"losses {_gbp(s['avg_loss'], False)}")
    bits.append(", ".join(w))
    if s.get("costs") is not None and s["costs"] >= 0.005:
        c = f"costs {_gbp(s['costs'], False)}"
        if s.get("costs_share") is not None:
            c += f" ({s['costs_share']:.0%} of the {_gbp(s['before_costs'], False)} it made before costs)"
        elif s.get("before_costs") is not None and s["before_costs"] <= 0:
            c += f" (on top of {_gbp(abs(s['before_costs']), False)} lost before costs)"
        bits.append(c)
    elif s.get("costs") is None and not partial:
        bits.append("costs not in the record")
    return "; ".join(bits)


def _count(n: int, split: Optional[tuple] = None) -> str:
    """'4 trades (2 real, 2 practice)' when a record mixes both, '3 practice
    trades' when it is all practice, else '4 trades'."""
    if split:
        r, p = int(split[0] or 0), int(split[1] or 0)
        if r and p:
            return f"{_n(n, 'trade')} ({r:,} real, {p:,} practice)"
        if p and not r:
            return _n(n, "practice trade")
    return _n(n, "trade")


def _span(s: dict, fallback: str = "") -> str:
    a, b = s.get("first_day"), s.get("last_day")
    if a is None:
        return fallback
    return _day(a) if a == b else f"{_day(a)} to {_day(b)}"


def in_period(view: Optional[dict]) -> str:
    """How the chosen period reads in a sentence: 'today', 'this week', 'in total', 'on Thu 08 Oct 2026'."""
    v = view or {}
    key, label = str(v.get("key") or "today"), str(v.get("label") or "Today")
    if key == "custom":
        return ("from " + label) if " to " in label else ("on " + label)
    if key == "total":
        return "in total"
    return label.lower() if key in ("today", "yesterday") or label.lower().startswith("this") else "in the " + label.lower()


def view_made(view: Optional[dict], bot: str) -> Optional[float]:
    """The bot's money in a ``standing.period_view`` - the very figure the
    page's rows show (for Total: balance-based, its deals since the reset).
    None when the view does not have it."""
    b = ((view or {}).get("bots") or {}).get(bot)
    if not isinstance(b, dict) or b.get("made") is None:
        return None
    return round(float(b["made"]), 2)


def money_gap(view: Optional[dict], magic: int, made: Optional[float], closed: dict, deals=None) -> str:
    """One line on why the view's figure for the bot (``made``) differs from
    its closed trades added up (``closed``, ``stats`` of ``real_trades``),
    from the same deal rows; '' when they agree. Only Total can differ: it
    counts every deal booked since the reset (the costs already charged on
    trades still open), and leaves out what was charged before the reset on
    trades that closed after it. ``deals``: the account's deal rows the view
    was worked out from, to count the trades still open."""
    if made is None or not view:
        return ""
    net = round(float(closed.get("net") or 0.0), 2)
    diff = round(made - net, 2)
    if abs(diff) < 0.005:
        return ""
    start = _when(view.get("start"))
    end = _when(view.get("end"))
    from .standing import bot_view
    mine = bot_view(view, int(magic))["closed"]
    pre, n_pre = 0.0, 0
    for p in mine:
        early = [float(d.get("profit") or 0.0) for d in p.get("deals") or ()
                 if start is not None and _when(d.get("time")) is not None and _when(d.get("time")) < start]
        if early:
            pre += sum(early)
            n_pre += 1
    pre = round(pre, 2)
    still_open, n_open, other = round(diff + pre, 2), 0, 0.0
    if deals is not None and start is not None:
        from .standing import positions_of
        ids = {int(p.get("position") or 0) for p in mine}
        closed_ids = {int(p.get("position") or 0) for p in positions_of(deals) if p["is_closed"]}
        rows = [r for r in deals if r.get("bot_magic") == magic and _when(r.get("time")) is not None
                and start <= _when(r["time"]) and (end is None or _when(r["time"]) < end)
                and int(r.get("position") or 0) not in ids and int(r.get("position") or 0) not in closed_ids]
        still_open = round(sum(float(r.get("profit") or 0.0) for r in rows), 2)
        n_open = len({int(r.get("position") or 0) for r in rows})
        other = round(diff - (still_open - pre), 2)
    n = int(closed.get("trades") or 0)
    also = "also " if n else ""
    bits = []
    if abs(still_open) >= 0.005:
        on = f"{_n(n_open, 'trade')} still open" if n_open else "trades still open"
        bits.append(f"{also}counts {_gbp(-still_open, False)} of costs already charged on {on}" if still_open < 0
                    else f"{also}counts {_gbp(still_open)} already booked on {on}")
    if abs(pre) >= 0.005:
        on = f"{_n(n_pre, 'trade')} that closed after it"
        bits.append(f"leaves out {_gbp(-pre, False)} of costs charged before the reset on {on}" if pre < 0
                    else f"leaves out {_gbp(pre)} booked before the reset on {on}")
    if abs(other) >= 0.005:
        bits.append(f"{also}counts {_gbp(other)} of other entries for its magic number")
    if not bits:
        return ""
    lead = (f"Its {_n(n, 'closed trade')} on their own come to {_gbp(net)}; the Overall figure ({_gbp(made)})"
            if n else f"No trade of it has closed; the Overall figure ({_gbp(made)})")
    return f"{lead} {_join(bits)}."


def what_happened(mode: str, phrase: str, real: dict, practice: dict, judged: dict,
                  research: Optional[dict], real_ok: bool = True, real_money: Optional[float] = None,
                  real_note: str = "", split: Optional[tuple] = None, mixed: bool = False) -> str:
    """Two or three plain sentences built from the numbers: the chosen
    period (real money for a LIVE bot, practice for a PAPER one), what the
    costs did, and the longer record or its research. ``real_ok`` False:
    MetaTrader's records could not be read, and a LIVE bot says so rather
    than "no trades". ``real_money``: the period's real money as the page's
    rows show it (``view_made``), used for the money in place of the closed
    trades added up, with ``real_note`` (``money_gap``) saying why they
    differ. ``split``: (real, practice) trades in ``judged``. ``mixed``:
    part LIVE (Trend & Breakout's Formula 1, 9 Oct), read as LIVE with its
    real trades called real, so a day of practice alone never reads as if
    nothing was traded."""
    live = str(mode or "").upper() == "LIVE"
    off = str(mode or "").upper() == "OFF"
    if live:
        main, kind = real, ("real " if mixed else "")
    elif off or (real["trades"] and not practice["trades"]):
        main, kind = real, "real "                      # a real trade left from its LIVE days
    else:
        main, kind = practice, "practice "
    real_net = real["net"] if real_money is None else real_money
    out = []
    if live and not real_ok:
        out.append(f"MetaTrader's records could not be read just now, so its real trades {phrase} are not shown "
                   f"rather than shown wrong.")
    elif off and not main["trades"]:
        out.append(f"No trades {phrase}: it is switched off.")
        if real_note:
            out.append(real_note)
    elif not main["trades"]:
        out.append(f"No {kind}trades {phrase}.")
        if kind != "practice " and real_note:
            out.append(real_note)
    else:
        tag = "" if mixed else {"practice ": " (practice, not money)", "real ": " - left from its LIVE days"}.get(kind, "")
        out.append(f"{_n(main['trades'], kind + 'trade')} {phrase}, {main['wins']} won, "
                   f"{_gbp(real_net if kind != 'practice ' else main['net'])} after costs{tag}.")
        if main["wins"] and main.get("avg_loss") is not None:
            s2 = f"Wins averaged {_gbp(main['avg_win'], False)}, losses {_gbp(main['avg_loss'], False)}"
        elif main["wins"]:
            s2 = f"No losing trade; wins averaged {_gbp(main['avg_win'], False)}"
        elif main.get("avg_loss") is not None:
            s2 = f"None won; losses averaged {_gbp(main['avg_loss'], False)}"
        else:
            s2 = "Every trade closed flat"
        c, before = main.get("costs"), main.get("before_costs")
        if c is not None and c >= 0.005 and before is not None:
            if before > 0 and main["net"] < 0:
                s2 += (f", and costs took {_gbp(c, False)} - more than the {_gbp(before, False)} it made before costs: "
                       f"the costs are bigger than its whole edge")
            elif before > 0:
                s2 += f", and costs took {_gbp(c, False)} of the {_gbp(before, False)} it made before costs"
            else:
                s2 += f", and costs added {_gbp(c, False)} to the {_gbp(abs(before), False)} it lost before costs"
        out.append(s2 + ".")
        if kind != "practice " and real_note:
            out.append(real_note)
    if live and practice["trades"]:
        out.append(f"Also {_n(practice['trades'], 'practice trade')} {phrase} ({_gbp(practice['net'])}, not money).")
    elif not live and kind == "practice " and real["trades"]:
        out.append(f"Also {_n(real['trades'], 'real trade')} left from its LIVE days {phrase} ({_gbp(real_net)})."
                   + (f" {real_note}" if real_note else ""))
    elif not live and kind == "practice " and real_note:
        out.append(real_note)
    if research and not research.get("synthetic") and research.get("trades"):
        out.append(_research_line(research))
    elif judged["trades"] and judged["trades"] != main["trades"]:
        mixed = bool(split and split[0] and split[1])
        out.append(f"Its whole record{', real and practice together' if mixed else ''}: {_count(judged['trades'], split)} "
                   f"over {_n(judged['days'], 'UK day')} ({judged['up_days']} up, {judged['down_days']} down), "
                   f"{_gbp(judged['expectancy'])} a trade after costs.")
    return " ".join(out)


def headline(code: str, s: dict, research: Optional[dict], blocked: str, split: Optional[tuple] = None) -> str:
    """The verdict in one line. ``split``: (real, practice) trades in ``s``,
    so a record that mixes them says how many of each."""
    n = int(s.get("trades") or 0)
    cnt = _count(n, split)
    if code == NO_EDGE:
        if research_counts(research) and research.get("code") == NO_EDGE:
            unit = research.get("unit")
            exp = research.get("expectancy")
            days = research.get("days")
            lead = f"No edge: its own {str(days) + '-day ' if days else ''}test on real past prices"
            over = f"over {_n(int(research['trades']), 'trade')}"
            if exp is None:
                return f"{lead} found no edge {over}."
            exp = float(exp)
            amount = _gbp(abs(exp), False) if unit == "£" else f"{abs(exp):.2f} pips"
            if exp < 0:                                  # "lost 1.33 pips a trade", never "lost -1.33"
                return f"{lead} lost {amount} a trade {over}."
            return f"{lead} made {_gbp(exp) if unit == '£' else f'{exp:+.2f} pips'} a trade {over}, but found no edge."
        return f"No edge: over {cnt} it {_pf_words(s.get('profit_factor'))} ({_gbp(s['expectancy'])} a trade)."
    if code == PROVEN:
        return (f"Proven: {_gbp(s['expectancy'])} a trade after costs over {cnt}, "
                f"{s['up_days']} of {s['days']} days up.")
    if code == PROMISING:
        return (f"Promising: {_gbp(s['expectancy'])} a trade after costs over {cnt} - needs "
                f"{_join(_missing_for_proven(s))} to count as proven.")
    if code == BREAK_EVEN:
        return f"Break-even: {_gbp(s['expectancy'])} a trade after costs over {cnt} - no edge as a whole."
    if code == WAITING:
        if blocked == SWITCHED_OFF:                      # 9 Oct: the Rider is off by choice, not waiting to trade
            return "Switched off: it places no trades, so there is nothing new to judge."
        return f"Waiting: {blocked} - nothing new to judge until it can trade."
    if not n:
        return f"Too early to tell: no trades yet - it needs {MIN_TRADES} over {MIN_DAYS} UK days."
    if n >= MIN_TRADES:
        return (f"Too early to tell: {cnt}, but over only {_n(int(s.get('days') or 0), 'UK day')} - "
                f"it needs {MIN_DAYS} days.")
    return f"Too early to tell: {cnt} so far - it needs {MIN_TRADES} over {MIN_DAYS} UK days."


def recommendation(code: str, mode: str, label: str, s: dict, blocked: str, bot: str = "",
                   segment: Optional[dict] = None, split: Optional[tuple] = None) -> str:
    """What to do about it, in one or two sentences. ``split``: (real,
    practice) trades in ``s``."""
    live = str(mode or "").upper() == "LIVE"
    off = str(mode or "").upper() == "OFF"
    n, days = int(s.get("trades") or 0), int(s.get("days") or 0)
    if code == NO_EDGE and off:
        return "Leave it off - it is binned: do not put money on it."
    if code == NO_EDGE:
        return ("Take it off live trading - put it back on PAPER (or bin it). Every live trade pays costs for an "
                "edge it does not have." if live else
                "Bin it, or leave it on PAPER only as a record - do not put money on it.")
    if code == PROVEN:
        return ("Keep it and grow it: raise the size one step at a time while it stays positive after costs." if live
                else "It has earned a live trial: put it live at the smallest size and judge it again after 20 live "
                     "trades.")
    if code == PROMISING:
        need = _join(_missing_for_proven(s))
        return (f"Keep it at its small size and keep testing: it needs {need} to count as proven." if live else
                f"Keep testing on PAPER: it needs {need} before it earns a live trial.")
    if code == BREAK_EVEN:
        part = ""
        if bot == TNB and segment and segment.get("trades"):
            # what its own record says of the Formula 1 candidate - never assumed; its journal trades say
            # that the commission charged when they opened is not in their money
            partial = _without_open_fee(segment)
            figs = f"{_gbp(segment['net'])} over {_n(segment['trades'], 'real trade')}" + (f", {partial}" if partial else "")
            if segment.get("net", 0) > 0:
                part = f" Its {SEGMENT_NAME} is the part that pays on its record ({figs}) - that is Formula 1."
            else:
                part = (f" Its {SEGMENT_NAME} (the Formula 1 candidate) is not paying on its record either "
                        f"({figs}).")
        return (("Keep it small or move it to PAPER: as a whole it does not pay." if live else
                 "Keep it on PAPER: as a whole it does not pay - look inside it for the part that does.") + part)
    if code == WAITING:
        if blocked == SWITCHED_OFF:
            return "Nothing to judge while it is switched off."
        return f"Nothing to judge until it can trade ({blocked})."
    need = f"{MIN_TRADES} trades over {MIN_DAYS} UK days"
    have = f"{_count(n, split)} over {_n(days, 'UK day')}" if n else "no trades yet"
    return (f"Keep testing at the smallest size: {have} is too few to judge - it needs {need}." if live else
            f"Keep testing on PAPER: {have} is too few to judge - it needs {need}.")


def bot_verdict(bot: str, label: str, mode: str, magic: int, *, sel: Optional[dict], tot: Optional[dict],
                data_dir, now: dt.datetime, reset: Optional[dt.datetime] = None, real_readable: bool = True,
                record=None, since_plan: Optional[dt.datetime] = None, period: Optional[dict] = None,
                deals=None, live_groups=(), formula1: Optional[dict] = None,
                judge_after: int = FORMULA1_JUDGE_AFTER, live_tactics=()) -> dict:
    """One bot: its verdict, a one-line headline, "What happened" in the
    chosen period (``sel``, a ``standing.period_view``), a recommendation,
    and the evidence with its source and dates. ``tot`` is the Total view
    (everything since the reset); ``real_readable`` False when MetaTrader's
    records for its magic could not be read (then no real figure is used or
    shown, and the evidence says so). ``period``: the chosen period's
    {key, label, start, end} when there is no ``sel`` (MetaTrader not read
    yet), so its practice is still that period's. Real money in a period is
    the view's own figure for the bot (``view_made``: Total is the
    balance-based Overall), the closed trades giving the counts and
    averages; ``deals`` (the account's deal rows) count the trades still
    open when the two differ. The verdict
    is judged only on trades whose full costs are on record. Never raises
    for a missing file.

    Trend & Breakout on PAPER with ``live_groups`` (9 Oct: its minor currency
    pairs LIVE, Formula 1) is part LIVE: its mode reads "LIVE: minor pairs
    (Formula 1) - rest PAPER" (``mode_words``), What happened leads with its
    real money (practice after it, labelled), and with ``formula1``
    (``formula1_live``) the recommendation is the plan's judgement of
    Formula 1 after ``judge_after`` live trades. The verdict itself is the
    same rule on the same trades (its real ones and its labelled practice).
    ``live_tactics`` (10 Oct, tnb.live_tactics): only those approaches trade
    for real there, and the words say so."""
    now = to_utc(now)
    mixed = bot == TNB and bool(live_groups) and str(mode or "").upper() != "LIVE"
    reset = to_utc(reset) if reset is not None else (_when((tot or {}).get("start")) or uk_midnight(uk_date(now)))
    tomorrow = uk_midnight(uk_date(now) + dt.timedelta(days=1))
    pv = sel if sel is not None else (period or None)
    p_start = _when((pv or {}).get("start")) or uk_midnight(uk_date(now))
    p_end = _when((pv or {}).get("end")) or tomorrow
    if bot == TNB and record is None and data_dir:
        from .standing import TnbPaperRecord
        record = TnbPaperRecord(data_dir)
    real_ok = real_readable and sel is not None and tot is not None
    real_p = stats(real_trades(sel, magic) if real_ok else [])
    real_all_trades = real_trades(tot, magic) if real_ok else []
    real_all = stats(real_all_trades)
    prac_p = stats(practice_trades(data_dir, bot, p_start, p_end, record))
    prac_all_trades = practice_trades(data_dir, bot, reset, max(tomorrow, p_end), record)
    prac_all = stats(prac_all_trades)
    research = read_research(data_dir, bot)
    # Financial Ian's research counts only when it was run on the market it trades now
    elsewhere = (research_elsewhere(research, ian_market(data_dir))
                 if bot == "financial_ian" and research and not research.get("synthetic") else "")
    research_used = None if elsewhere else research
    history = None
    # judged only on trades whose full costs are on record: MetaTrader's (since the reset) and practice.
    # Trend & Breakout's journal from before the reset leaves out the commission charged when each trade
    # opened, so it is evidence, shown apart and labelled, never part of the verdict
    judged_trades = real_all_trades + prac_all_trades
    if bot == TNB:
        try:
            history = tnb_history(data_dir, reset, {t["ticket"]: t for t in real_all_trades}, record, since_plan)
        except Exception as exc:                          # never a reason to fail the page
            log.warning("Trend & Breakout's journal: %s", exc)
            history = None
    judged = stats(judged_trades)
    split = (real_all["trades"], prac_all["trades"])
    blocked = cannot_trade(data_dir, bot, mode)
    if real_ok:
        code, why = decide(judged, research_used, blocked)
    else:
        # MetaTrader's records could not be read: never a verdict from half a record - only its research
        # (which does not depend on them) or the fact that it cannot trade can still speak
        code, why = decide(stats([]), research_used, blocked)
        if code == NOT_ENOUGH:
            why = "MetaTrader's records could not be read just now"
    action, colour = VERDICTS[code]
    seg = stats(history["segment"]) if history else None
    phrase = in_period(pv)
    started = _day(uk_date(reset))
    # real money: the view's own figure for the bot, as the page's rows show it (Total: balance-based)
    made_p = view_made(sel, bot) if real_ok else None
    made_all = view_made(tot, bot) if real_ok else None
    gap_p = money_gap(sel, magic, made_p, real_p, deals) if real_ok else ""
    gap_all = money_gap(tot, magic, made_all, real_all, deals) if real_ok else ""
    evidence = []
    if judged["trades"]:
        src = ["MetaTrader's records (real money)"] if real_all["trades"] else []
        if prac_all["trades"]:
            src.append("its own paper record (practice)")
        facts = _facts(judged, money=False)
        if history and history["before_reset"]:
            facts += (f". Its journal trades from before the reset are shown below but not judged: {OPEN_FEE} "
                      f"is not in them")
        evidence.append({"label": "What the verdict is judged on - every trade whose full costs are on record, "
                                  "each after its own costs",
                         "facts": facts, "source": _join(src) or "its own records", "period": _span(judged)})
    if not real_ok and str(mode).upper() == "LIVE":
        evidence.append({"label": "Real money", "facts": "MetaTrader's records for it could not be read just now - "
                                                         "nothing is shown rather than a guess",
                         "source": "MetaTrader", "period": ""})
    elif real_all["trades"] or (made_all is not None and abs(made_all) >= 0.005):
        if made_all is None:
            facts = _facts(real_all)
        else:
            facts = f"Overall since {started}: {_gbp(made_all)}"
            if real_all["trades"]:
                facts += f". Its closed trades: {_facts(real_all, money=False)}"
            facts += "." + (f" {gap_all}" if gap_all else "")
        evidence.append({"label": "Real money", "facts": facts,
                         "source": f"MetaTrader's records for magic number {magic}: each closed trade in full, "
                                   f"after commission; Overall is every deal it booked since the reset, as on the "
                                   f"main page",
                         "period": f"since {started}" + (f" - its trades closed {_span(real_all)}"
                                                         if real_all["trades"] else "")})
    if prac_all["trades"]:
        rec_name = ("data/tnb_paper.sqlite" if bot == TNB else
                    f"data/{OWN_PRACTICE.get(bot, ('its own record',))[0]}")
        evidence.append({"label": "Practice (PAPER - simulated orders on real prices, not money)",
                         "facts": _facts(prac_all), "source": f"its own paper record ({rec_name})",
                         "period": f"since {started} - its trades closed {_span(prac_all)}"})
    if history and history["before_reset"]:
        hs = stats(history["before_reset"])
        evidence.append({"label": "Before the account reset (real trades)", "facts": _facts(hs),
                         "source": f"its own journal (data/journal.sqlite), booked when each trade closed: {OPEN_FEE} "
                                   f"is not in it, so the real figure is lower - not used for the verdict",
                         "period": _span(hs)})
    if seg is not None and seg["trades"]:
        evidence.append({"label": f"The Formula 1 candidate - its {SEGMENT_NAME}, real trades",
                         "facts": _facts(seg),
                         "source": ("its own journal for the trades before the reset (booked when each closed: "
                                    f"{OPEN_FEE} is not in it), with MetaTrader's full figure for every trade since"
                                    if seg.get("journal") else
                                    "MetaTrader's full figure for each trade, found through its own journal"),
                         "period": _span(seg)})
        if since_plan is not None:
            fr, fp = stats(history["forward_real"]), stats(history["forward_practice"])
            evidence.append({"label": "That part since the plan was written (it has to prove itself forward)",
                             "facts": (f"real: {_facts(fr)}" if fr["trades"] else "real: no trades")
                                      + (f" | practice: {_facts(fp)}" if fp["trades"] else " | practice: no trades"),
                             "source": "MetaTrader (real) and its paper record (practice)",
                             "period": f"since {_day(uk_date(since_plan))}"})
    if research:
        if research.get("error"):
            facts = f"{research['error']}"
        elif research.get("synthetic"):
            facts = "a SYNTHETIC test run (made-up prices) - not evidence, never used for the verdict"
        else:
            facts = _research_line(research) + (f" Its verdict: {research.get('code')}." if research.get("code") else "")
            if elsewhere:
                facts += f" Not used for the verdict: {elsewhere}."
            elif int(research.get("trades") or 0) < MIN_TRADES:
                facts += f" Fewer than {MIN_TRADES} trades, so not used for the verdict."
        evidence.append({"label": "Its own research on real past prices", "facts": facts,
                         "source": research.get("source", ""), "period": research.get("period", "")})
    if blocked:
        evidence.append({"label": "Can it trade?", "facts": f"No - {blocked}", "source": "its own settings and status file",
                         "period": "now"})
    if mixed and formula1 is not None:
        f1s = formula1.get("stats") or stats([])
        since = formula1.get("since")
        when = f"{_day(uk_date(since))} {since.astimezone(UK_TZ).strftime('%H:%M')} UK" if since else ""
        if not formula1.get("readable", True):
            facts = "MetaTrader's records for it could not be read just now - nothing is shown rather than a guess"
        elif since is None:
            facts = "its live start time is not recorded (tnb.live_since_utc), so its live trades are not counted yet"
        else:
            k = int(formula1.get("open") or 0)
            facts = (_facts(f1s) if f1s["trades"] else "no live trades closed yet") + (
                f"; {k} more still open (not counted until {'it closes' if k == 1 else 'they close'})"
                if k else "") + (f". {f1s['trades']} of {judge_after} toward the judgement"
                                                    if f1s["trades"] < judge_after else
                                                    f". Its {judge_after} live trades are in: time to judge it")
        evidence.insert(0, {"label": f"Formula 1 LIVE - {formula1_trades(live_tactics)} with real money",
                            "facts": facts,
                            "source": f"MetaTrader's records for magic number {magic}: positions on minor currency pairs "
                                      f"opened since it went live, each in full after commission - never its journal",
                            "period": f"since {when}" if when else ""})
    head = (headline(code, judged, research_used, blocked, split) if real_ok or code != NOT_ENOUGH else
            "No verdict just now: MetaTrader's records could not be read - it comes back on its own.")
    if mixed and (real_ok or code != NOT_ENOUGH):
        # 10 Oct: the chip and this line judge Trend & Breakout as a whole (its practice and its real trades,
        # earlier ones included), next to a pill that says Formula 1 is LIVE - so they say what they cover
        head = f"As a whole (practice and real together; Formula 1 is judged on its own below): {head}"
        action = f"{action} for Trend & Breakout as a whole - Formula 1 is judged on its own"
    return {"bot": bot, "label": label, "mode": str(mode or ""), "magic": magic, "code": code, "why": why,
            "action": action, "colour": colour,
            "mode_words": live_words(live_groups, live_tactics) if mixed else str(mode or ""),
            "live_kinds": live_kinds(live_groups, live_tactics) if mixed else "",
            "live_groups": tuple(live_groups) if mixed else (),
            "formula1": formula1 if mixed else None,
            "headline": head,
            # part LIVE: What happened leads with its real money, its practice after it (labelled)
            "what_happened": what_happened("LIVE" if mixed else mode, phrase, real_p, prac_p, judged, research_used,
                                           real_ok, real_money=made_p, real_note=gap_p, split=split, mixed=mixed),
            "recommendation": (formula1_judgement(formula1, judge_after, live_tactics)
                               if mixed and formula1 is not None else
                               recommendation(code, mode, label, judged, blocked, bot, seg, split)),
            "evidence": evidence, "rule": RULE_TEXT,
            "period": {"real": real_p, "practice": prac_p, "phrase": phrase, "made": made_p},
            "real": real_all, "practice": prac_all, "judged": judged, "research": research, "segment": seg,
            "made": made_all,
            "forward": ({"real": stats(history["forward_real"]), "practice": stats(history["forward_practice"]),
                         "since": since_plan} if history and since_plan is not None else None)}


def all_verdicts(sd: Optional[dict], sel: Optional[dict], tot: Optional[dict], data_dir, now: dt.datetime,
                 plan: Optional[dict] = None, period: Optional[dict] = None, deals=None) -> dict:
    """{bot id: ``bot_verdict``} for the six active bots, in the page's
    order. ``sd`` is the account's standing (each bot's mode and magic);
    without it the bots are listed with their usual magic
    numbers and their practice alone (for ``period``, the chosen period's
    bounds). ``deals``: the deal rows ``sel`` and ``tot`` were worked out
    from, to name a bot's trades still open. Never raises: a bot whose
    verdict fails says so."""
    from .standing import ACTIVE_BOTS, TnbPaperRecord
    from .dashboard import DEFAULT_MAGICS
    sd = sd or {}
    by_id = {b.get("id"): b for b in sd.get("bots") or () if isinstance(b, dict)}
    unknown = set(sd.get("unknown_magics") or ())
    reset = _when(sd.get("start")) or _when((tot or {}).get("start"))
    since_plan = None
    try:
        if plan and plan.get("updated"):
            since_plan = uk_midnight(dt.date.fromisoformat(str(plan["updated"])[:10]))
    except ValueError:
        since_plan = None
    record = None
    if data_dir:
        try:
            record = TnbPaperRecord(data_dir)
        except Exception:
            record = None
    live = tnb_live_setting(data_dir)
    focus = (plan or {}).get("focus") if isinstance((plan or {}).get("focus"), dict) else {}
    try:
        judge_after = int(focus.get("judge_after") or FORMULA1_JUDGE_AFTER)
    except (TypeError, ValueError):
        judge_after = FORMULA1_JUDGE_AFTER
    out = {}
    for bid, label in ACTIVE_BOTS:
        b = by_id.get(bid) or {}
        magic = int(b.get("magic") or DEFAULT_MAGICS.get(bid, 0))
        mode = str(b.get("mode") or "")
        readable = bool(b) and magic not in unknown and b.get("made") is not None
        extra = {}
        if bid == TNB and live["groups"]:
            extra["live_groups"] = live["groups"]
            if live.get("tactics"):
                extra["live_tactics"] = live["tactics"]       # 10 Oct: only those approaches are real there
            if FORMULA1_GROUP in live["groups"]:
                # Formula 1's live trades: MetaTrader's deal rows only, and only when they could be read
                f1 = (formula1_live(deals, sd.get("open_tickets"), live["since"], magic)
                      if readable and deals is not None else {"stats": stats([]), "open": 0, "since": live["since"]})
                extra["formula1"] = dict(f1, readable=readable and deals is not None)
                extra["judge_after"] = judge_after
        try:
            out[bid] = bot_verdict(bid, str(b.get("label") or label), mode, magic, sel=sel, tot=tot,
                                   data_dir=data_dir, now=now, reset=reset,
                                   real_readable=readable,
                                   record=record, since_plan=since_plan, period=period, deals=deals, **extra)
        except Exception as exc:                          # never a reason to fail the page
            log.warning("verdict of %s: %s", bid, exc)
            if extra.get("live_groups") and mode.upper() != "LIVE":
                # 10 Oct: still part LIVE (Formula 1) - never plain PAPER with its real money called "LIVE days"
                g, lt = tuple(extra["live_groups"]), tuple(extra.get("live_tactics") or ())
                extra = {"mode_words": live_words(g, lt), "live_kinds": live_kinds(g, lt), "live_groups": g,
                         "formula1": extra.get("formula1")}
            else:
                extra = {}
            out[bid] = {"bot": bid, "label": label, "mode": mode, "magic": magic, "code": NOT_ENOUGH,
                        "action": VERDICTS[NOT_ENOUGH][0], "colour": VERDICTS[NOT_ENOUGH][1],
                        "headline": f"Its records could not be read just now ({exc}).", "what_happened": "",
                        "recommendation": "", "evidence": [], "rule": RULE_TEXT,
                        "period": {"real": stats([]), "practice": stats([]), "phrase": in_period(sel or period)},
                        "real": stats([]), "practice": stats([]), "judged": stats([]), "research": None,
                        "segment": None, "error": str(exc), **extra}
    if out.get(TNB) is not None and live["groups"]:
        # 10 Oct review: what config.json changed; 10 Oct, Aaron's answers: and what of the plan's it lacks
        out[TNB]["in_force"] = formula1_in_force(data_dir, focus.get("settings"))
    return out


# ------------------------------------------------------------- the plan --
PLAN_FILE = Path(__file__).resolve().parents[2] / "docs" / "plan.json"

# What each plan stance expects of a bot's record; a verdict listed here disagrees with the plan and the page
# says so next to it: (the words the plan uses, the verdicts that disagree)
PLAN_STANCES = {
    "bin": ("bin", {PROVEN, PROMISING}),
    "test": ("keep testing", {NO_EDGE, PROVEN}),
    "grow": ("keep and grow", {NO_EDGE, BREAK_EVEN}),
}


def load_plan(path=None) -> tuple[Optional[dict], str]:
    """(the plan, '') or (None, why not). The plan is docs/plan.json,
    shipped with the code and updated by the nightly review."""
    p = Path(path) if path is not None else PLAN_FILE
    if not p.exists():
        return None, f"the plan file is not on this computer yet ({p.name} in the docs folder)"
    try:
        raw = json.loads(p.read_text())
    except Exception as exc:
        return None, f"the plan file could not be read ({exc})"
    if not isinstance(raw, dict):
        return None, "the plan file does not hold a plan"
    return raw, ""


def plan_items(plan: Optional[dict], bucket: str) -> list[dict]:
    """The plan's BIN, CHANGE or KEEP items as dicts ({text, why, bot, stance})."""
    out = []
    for x in (plan or {}).get(bucket) or []:
        if isinstance(x, dict):
            out.append(x)
        elif x:
            out.append({"text": str(x)})
    return out


def plan_disagreements(plan: Optional[dict], verdicts: dict) -> list[dict]:
    """Every plan item about a bot whose record now says otherwise: "the
    plan says keep testing Financial Ian, but its record now says NO EDGE".
    An item's "says" may word the plan's stance itself, with {name} for the
    bot's name. [{bot, text, code}]."""
    out = []
    seen = set()
    for bucket in ("bin", "change", "keep"):
        for item in plan_items(plan, bucket):
            bot, stance = item.get("bot"), str(item.get("stance") or "")
            if not bot or stance not in PLAN_STANCES or bot not in verdicts:
                continue
            words, against = PLAN_STANCES[stance]
            v = verdicts[bot]
            code = v.get("code")
            if code in against and (bot, stance) not in seen:
                seen.add((bot, stance))
                name = str(v.get("label") or bot)
                says = str(item.get("says") or "{words} {name}")
                try:
                    says = says.format(words=words, name=name)
                except (KeyError, IndexError, ValueError):
                    says = f"{words} {name}"
                out.append({"bot": bot, "code": code,
                            "text": f"The plan says {says}, but its record now says {code}."})
    return out


def next_steps(plan: Optional[dict], n: int = 3) -> list[str]:
    """The plan's next ``n`` steps not yet done, in order."""
    steps = []
    for s in (plan or {}).get("steps") or []:
        if isinstance(s, dict):
            if s.get("done"):
                continue
            steps.append(str(s.get("text") or ""))
        elif s:
            steps.append(str(s))
    return [s for s in steps if s][:n]

