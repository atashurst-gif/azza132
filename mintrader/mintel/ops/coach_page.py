"""The coach on the page, in plain words (spec section 15, with section 21).

* :func:`one_line` - the coach on "/" in ONE line (12 Oct: the main page is
  short): its state and how many changes it made today, linking to /coach.
* :func:`render_card` - the card at the top of /coach: what the coach did
  today and why, what is on real money, what it is watching, what needs
  Aaron's yes, the one real figure against his £100 a day aim, and how to
  switch it off. No ``<table``, no ``class="tile"``.
* :func:`render_page_body` - ``/coach``: the full record (each piece of work,
  tonight's plan, the decisions, the brakes, the data checks, the policy's age,
  the luck check, the scorecard, the honest arithmetic, the sources). Tables
  only inside ``.s-old``.
* :func:`progress` - the one real figure: ``standing.period_view`` custom from
  Mon 12 Oct to today (UK days), the page's own arithmetic - real money, every
  bot, after commission, as MetaTrader records it. Never a projection. The
  page's ``/api`` carries it as ``coach_progress`` (the coach copies its day
  rows into its ledger each night).

What the page reads: ``data/coach.json`` (Aaron's envelope: is the coach
switched on?), ``data/COACH-OFF``, and ``data/coach/status.json`` (written by
the coach every minute). It never writes anything, never opens the secrets
file, and the coach never reads this module (``decide.py`` may not import it).

Every money figure carries its source. A status file stamped
``"synthetic": true`` shows "TEST DATA - not a result" in place of every
figure, with no pound sign anywhere on the card.
"""
from __future__ import annotations

import datetime as dt
import html
import json
import re
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
TZ_UK = ZoneInfo("Europe/London")

PROGRESS_FROM = dt.date(2026, 10, 12)       # Aaron's request of 12 Oct: the count starts that UK day
PROGRESS_TO = dt.date(2026, 10, 30)         # three weeks: 15 UK trading days, 12-30 Oct
TARGET_GBP_DAY = 100.0
TARGET_DAYS = 15
NO_PACE_BEFORE = 5                          # no "a day" line before the 5th trading day (today counts)
STALE_S = 180.0                             # the policy's lease: older than this, its grants have lapsed

TEST_DATA = "TEST DATA - not a result"
NOT_ON = "The coach is not switched on yet."
OFF_WORDS = "Switched off - the bots are on your own settings."
WATCH_WORDS = "Watching only - it changes nothing today."
CANNOT_COUNT = "cannot be counted just now"
ESTIMATE = "an estimate, not a result"
PROGRESS_SOURCE = ("Source: MetaTrader's record, every bot, after commission (the page's own figures, "
                   "positions closed on UK days from Mon 12 Oct).")
STOP_LINE = ('Stop the coach: double-click "The Coach" on the desktop and choose 2 - everything goes back to your '
             'own settings at once.')
F1_KEY = "tnb|MOMENTUM_CONTINUATION|FX_MINOR|*"
F1_WORDS = "Formula 1 (momentum trades on minor currency pairs)"
REAL_STATES = ("PINNED", "PLACED", "LIVE", "LIVE_TRIAL")
PRACTICE_STATES = ("PRACTICE", "BANNED_REAL")
_MONEY_RE = re.compile(r"[+\-−]?£\s?[\d,]+(?:\.\d+)?")

e = html.escape


# ------------------------------------------------------------------ helpers --
def _fin(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and x == x and abs(float(x)) != float("inf")


def _utc(t) -> Optional[dt.datetime]:
    if isinstance(t, dt.datetime):
        return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)
    if not isinstance(t, str) or not t.strip():
        return None
    try:
        v = dt.datetime.fromisoformat(t.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC)


def _uk(t, fmt: str = "%a %d %b, %H:%M") -> str:
    v = _utc(t)
    return v.astimezone(TZ_UK).strftime(fmt) if v else ""


def _day_words(d: dt.date) -> str:
    return d.strftime("%a %d %b")


def gbp(v, signed: bool = False, syn: bool = False) -> str:
    """£13.00, +£5.52, -£8.52; TEST DATA in place of any figure from a made-up status."""
    if syn:
        return TEST_DATA
    if not _fin(v):
        return "-"
    v = float(v)
    if signed:
        return f"{'-' if v < 0 else '+'}£{abs(v):,.2f}"
    return f"{'-' if v < 0 else ''}£{abs(v):,.2f}"


def _clean(text: Any, syn: bool) -> str:
    """A sentence from the status file; with a made-up status every money figure in it becomes TEST DATA."""
    s = str(text or "")
    return _MONEY_RE.sub(TEST_DATA, s) if syn else s


def trading_days(a: dt.date, b: dt.date) -> int:
    """UK weekdays from a to b, both included (0 when b < a)."""
    n, d = 0, a
    while d <= b:
        if d.weekday() < 5:
            n += 1
        d += dt.timedelta(days=1)
    return n


# ----------------------------------------------------------------- progress --
def progress(sd: Optional[dict], deals, now: Optional[dt.datetime] = None) -> dict:
    """Real money since Mon 12 Oct against £100 a day: ``standing.period_view(sd, deals, "custom",
    "2026-10-12", <today UK>)["trading"]`` - the page's own arithmetic (every bot, after commission and swap,
    positions closed on UK days). One row per UK day with its own ``period_view``. Trading days are UK
    weekdays from 12 Oct to today (today counts); the "need a day" figure is (100 x 15 - total) / days left.
    No pace before the 5th trading day. Never an estimate: what cannot be counted says so."""
    now = _utc(now) or dt.datetime.now(UTC)
    today = now.astimezone(TZ_UK).date()
    out: dict = {"from": PROGRESS_FROM.isoformat(), "to": today.isoformat(), "until": PROGRESS_TO.isoformat(),
                 "target_gbp_day": TARGET_GBP_DAY, "target_days": TARGET_DAYS,
                 "source": "standing.period_view custom from 2026-10-12 (MetaTrader's record, every bot, after "
                           "commission)",
                 "ok": False, "why": "", "total": None, "trades": None, "trading_days": 0, "days_left": 0,
                 "a_day": None, "need_a_day": None, "pace": False, "days": []}
    if today < PROGRESS_FROM:
        out.update(ok=True, why="the count starts on Mon 12 Oct", total=0.0, trades=0,
                   days_left=trading_days(PROGRESS_FROM, PROGRESS_TO))
        return out
    n = trading_days(PROGRESS_FROM, min(today, PROGRESS_TO))
    left = trading_days(today + dt.timedelta(days=1), PROGRESS_TO) if today < PROGRESS_TO else 0
    out.update(trading_days=n, days_left=left)
    sd = sd or {}
    if not sd.get("start") or sd.get("made") is None or sd.get("error"):
        out["why"] = str(sd.get("error") or "MetaTrader's figures have not been read yet")
        return out
    try:
        from .standing import period_view
        v = period_view(sd, list(deals or ()), "custom", PROGRESS_FROM.isoformat(), today.isoformat(), now=now)
        total = v.get("trading")
        if not _fin(total):
            out["why"] = "MetaTrader's figures are incomplete"
            return out
        rows = []
        d = PROGRESS_FROM
        while d <= today:
            dv = period_view(sd, list(deals or ()), "custom", d.isoformat(), d.isoformat(), now=now)
            rows.append({"day": d.isoformat(), "net": dv.get("trading"), "trades": dv.get("trades"),
                         "weekday": d.weekday() < 5})
            d += dt.timedelta(days=1)
    except Exception as exc:                               # never a reason to fail the page
        out["why"] = f"MetaTrader's figures could not be worked out ({exc})"
        return out
    total = round(float(total), 2)
    out.update(ok=True, total=total, trades=v.get("trades"), days=rows, complete=bool(v.get("complete", True)),
               counted_from=v.get("start"))
    if n >= NO_PACE_BEFORE:
        out["pace"] = True
        out["a_day"] = round(total / n, 2)
        if left > 0:
            out["need_a_day"] = round((TARGET_GBP_DAY * TARGET_DAYS - total) / left, 2)
    return out


def progress_words(p: Optional[dict], syn: bool = False) -> str:
    """The one line against £100 a day (without its source line)."""
    head = "Against your daily aim:" if syn else f"Against £{TARGET_GBP_DAY:.0f} a day:"
    if not p:
        return f"{head} real money since 12 Oct {CANNOT_COUNT}."
    if not p.get("ok"):
        return f"{head} real money since 12 Oct {CANNOT_COUNT}."
    n = int(p.get("trading_days") or 0)
    if n == 0:
        return f"{head} the count starts on Mon 12 Oct."
    days = f"{n} trading day{'' if n == 1 else 's'}"
    line = f"{head} real money since 12 Oct: {gbp(p.get('total'), True, syn)} over {days}"
    if not p.get("pace"):
        return line + f". Too early to say a pace: {days} so far."
    line += f", {gbp(p.get('a_day'), True, syn)} a day."
    left = int(p.get("days_left") or 0)
    if left > 0 and p.get("need_a_day") is not None:
        aim = "your daily aim" if syn else f"£{TARGET_GBP_DAY:.0f} a day"
        line += (f" To average {aim} by Fri 30 Oct, the {left} trading day{'' if left == 1 else 's'} left need "
                 f"{gbp(p.get('need_a_day'), False, syn)} a day.")
    elif left == 0:
        line += " The three weeks to Fri 30 Oct are over."
    return line


# ------------------------------------------------------------------- state --
def _read_json(path: Path) -> tuple[Optional[Any], str]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None, "missing"
    except OSError as exc:
        return None, f"unreadable ({exc})"
    if len(raw) > 2_000_000:
        return None, "too large"
    try:
        return json.loads(raw.decode("utf-8")), ""
    except Exception as exc:
        return None, f"not valid JSON ({exc})"


def load(data_dir, now: Optional[dt.datetime] = None) -> dict:
    """What the page knows about the coach, read only. ``state``: NONE (no data/coach.json: not switched on),
    BROKEN (the envelope cannot be read), OFF (COACH-OFF or the envelope's mode), NO_STATUS (switched on, no
    status yet or it cannot be read), STALE (older than 3 minutes), OK."""
    now = _utc(now) or dt.datetime.now(UTC)
    data = Path(str(data_dir or ""))
    out: dict = {"state": "NONE", "now": now, "status": None, "envelope": None, "why": "", "age_s": None,
                 "synthetic": False}
    if not str(data_dir or ""):
        return out
    env, why = _read_json(data / "coach.json")
    if env is None and why == "missing":
        return out
    if not isinstance(env, dict):
        out.update(state="BROKEN", why=why or "it is not a table")
        return out
    out["envelope"] = env
    st, swhy = _read_json(data / "coach" / "status.json")
    if isinstance(st, dict):
        out["status"] = st
        out["synthetic"] = bool(st.get("synthetic"))
        t = _utc(st.get("written_utc"))
        if t is not None:
            out["age_s"] = max(0.0, (now - t).total_seconds())
    if (data / "COACH-OFF").exists() or str(env.get("mode") or "").upper() == "OFF":
        out["state"] = "OFF"
        return out
    if isinstance(st, dict) and st.get("envelope_error"):
        out.update(state="BROKEN", why=str(st.get("envelope_error")))
        return out
    if not isinstance(st, dict):
        out.update(state="NO_STATUS", why=swhy if swhy != "missing" else "")
        return out
    if out["age_s"] is None or out["age_s"] > STALE_S:
        out["state"] = "STALE"
        return out
    out["state"] = "OK"
    return out


def _watch_until_words(env: Optional[dict], now: dt.datetime) -> str:
    """When PILOT starts after the WATCH day: the next 06:30 UK release after watch_until_utc."""
    try:
        from ..coach import next_release_utc
        until = _utc((env or {}).get("watch_until_utc"))
        if until is None or str((env or {}).get("mode") or "").upper() != "PILOT":
            return ""
        starts = next_release_utc(until)
        if now >= starts:
            return ""
        return (f"It takes charge within your limits at 06:30 UK on {_day_words(starts.astimezone(TZ_UK).date())}, "
                f"if today's checks pass.")
    except Exception:
        return ""


def headline(c: dict) -> tuple[str, str]:
    """(the state in a few words, a sentence under it) - the same words on the card and on /coach."""
    st, state = c.get("status") or {}, c.get("state")
    if state == "NONE":
        return NOT_ON, "Every bot is on your own settings. The coach is switched on by the installer."
    if state == "BROKEN":
        return ("Switched off", f"The coach's instructions could not be read ({c.get('why') or 'unknown'}): "
                                f"the bots are on your own settings.")
    if state == "OFF":
        return "Switched off", OFF_WORDS
    if state == "NO_STATUS":
        if c.get("why"):
            return ("Not reporting", f"The coach's status could not be read ({c['why']}), so this page cannot say "
                                     f"what it is doing. Without a fresh instruction from it the bots are on your "
                                     f"own settings within 3 minutes.")
        return ("Starting", "The coach has not reported yet - the bots are on your own settings until it does.")
    if state == "STALE":
        mins = int((c.get("age_s") or 0) // 60)
        return ("Not reporting", f"The coach has not reported for {mins} minutes: its grants have lapsed, so the "
                                 f"bots are on your own settings with its last restrictions.")
    mode = str(st.get("mode") or "")
    if mode == "WATCH":
        return "watching only", " ".join(x for x in (WATCH_WORDS, _watch_until_words(c.get("envelope"), c["now"]))
                                         if x)
    if mode == "PILOT":
        return "in charge within your limits", ""
    if mode == "GUARD":
        return "may only restrict", str(st.get("words") or "It may only restrict: it adds no risk.")
    if mode == "OFF":
        return "Switched off", OFF_WORDS
    return mode.lower() or "unknown", str(st.get("words") or "")


def one_line(data_dir, now: Optional[dt.datetime] = None, coach: Optional[dict] = None) -> str:
    """The coach in ONE line for "/" (12 Oct: the main page is short): its state and how many changes it made
    today, e.g. "Coach: watching only · no changes today" or "Coach: in charge within your limits · 2 changes
    today - see why". The rest is on /coach. Never raises; never a money figure."""
    now = _utc(now) or dt.datetime.now(UTC)
    try:
        c = coach or load(data_dir, now)
        state = c.get("state")
        if state == "NONE":
            return "Coach: not switched on yet"
        if state in ("BROKEN", "OFF"):
            return "Coach: switched off - the bots are on your own settings"
        if state in ("NO_STATUS", "STALE"):
            title, _sub = headline(c)
            return f"Coach: {title.lower()} - the bots are on your own settings"
        title, _sub = headline(c)
        n = len(_today_changes(c.get("status") or {}, now, limit=10_000))
        line = f"Coach: {title} · " + (f"{n} change{'' if n == 1 else 's'} today - see why" if n else
                                        "no changes today")
        if c.get("synthetic"):
            line += f" ({TEST_DATA})"
        return line
    except Exception as exc:                               # never a reason to fail the page
        return f"Coach: could not be read just now ({exc})"


def _key_name(k: dict) -> str:
    if k.get("key") == F1_KEY:
        return F1_WORDS
    return str(k.get("words") or k.get("key") or "")


def _real_line(k: dict, syn: bool) -> str:
    name = _key_name(k)
    money = k.get("money")
    if k.get("key") == F1_KEY and str(k.get("state")) == "PINNED":
        amt = gbp(money if _fin(money) else 13.0, False, syn)         # Aaron's flat £13 (10 Oct)
        return (f"{name}: {amt} a trade - your choice (10 Oct) until its 20-live-trade judgement. "
                f"The coach leaves it alone until then.")
    bits = [name + ":"]
    if _fin(money) or syn and money is not None:
        bits.append(f"{gbp(money, False, syn)} a trade")
    state = str(k.get("state") or "")
    bits.append({"LIVE_TRIAL": "on a live trial", "LIVE": "live", "PLACED": "your choice until it is judged at 30 "
                 "real trades", "PINNED": "pinned by you"}.get(state, state.lower()))
    line = " ".join(bits[:1] + [", ".join(bits[1:])])
    if k.get("why"):
        line += f" - {_clean(k['why'], syn)}"
    return line.rstrip(".") + "."


def _today_changes(st: dict, now: dt.datetime, limit: int = 5) -> list[str]:
    today = now.astimezone(TZ_UK).date()
    out = []
    for ch in st.get("changes") or ():
        if not isinstance(ch, dict):
            continue
        t = _utc(ch.get("ts_utc"))
        if t is None or t.astimezone(TZ_UK).date() != today:
            continue
        out.append(f"{t.astimezone(TZ_UK).strftime('%H:%M')} {ch.get('words') or ''}")
        if len(out) >= limit:
            break
    return out


def _demo_line(st: dict, syn: bool) -> str:
    line = str(st.get("demo_line") or "")
    if not line:
        return ""
    if line.startswith("Demo mode") and "real-money sizes" not in line:
        line += " No trade was sized by the coach today, so there is no real-money comparison yet."
    return _clean(line, syn)


def _outlook(st: dict, sd: Optional[dict], syn: bool) -> str:
    o = st.get("outlook") if isinstance(st.get("outlook"), dict) else None
    if o and _fin(o.get("lo")) and _fin(o.get("hi")):
        est = (f"about {gbp(o['lo'], False, syn)} to {gbp(o['hi'], False, syn)} a day on the evidence so far - "
               f"{ESTIMATE}.")
    else:
        est = f"not estimated yet (the nightly review works it out) - when it comes it is {ESTIMATE}."
    eq = (sd or {}).get("equity")
    share = ""
    if _fin(eq) and float(eq) > 0 and not syn:
        share = (f" £{TARGET_GBP_DAY:.0f} a day is {TARGET_GBP_DAY / float(eq) * 100:.1f}% of the account "
                 f"(MetaTrader's equity) every day.")
    return f"Honest outlook: {est}{share} The coach never sizes up to catch up."


# -------------------------------------------------------------------- card --
def render_card(data_dir, sd: Optional[dict] = None, deals=None, now: Optional[dt.datetime] = None,
                coach: Optional[dict] = None, prog: Optional[dict] = None) -> str:
    """The coach's card on "/": plain words, no table, no tile. Never raises."""
    now = _utc(now) or dt.datetime.now(UTC)
    try:
        c = coach or load(data_dir, now)
        return _card(c, sd, deals, now, prog)
    except Exception as exc:                               # never a reason to fail the page
        return (f'<section class="s-card s-coach"><h2>The coach\'s call</h2><p class="s-none">It could not be shown '
                f'just now ({e(str(exc))}).</p></section>')


def _card(c: dict, sd, deals, now, prog) -> str:
    st = c.get("status") or {}
    syn = bool(c.get("synthetic"))
    state = c.get("state")
    title, sub = headline(c)
    when = _uk(now, "%a %d %b, %H:%M")
    parts = []
    if state in ("NONE", "BROKEN", "OFF", "NO_STATUS", "STALE"):
        parts.append(f'<h2>The coach\'s call - {e(when)}</h2>')
        parts.append(f'<p class="s-aim">{e(title if state == "NONE" else sub)}</p>')
        if state == "NONE":
            parts.append(f'<p class="s-small">{e(sub)}</p>')
    else:
        age = c.get("age_s")
        checked = f" · checked {int(age)} seconds ago" if age is not None else ""
        parts.append(f'<h2>The coach\'s call - {e(when)} · {e(title)}{e(checked)}</h2>')
        if syn:
            parts.append(f'<p class="s-test"><b>{e(TEST_DATA)}.</b> This status was made up to check the page; '
                         f'nothing on this card is a result.</p>')
        if sub:
            parts.append(f'<p class="s-small">{e(_clean(sub, syn))}</p>')
        keys = [k for k in st.get("keys") or () if isinstance(k, dict)]
        real = [k for k in keys if k.get("route") in ("REAL", "PIN") or str(k.get("state")) in REAL_STATES]
        practice = [k for k in keys if k not in real and (k.get("route") in ("PRACTICE", "SKIP")
                                                          or str(k.get("state")) in PRACTICE_STATES)]
        parts.append('<h3>Real money now</h3>')
        if real:
            parts.append('<ul class="s-changes">' + "".join(f'<li>{e(_real_line(k, syn))}</li>' for k in real[:8])
                         + '</ul>')
        else:
            parts.append('<p class="s-none">Nothing on real money through the coach - every bot trades on your own '
                         'settings.</p>')
        names = ", ".join(_key_name(k) for k in practice[:4])
        more = f" and {len(practice) - 4} more" if len(practice) > 4 else ""
        prac = (f"Practising, no money: {names}{more}. They keep practising - that is how they earn a place."
                if practice else "Practising, no money: everything not on real money keeps practising - that is "
                                 "how it earns a place.")
        parts.append(f'<p class="s-small">{e(prac)}</p>')
        if st.get("board"):
            parts.append(f'<p class="s-small">{e(_clean(st["board"], syn))}</p>')
        changed = _today_changes(st, now)
        if changed:
            parts.append('<p class="s-small"><b>Changed today:</b></p><ul class="s-changes">'
                         + "".join(f'<li>{e(_clean(x, syn))}</li>' for x in changed) + '</ul>')
        else:
            parts.append('<p class="s-small"><b>Changed today:</b> nothing.</p>')
        demo = _demo_line(st, syn)
        if demo:
            parts.append(f'<p class="s-small">{e(demo)}</p>')
    # the one real figure: shown whatever the coach's state (it is the account's, not the coach's)
    p = prog if prog is not None else progress(sd, deals, now)
    parts.append(f'<p class="s-small"><b>{e(progress_words(p, syn))}</b></p>')
    parts.append(f'<p class="s-src">{e(PROGRESS_SOURCE)}</p>')
    if state not in ("NONE", "BROKEN", "OFF", "NO_STATUS", "STALE"):
        parts.append(f'<p class="s-small">{e(_outlook(st, sd, syn))}</p>')
        yes = [str(x) for x in st.get("needs_your_yes") or () if x]
        needs = [str(x) for x in st.get("needs_you") or () if x]
        if needs:
            parts.append('<p class="s-small"><b>Needs you:</b> '
                         + e(" ".join(f"({i}) {_clean(x, syn)}" for i, x in enumerate(needs, 1))) + '</p>')
        parts.append('<p class="s-small"><b>Needs your yes:</b> '
                     + (e(" ".join(f"({i}) {_clean(x, syn)}" for i, x in enumerate(yes, 1))) if yes else "nothing.")
                     + '</p>')
    if state != "NONE":
        parts.append(f'<p class="s-small">{e(STOP_LINE)}</p>')
    parts.append('<p class="s-small"><a href="/coach">The coach in full</a></p>')
    body = "".join(parts)
    if syn:
        body = _MONEY_RE.sub(TEST_DATA, body).replace("£", "")
    return f'<section class="s-card s-coach">{body}</section>'


# ------------------------------------------------------------------ /coach --
def _tbl(head: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{e(h)}</th>" for h in head)
    trs = "".join("<tr>" + "".join(f"<td>{e(str(x))}</td>" for x in r) + "</tr>" for r in rows)
    return f'<table><tr>{th}</tr>{trs}</table>'


def _num(v, fmt: str = "{:.2f}", syn: bool = False) -> str:
    if syn and v is not None:
        return TEST_DATA
    return fmt.format(float(v)) if _fin(v) else "-"


def render_page_body(data_dir, sd: Optional[dict] = None, deals=None, now: Optional[dt.datetime] = None) -> str:
    """/coach: the full record, read only. Tables only inside .s-old. Never raises."""
    now = _utc(now) or dt.datetime.now(UTC)
    try:
        return _page(load(data_dir, now), sd, deals, now)
    except Exception as exc:                               # never a reason to fail the page
        return (f'<section class="s-card"><p class="s-none">The coach\'s page could not be shown just now '
                f'({e(str(exc))}).</p></section>')


def _page(c: dict, sd, deals, now) -> str:
    st = c.get("status") or {}
    syn = bool(c.get("synthetic"))
    prog = progress(sd, deals, now)
    out = [render_card(None, sd, deals, now, coach=c, prog=prog)]       # the title is the shell's (12 Oct)
    if c.get("state") in ("NONE", "BROKEN"):
        out.append(_arithmetic(sd, syn))
        return "".join(out)
    old = []
    # each piece of work
    keys = [k for k in st.get("keys") or () if isinstance(k, dict)]
    if keys:
        rows = []
        for k in keys:
            rows.append([_key_name(k), str(k.get("state") or ""), str(k.get("route") or ""),
                         gbp(k.get("money"), False, syn) if k.get("money") is not None else "-",
                         _num(k.get("p_plus"), "{:.0%}", syn), _num(k.get("lcb80"), "{:+.2f} R", syn),
                         "-" if k.get("trades") is None else f"{k.get('trades')} ({k.get('real_trades') or 0} real)",
                         _clean(k.get("why") or k.get("rule") or "", syn)])
        old.append('<h2>Each piece of work</h2><p class="s-small">The chance of a real edge and the lower bound '
                   '(a trade, in units of its risk, after costs) are worked out from each bot\'s own closed trades, '
                   'practice counted at half weight. They are estimates, not results.</p>'
                   + _tbl(["What", "State", "Route", "Money a trade", "Chance of a real edge", "Lower bound (80%)",
                           "Trades", "Why"], rows))
    else:
        old.append('<h2>Each piece of work</h2><p class="s-none">Nothing registered yet.</p>')
    # tonight's plan
    plan = st.get("plan") if isinstance(st.get("plan"), dict) else None
    if plan and plan.get("items"):
        rel = _uk(plan.get("release_utc"), "%a %d %b %H:%M UK")
        rows = [[str(i.get("item") or ""), _clean(i.get("words") or "", syn), str(i.get("rule") or "")]
                for i in plan["items"] if isinstance(i, dict)]
        old.append(f'<h2>Tonight\'s plan</h2><p class="s-small">Released {e(rel)} unless you veto an item: '
                   f'double-click "The Coach" and choose 5, then type v and the item\'s number (or '
                   f'<code>python -m mintel.coach veto NUMBER</code>).</p>'
                   + _tbl(["Item", "What", "Rule"], rows))
    else:
        old.append('<h2>Tonight\'s plan</h2><p class="s-none">Nothing announced - nothing moves up.</p>')
    # the decision log
    ch = [x for x in st.get("changes") or () if isinstance(x, dict)]
    if ch:
        rows = [[_uk(x.get("ts_utc"), "%a %d %b %H:%M"), _clean(x.get("words") or "", syn), str(x.get("rule") or ""),
                 str(x.get("by") or "")] for x in ch]
        old.append('<h2>Decisions</h2>' + _tbl(["When (UK)", "What and why", "Rule", "By"], rows))
    else:
        old.append('<h2>Decisions</h2><p class="s-none">None yet.</p>')
    # brakes, headroom, data checks, the policy
    br = st.get("brakes") if isinstance(st.get("brakes"), dict) else {}
    data = st.get("data") if isinstance(st.get("data"), dict) else {}
    pol = st.get("policy") if isinstance(st.get("policy"), dict) else {}
    lines = []
    if br:
        lines.append(f"Brakes: day {br.get('day', '-')}, week {br.get('week', '-')}, drawdown "
                     f"{br.get('drawdown', '-')}; trial money left this week "
                     f"{gbp(br.get('trial_budget_left_gbp'), False, syn)}.")
    for w in (st.get("brake_words") or {}).values() if isinstance(st.get("brake_words"), dict) else ():
        lines.append(_clean(w, syn))
    if "headroom_gbp" in st:
        lines.append("Headroom for new real risk today: "
                     + (gbp(st.get("headroom_gbp"), False, syn) if st.get("headroom_gbp") is not None
                        else "not known") + " (what is left under your daily stop, every bot).")
    if _fin(st.get("stop_pct")):
        lines.append(f"Daily stop in force: {_num(st.get('stop_pct'), '{:g}%', syn)} of the day's starting equity.")
    if data:
        lines.append("Data checks: " + ("all passing." if data.get("ok") else
                                        "FAILED - " + "; ".join(str(x) for x in data.get("failed") or ()) +
                                        " (no grants while a check fails)."))
    if pol:
        age = ""
        t = _utc(pol.get("written_utc"))
        if t is not None:
            age = f", written {int(max(0, (now - t).total_seconds()))} seconds ago"
        lines.append(f"Instruction to the bots: number {pol.get('seq', '-')}{age}; each lasts "
                     f"{pol.get('valid_s', 180)} seconds unless rewritten.")
    if st.get("phase"):
        lines.append(f"Market session now: {st.get('phase')}.")
    old.append('<h2>Brakes, headroom and checks</h2>'
               + ("".join(f'<p class="s-small">{e(_clean(x, syn))}</p>' for x in lines) if lines
                  else '<p class="s-none">Not reported yet.</p>'))
    # the luck check and the scorecard
    luck = st.get("luck") if isinstance(st.get("luck"), dict) else None
    if luck and luck.get("shuffles"):
        rate = luck.get("pass_rate")
        old.append(f'<h2>The luck check</h2><p class="s-small">Last night ({e(str(luck.get("date") or ""))}): '
                   f'{e(_num(rate, "{:.1%}", syn))} of {e(str(luck.get("shuffles")))} made-up segments passed the '
                   f'promotion test; ' + ("promotions were blocked." if luck.get("blocked") else
                                          "promotions were allowed (2% or fewer).") + '</p>')
    else:
        old.append('<h2>The luck check</h2><p class="s-none">Not run yet (it runs at the nightly review, '
                   '22:20 UK).</p>')
    sc = st.get("scorecard") if isinstance(st.get("scorecard"), dict) else None
    if sc:
        old.append(f'<h2>The coach\'s scorecard</h2><p class="s-small">{e(str(sc.get("date") or ""))}: saved '
                   f'{e(gbp(sc.get("saved_gbp"), True, syn))}, added {e(gbp(sc.get("added_gbp"), True, syn))} over '
                   f'{e(str(sc.get("trades") or 0))} affected trades - from the bots\' own records, priced at the '
                   f'normal size.</p>')
    else:
        old.append('<h2>The coach\'s scorecard</h2><p class="s-none">Nothing to score yet.</p>')
    # the real days
    if prog.get("ok") and prog.get("days"):
        rows = [[_day_words(dt.date.fromisoformat(r["day"])), gbp(r.get("net"), True, syn),
                 str(r.get("trades") if r.get("trades") is not None else "-")] for r in prog["days"]]
        old.append('<h2>Real money each day since 12 Oct</h2>'
                   + _tbl(["UK day", "Real money, after commission", "Positions closed"], rows)
                   + f'<p class="s-src">{e(PROGRESS_SOURCE)}</p>')
    sugg = [str(x) for x in st.get("suggestions") or () if x]
    if sugg:
        old.append('<h2>Suggestions for the next install (nothing here is applied)</h2><ul>'
                   + "".join(f"<li>{e(_clean(x, syn))}</li>" for x in sugg) + '</ul>')
    out.append('<div class="s-old">' + "".join(old) + '</div>')
    out.append(_arithmetic(sd, syn))
    src = st.get("sources") if isinstance(st.get("sources"), dict) else {}
    if src:
        out.append('<section class="s-card"><h2>Sources</h2>'
                   + "".join(f'<p class="s-small"><b>{e(str(k))}:</b> {e(str(v))}</p>' for k, v in src.items())
                   + f'<p class="s-small"><b>progress:</b> {e(PROGRESS_SOURCE)}</p></section>')
    return "".join(out)


def _arithmetic(sd: Optional[dict], syn: bool) -> str:
    """The honest arithmetic of section 17 and 21, from the account's own equity: never a forecast."""
    eq = (sd or {}).get("equity")
    lines = []
    if _fin(eq) and float(eq) > 0 and not syn:
        eq = float(eq)
        lines.append(f"£{TARGET_GBP_DAY:.0f} a day is {TARGET_GBP_DAY / eq * 100:.2f}% of the account "
                     f"(MetaTrader's equity, £{eq:,.2f}) every day; over the {TARGET_DAYS} UK trading days from 12 to "
                     f"30 Oct it is £{TARGET_GBP_DAY * TARGET_DAYS:,.0f}.")
    lines += [
        "Daily money = edge x money a trade x trades a day. Formula 1 has traded about 2.7 times a weekday. "
        "At 5% a trade a 0.20 R edge is about £50 a day on average with very wide days; £100 a day needs a "
        "confirmed edge of about 0.4 R a trade or several independent edges - arithmetic, not a forecast.",
        "Claude's estimate of 12 Oct on the record so far: about £3 to £15 a day, central about £7 - "
        f"{ESTIMATE}. The coach reaches larger sizes only by earning each step on real results, which takes weeks, "
        "not days. The page tracks the real daily average from 12 Oct, never a projection."]
    return ('<section class="s-card"><h2>The honest arithmetic</h2>'
            + "".join(f'<p class="s-small">{e(x)}</p>' for x in lines) + '</section>')
