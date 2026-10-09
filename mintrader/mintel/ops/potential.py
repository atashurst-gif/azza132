"""How far could each trade have gone? Replays the broker's one-minute
prices after every closed Trend & Breakout trade.

    python -m mintel.ops.potential --config ~/MarketBot/data/config.json
    python -m mintel.ops.potential --config ... --since 2026-09-17 --hours 8

For each trade, from the moment it opened, in R (1 R = the distance from the
entry to the ORIGINAL stop):

  kept         what the trade actually made
  peak held    the best point it reached while we held it
  could reach  the best point price reached before it came back to the
               original stop (or the window ended) - "if we had ridden it"
  could reach  the same with a stop 1.5x further away (15 instead of 10)
  (wide)

and two plain riding rules, so "could reach" is not mistaken for something
anyone can actually bank:

  ride 1R      once +1 R is reached, trail the stop 1 R behind the best price
  ride wide    stop 1.5x further away, then trail 1.5 R behind the best price

With --runner it replays the Momentum Runner instead, on index trades only,
under each variant in RUNNER_VARIANTS: the 3 R trail with every approach (the
Runner to 8 Oct), without momentum continuation (version 5), a 2 R trail, and
"enter only once the real trade has proved itself at +x R" (``confirm_r``):

    python -m mintel.ops.potential --config ~/MarketBot/data/config.json --runner

For a confirmed entry the Runner buys at the +x R mark, paying the spread,
with its stop one of the real trade's R behind its own entry (the same money
at risk); a minute bar that reaches the mark AND the Runner's stop is counted
as stopped. Results are in R of that risk.

Bars are the broker's (bid prices; the ask is the bid plus the bar's spread).
Within a bar we cannot see whether the high or the low came first, so a bar
that touches the stop is always counted as stopped before it reached its
high: the numbers lean pessimistic, never flattering. Commission is left out:
it is the same in every column. Read-only; nothing is changed. With
--upload the results are also written to the reports branch.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import statistics as st
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..broker.base import TF
from ..clock import to_utc, utcnow
from ..config import Config


@dataclass
class Potential:
    ticket: int
    symbol: str
    side: str
    opened: str
    tactic: str
    kept_r: float
    peak_held_r: float
    reach_r: float = 0.0              # original stop
    reach_minutes: float = 0.0
    stopped: bool = False
    reach_wide_r: float = 0.0        # stop 1.5x further
    ride_r: float = 0.0              # trail 1 R behind once +1 R
    ride_wide_r: float = 0.0         # 1.5x stop, trail 1.5 R behind once +1.5 R
    rules: dict = field(default_factory=dict)   # name -> R banked, for every rule in RULES
    bars: int = 0
    note: str = ""


# The riding rules compared, all on the same bars. (stop, trail, hold hours)
#   stop   how far the first stop sits, in R of the trade's own stop (1.5 = 15 on a 10)
#   trail  once the best price is this many R ahead, the stop trails this far behind it
#   hold   None = until stopped or the window ends; otherwise out at this many hours
RULES: dict[str, tuple[float, Optional[float], Optional[float]]] = {
    "trail 1R": (1.0, 1.0, None),
    "trail 2R": (1.0, 2.0, None),
    "trail 3R": (1.0, 3.0, None),
    "wide, trail 1.5R": (1.5, 1.5, None),
    "wide, trail 2R": (1.5, 2.0, None),
    "wide, trail 3R": (1.5, 3.0, None),
    "hold 2h": (1.0, None, 2.0),
    "hold 4h": (1.0, None, 4.0),
    "hold 8h": (1.0, None, 8.0),
    "wide, hold 4h": (1.5, None, 4.0),
}


def _walk(bars, sign: int, entry: float, dist: float, point: float, opened: dt.datetime,
          stop_mult: float, trail_r: Optional[float],
          hold_hours: Optional[float] = None) -> tuple[float, float, bool, float]:
    """Walk the bars from the entry. Returns (peak R before the stop, minutes
    to that peak, stopped?, R at exit under the rule or at the end)."""
    stop_r = -stop_mult
    peak = 0.0
    peak_min = 0.0
    last_r = 0.0
    for i, b in enumerate(bars):
        if hold_hours is not None and (b.time - opened).total_seconds() >= hold_hours * 3600:
            return peak, peak_min, False, last_r        # out at the last close before the deadline
        spr = (b.spread_points or 0.0) * point
        if sign > 0:
            worst = (b.low - entry) / dist
            best = (b.high - entry) / dist
            close = (b.close - entry) / dist
        else:
            worst = (entry - (b.high + spr)) / dist
            best = (entry - (b.low + spr)) / dist
            close = (entry - (b.close + spr)) / dist
        if i == 0:
            best = max(close, 0.0)          # the entry minute: before the fill is unknown
        # a trailing stop, set from the peak BEFORE this bar
        level = stop_r
        if trail_r is not None and peak >= trail_r:
            level = max(level, peak - trail_r)
        if worst <= level:
            return peak, peak_min, True, level
        if best > peak:
            peak = best
            peak_min = (b.time - opened).total_seconds() / 60.0
        last_r = close
    return peak, peak_min, False, last_r


def analyse(trade: dict, bars: Sequence, point: float) -> Potential:
    sign = 1 if str(trade.get("side", "")).upper() in ("BUY", "LONG") else -1
    entry = float(trade["entry"])
    stop = float(trade["stop"])
    dist = abs(entry - stop)
    opened = to_utc(dt.datetime.fromisoformat(str(trade["opened_utc"])))
    p = Potential(ticket=int(trade.get("ticket") or 0), symbol=str(trade.get("symbol")),
                  side="BUY" if sign > 0 else "SELL", opened=opened.isoformat()[:16],
                  tactic=str(trade.get("tactic") or ""),
                  kept_r=round(float(trade.get("realised_r") or 0.0), 2),
                  peak_held_r=round(float(trade.get("mfe_r") or 0.0), 2))
    if dist <= 0:
        p.note = "no stop recorded"
        return p
    start = opened.replace(second=0, microsecond=0)
    seq = [b for b in bars if to_utc(b.time) >= start]
    p.bars = len(seq)
    if not seq:
        p.note = "no price history"
        return p
    p.reach_r, p.reach_minutes, p.stopped, _ = _walk(seq, sign, entry, dist, point, opened, 1.0, None)
    p.reach_wide_r, _, _, _ = _walk(seq, sign, entry, dist, point, opened, 1.5, None)
    _, _, _, p.ride_r = _walk(seq, sign, entry, dist, point, opened, 1.0, 1.0)
    _, _, _, p.ride_wide_r = _walk(seq, sign, entry, dist, point, opened, 1.5, 1.5)
    for name, (stop_mult, trail, hold) in RULES.items():
        p.rules[name] = round(_walk(seq, sign, entry, dist, point, opened, stop_mult, trail, hold)[3], 2)
    for k in ("reach_r", "reach_wide_r", "ride_r", "ride_wide_r", "reach_minutes"):
        setattr(p, k, round(getattr(p, k), 2))
    return p


def run(trades: list[dict], fetch: Callable[[str, dt.datetime, int], list],
        point_of: Callable[[str], float], hours: float) -> list[Potential]:
    out = []
    for t in trades:
        try:
            opened = to_utc(dt.datetime.fromisoformat(str(t["opened_utc"])))
            count = int(hours * 60) + 5
            bars = fetch(str(t["symbol"]), opened + dt.timedelta(hours=hours), count)
            out.append(analyse(t, bars, point_of(str(t["symbol"]))))
        except Exception as exc:                       # one bad trade never stops the rest
            out.append(Potential(int(t.get("ticket") or 0), str(t.get("symbol")), str(t.get("side")),
                                 str(t.get("opened_utc"))[:16], str(t.get("tactic") or ""),
                                 float(t.get("realised_r") or 0), float(t.get("mfe_r") or 0),
                                 note=f"skipped: {exc}"))
    return out


def render(res: list[Potential], hours: float) -> str:
    ok = [p for p in res if p.bars]
    if not ok:
        return "No trades with price history found."
    winners = sorted([p for p in ok if p.kept_r >= 1.0], key=lambda p: -p.reach_r)
    mean = lambda xs: st.mean(xs) if xs else 0.0          # noqa: E731
    lines = [f"HOW FAR COULD THEY HAVE GONE? {len(ok)} trades, the {hours:g} hours after each entry, in R",
             "(1 R = the distance to the original stop; with a 10 stop, 1 R = 10)", "",
             "THE BIG WINNERS (kept 1 R or more)",
             f"  {'opened':16} {'market':8} {'kept':>6} {'peak':>6} {'could':>6} {'could':>6} {'ride':>6} {'ride':>6}  {'min to':>6}",
             f"  {'':16} {'':8} {'':>6} {'held':>6} {'reach':>6} {'wide':>6} {'1R':>6} {'wide':>6}  {'peak':>6}"]
    for p in winners[:25]:
        lines.append(f"  {p.opened:16} {p.symbol:8} {p.kept_r:+6.2f} {p.peak_held_r:+6.2f} {p.reach_r:+6.2f} "
                     f"{p.reach_wide_r:+6.2f} {p.ride_r:+6.2f} {p.ride_wide_r:+6.2f}  {p.reach_minutes:6.0f}")

    def row(label, ps):
        return (f"  {label:28} {len(ps):4d}  kept {mean([p.kept_r for p in ps]):+5.2f}  "
                f"could reach {mean([p.reach_r for p in ps]):+5.2f} (wide {mean([p.reach_wide_r for p in ps]):+5.2f})  "
                f"ride 1R {mean([p.ride_r for p in ps]):+5.2f}  ride wide {mean([p.ride_wide_r for p in ps]):+5.2f}")
    lines += ["", "AVERAGES PER TRADE (R)", row("big winners (kept 1R+)", winners),
              row("every trade", ok),
              row("trades that lost", [p for p in ok if p.kept_r < 0])]
    tot = lambda k: sum(getattr(p, k) for p in ok)          # noqa: E731
    lines += ["", "ALL TRADES ADDED UP (R; x10 for money at a 10 stop)",
              f"  as traded {tot('kept_r'):+.1f}   ride 1R {tot('ride_r'):+.1f}   ride wide {tot('ride_wide_r'):+.1f}",
              f"  big winners that kept going past 3 R before the stop: {sum(p.reach_r >= 3 for p in winners)} of {len(winners)}"]
    # the rule table: which plain riding rule, if any, beats what the ladder kept
    def table(title, ps):
        if not ps:
            return []
        out = ["", f"{title} ({len(ps)} trades): R per trade, and the total",
               f"  {'rule':20} {'per trade':>10} {'total':>8}   {'beats the ladder on':>20}"]
        kept = [p.kept_r for p in ps]
        out.append(f"  {'the ladder (as traded)':20} {mean(kept):+10.2f} {sum(kept):+8.1f}")
        for name in RULES:
            xs = [p.rules.get(name, 0.0) for p in ps]
            better = sum(1 for p in ps if p.rules.get(name, 0.0) > p.kept_r)
            out.append(f"  {name:20} {mean(xs):+10.2f} {sum(xs):+8.1f}   {better:4d} of {len(ps)}")
        return out
    lines += table("EVERY TRADE", ok)
    lines += table("THE BIG WINNERS ONLY", winners)
    idx = [p for p in ok if p.symbol in INDICES]
    lines += table("INDICES ONLY", idx)
    lines += ["", "Money: with a 10 stop, 1 R = 10; the 'wide' rules risk 15 on every trade.",
              "A 'hold' rule still has the stop; it just does not trail. Commission is left out of every column."]
    skipped = [p for p in res if not p.bars]
    if skipped:
        lines.append(f"  ({len(skipped)} trades had no price history and are left out)")
    return "\n".join(lines)


INDICES = ("US30", "US500", "DE40", "UK100", "NAS100", "USTEC", "JP225", "AUS200", "FRA40", "EU50")


# ------------------------------------------------------------- the Runner --
MC = ("MOMENTUM_CONTINUATION",)
# name -> (trail R, confirm R (0 = enter with the trade), approaches not ridden)
RUNNER_VARIANTS: dict[str, tuple[float, float, tuple[str, ...]]] = {
    "with the trade, trail 3R, every approach (to 8 Oct)": (3.0, 0.0, ()),
    "with the trade, trail 3R, no momentum (version 5)": (3.0, 0.0, MC),
    "with the trade, trail 2R, no momentum": (2.0, 0.0, MC),
    "at +0.5R, trail 3R, no momentum": (3.0, 0.5, MC),
    "at +1R, trail 3R, no momentum": (3.0, 1.0, MC),
    "at +1R, trail 3R, every approach": (3.0, 1.0, ()),
}


def _marks(b, sign: int, entry: float, dist: float, point: float) -> tuple[float, float, float, float]:
    """(worst, best, close, spread) of one bar in R, pessimistic: a sell is
    marked on the ask."""
    spr = (b.spread_points or 0.0) * point
    if sign > 0:
        return (b.low - entry) / dist, (b.high - entry) / dist, (b.close - entry) / dist, spr / dist
    return ((entry - (b.high + spr)) / dist, (entry - (b.low + spr)) / dist,
            (entry - (b.close + spr)) / dist, spr / dist)


def runner_walk(bars, sign: int, entry: float, dist: float, point: float, opened: dt.datetime,
                trail_r: float, confirm_r: float = 0.0, window_hours: float = 8.0) -> Optional[float]:
    """The Runner's result in R on these bars, or None when it never entered.

    confirm_r 0: the real trade's entry and stop, nothing moves until the best
    price is trail_r ahead, then trailed trail_r behind it (``_walk``).
    confirm_r > 0: nothing until the real trade is confirm_r ahead (a trade
    stopped first is never entered); then in at that mark plus the spread,
    stop 1 R behind, trailed the same way. The bar that reaches the mark is
    counted stopped if it also reached the new stop (order unknown)."""
    if confirm_r <= 0:
        return _walk(bars, sign, entry, dist, point, opened, 1.0, trail_r, window_hours)[3]
    limit = window_hours * 3600
    for i, b in enumerate(bars):
        if (b.time - opened).total_seconds() >= limit:
            return None
        worst, best, close, spr = _marks(b, sign, entry, dist, point)
        if i == 0:
            best = max(close, 0.0)                         # the entry minute: before the fill is unknown
        if worst <= -1.0:
            return None                                   # the real trade was stopped before it proved itself
        if best < confirm_r:
            continue
        in_r = confirm_r + spr                           # paying the spread to get in
        stop = in_r - 1.0
        if worst <= stop:
            return -1.0
        peak = max(0.0, close - in_r)
        last = close - in_r
        for b2 in bars[i + 1:]:
            if (b2.time - opened).total_seconds() >= limit:
                return last
            w2, b2best, c2, _ = _marks(b2, sign, entry, dist, point)
            level = stop
            if peak >= trail_r:
                level = max(level, in_r + peak - trail_r)
            if w2 <= level:
                return level - in_r
            peak = max(peak, b2best - in_r)
            last = c2 - in_r
        return last
    return None


@dataclass
class RunnerReplay:
    ticket: int
    symbol: str
    tactic: str
    kept_r: float
    results: dict = field(default_factory=dict)       # variant -> R, or None when not entered
    note: str = ""
    day: str = ""                                    # the UTC date it opened, for the days-down count


def runner_run(trades: list[dict], fetch: Callable[[str, dt.datetime, int], list],
               point_of: Callable[[str], float], hours: float = 8.0) -> list[RunnerReplay]:
    """Every INDEX trade, under every Runner variant, on the same bars."""
    from ..contracts import infer_group
    out = []
    for t in trades:
        sym = str(t.get("symbol") or "")
        if infer_group(sym) != "INDEX":
            continue
        tactic = str(t.get("tactic") or "").upper()
        base = tactic[:-3] if tactic.endswith("_2X") else tactic
        rr = RunnerReplay(int(t.get("ticket") or 0), sym, base, round(float(t.get("realised_r") or 0.0), 2),
                          day=str(t.get("opened_utc") or "")[:10])
        try:
            sign = 1 if str(t.get("side", "")).upper() in ("BUY", "LONG") else -1
            entry, stop = float(t["entry"]), float(t["stop"])
            dist = abs(entry - stop)
            opened = to_utc(dt.datetime.fromisoformat(str(t["opened_utc"])))
            if dist <= 0:
                rr.note = "no stop recorded"
                out.append(rr)
                continue
            bars = fetch(sym, opened + dt.timedelta(hours=hours), int(hours * 60) + 5)
            seq = [b for b in bars if to_utc(b.time) >= opened.replace(second=0, microsecond=0)]
            if not seq:
                rr.note = "no price history"
                out.append(rr)
                continue
            point = point_of(sym)
            for name, (trail, confirm, skip) in RUNNER_VARIANTS.items():
                if base in skip:
                    rr.results[name] = None
                    continue
                v = runner_walk(seq, sign, entry, dist, point, opened, trail, confirm, hours)
                rr.results[name] = None if v is None else round(v, 2)
        except Exception as exc:                            # one bad trade never stops the rest
            rr.note = f"skipped: {exc}"
        out.append(rr)
    return out


def render_runner(res: list[RunnerReplay], hours: float = 8.0) -> str:
    ok = [r for r in res if r.results]
    if not ok:
        return "No index trades with price history found."
    lines = [f"THE MOMENTUM RUNNER, REPLAYED: {len(ok)} index trades, the {hours:g} hours after each entry, in R",
             "(1 R = the real trade's distance to its original stop; the Runner risks the same)", "",
             f"  {'variant':52} {'taken':>5} {'wins':>5} {'total R':>8} {'per trade':>9}  {'days down':>9}"]
    for name in RUNNER_VARIANTS:
        taken = [(r, r.results.get(name)) for r in ok if r.results.get(name) is not None]
        xs = [v for _, v in taken]
        days: dict = {}
        for r, v in taken:
            days[r.day] = days.get(r.day, 0.0) + v
        down = f"{sum(1 for v in days.values() if v < 0)} of {len(days)}"
        lines.append(f"  {name:52} {len(xs):5d} {sum(1 for v in xs if v > 0):5d} {sum(xs):+8.1f} "
                     f"{(sum(xs) / len(xs) if xs else 0.0):+9.2f}  {down:>9}")
    base = next(iter(RUNNER_VARIANTS))
    lines += ["", f"BY APPROACH ({base})",
              f"  {'approach':28} {'trades':>6} {'Runner R':>9} {'ladder kept R':>14}"]
    tactics = sorted({r.tactic for r in ok})
    for tac in tactics:
        rs = [r for r in ok if r.tactic == tac and r.results.get(base) is not None]
        if not rs:
            continue
        lines.append(f"  {tac.replace('_', ' ').lower():28} {len(rs):6d} {sum(r.results[base] for r in rs):+9.1f} "
                     f"{sum(r.kept_r for r in rs):+14.1f}")
    lines += ["", "A Runner trade is a second position beside the real one: its R is added to what the",
              "ladder kept, not instead of it. Commission is left out (indices carry none at IC Markets).",
              "Pessimistic: a bar that touches a stop is stopped first; a confirmed entry pays the spread."]
    skipped = [r for r in res if not r.results]
    if skipped:
        lines.append(f"  ({len(skipped)} index trades had no price history and are left out)")
    return "\n".join(lines)


def runner_csv(res: list[RunnerReplay]) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["ticket", "symbol", "tactic", "kept_r", "note"] + list(RUNNER_VARIANTS))
    for r in res:
        w.writerow([r.ticket, r.symbol, r.tactic, r.kept_r, r.note]
                   + ["" if r.results.get(n) is None else r.results[n] for n in RUNNER_VARIANTS])
    return out.getvalue()


def to_csv(res: list[Potential]) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    cols = [c for c in Potential.__dataclass_fields__ if c != "rules"] + list(RULES)
    w.writerow(cols)
    for p in res:
        w.writerow([getattr(p, c) for c in cols if c in Potential.__dataclass_fields__]
                   + [p.rules.get(name, "") for name in RULES])
    return out.getvalue()


def since_utc(text: str) -> dt.datetime:
    """A date or date-time on the command line, read as UTC unless it says otherwise."""
    d = dt.datetime.fromisoformat(text)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return to_utc(d)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="how far could each trade have gone")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--since", default="2026-09-17")
    ap.add_argument("--hours", type=float, default=8.0)
    ap.add_argument("--upload", action="store_true", help="also write the results to the reports branch")
    ap.add_argument("--runner", action="store_true",
                    help="replay the Momentum Runner's variants on the index trades instead")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    from ..engine.journal import Journal
    from ..run import build_broker
    j = Journal(Path(cfg.ops.data_dir) / "journal.sqlite")
    since = since_utc(args.since)
    trades = [t for t in j.closed_trades(limit=5000, since=since) if t.get("entry") and t.get("stop")]
    trades.sort(key=lambda t: str(t.get("opened_utc")))
    broker = build_broker(cfg)
    if not broker.connect():
        print("FAIL: could not connect to the bridge/MetaTrader", file=sys.stderr)
        return 2
    try:
        cache: dict = {}

        def point_of(sym: str) -> float:
            if sym not in cache:
                s = broker.spec(sym)
                cache[sym] = float(s.point) if s else 0.0
            return cache[sym]

        print(f"Replaying {len(trades)} trades from the broker's price history...", file=sys.stderr)
        fetch = lambda sym, end, n: broker.bars(sym, TF.M1, n, end)          # noqa: E731
        if args.runner:
            rres = runner_run(trades, fetch, point_of, args.hours)
        else:
            res = run(trades, fetch, point_of, args.hours)
    finally:
        try:
            broker.disconnect()
        except Exception:
            pass
    data = Path(cfg.ops.data_dir)
    if args.runner:
        text = render_runner(rres, args.hours)
        table, name = runner_csv(rres), "potential-runner"
    else:
        text = render(res, args.hours)
        table, name = to_csv(res), "potential"
    print(text)
    (data / f"{name}.txt").write_text(text + "\n")
    (data / f"{name}.csv").write_text(table)
    if args.upload:
        from .report_upload import read_token, upload
        token = read_token(args.config)
        if not token:
            print("\n(not uploaded: no GitHub token saved - paste the text above instead)")
            return 0
        day = to_utc(utcnow()).date().isoformat()
        try:
            stem = f"{day}-runner" if args.runner else day
            upload({f"reports/potential/{stem}.txt": text + "\n", f"reports/potential/{stem}.csv": table},
                   cfg.report_repo, cfg.report_branch, token, day=day)
            print(f"\nUploaded to reports/potential/{stem}.txt")
        except Exception as exc:
            print(f"\n(not uploaded: {exc} - paste the text above instead)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
