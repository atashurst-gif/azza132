"""Trade statistics, computed one way for the report, the registry and the
walk-forward. Every figure is derived from the trade list it is given -
nothing is estimated or filled in. A WIN is a trade whose NET result (after
spread, slippage and commission) is above zero."""
from __future__ import annotations

import math
from collections import Counter
from typing import Iterable, Optional, Sequence


def _r(x: Optional[float], nd: int = 2) -> Optional[float]:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    return round(float(x), nd)


def median(xs: Sequence[float]) -> Optional[float]:
    s = sorted(xs)
    n = len(s)
    if not n:
        return None
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def drawdown(values: Iterable[float]) -> float:
    """Largest peak-to-trough fall of the running total (a positive number)."""
    eq = peak = dd = 0.0
    for v in values:
        eq += v
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return dd


def summarise(trades: Sequence[dict], days: Optional[int] = None) -> dict:
    """trades: dicts with net_pips, net_money, gross_pips, mfe_pips, mae_pips,
    exit_ms, day, symbol, family, exit_reason."""
    n = len(trades)
    out: dict = {"trades": n}
    ndays = days if days is not None else len({t.get("day") for t in trades})
    out["days"] = ndays
    out["trades_per_day"] = _r(n / ndays, 2) if ndays else None
    if not n:
        out.update({"wins": 0, "losses": 0, "win_rate": None, "net_pips": 0.0, "net_money": 0.0,
                    "expectancy_pips": None, "expectancy_money": None, "profit_factor": None,
                    "avg_win_pips": None, "avg_loss_pips": None, "max_drawdown_pips": 0.0,
                    "max_drawdown_money": 0.0, "mfe_capture": None, "median_capture": None,
                    "avg_mfe_pips": None, "avg_mae_pips": None, "best_pips": None, "worst_pips": None,
                    "commission_gbp": 0.0, "exit_reasons": {}, "by_family": {}, "by_symbol": {}})
        return out
    ordered = sorted(trades, key=lambda t: (t.get("exit_ms") or 0, t.get("symbol", "")))
    net = [float(t["net_pips"]) for t in ordered]
    money = [float(t["net_money"]) for t in ordered]
    wins = [x for x in net if x > 0]
    losses = [x for x in net if x <= 0]
    gw = sum(m for m in money if m > 0)
    gl = -sum(m for m in money if m <= 0)
    out["wins"] = len(wins)
    out["losses"] = len(losses)
    out["win_rate"] = _r(len(wins) / n, 4)
    out["net_pips"] = _r(sum(net))
    out["net_money"] = _r(sum(money))
    out["expectancy_pips"] = _r(sum(net) / n, 3)
    out["expectancy_money"] = _r(sum(money) / n, 3)
    out["profit_factor"] = _r(gw / gl, 3) if gl > 0 else None
    out["profit_factor_note"] = "" if gl > 0 else ("no losing trades" if gw > 0 else "no money made or lost")
    out["avg_win_pips"] = _r(sum(wins) / len(wins)) if wins else None
    out["avg_loss_pips"] = _r(sum(losses) / len(losses)) if losses else None
    out["max_drawdown_pips"] = _r(drawdown(net))
    out["max_drawdown_money"] = _r(drawdown(money))
    mfe = [float(t.get("mfe_pips") or 0.0) for t in ordered]
    gross = [float(t.get("gross_pips") or 0.0) for t in ordered]
    tot_mfe = sum(m for m in mfe if m > 0)
    cap_trades = [(g, m) for g, m in zip(gross, mfe) if m > 0]
    out["mfe_capture"] = _r(sum(g for g, _ in cap_trades) / tot_mfe, 3) if tot_mfe > 0 else None
    out["median_capture"] = _r(median([g / m for g, m in cap_trades]), 3) if cap_trades else None
    out["avg_mfe_pips"] = _r(sum(mfe) / n)
    out["avg_mae_pips"] = _r(sum(float(t.get("mae_pips") or 0.0) for t in ordered) / n)
    out["best_pips"] = _r(max(net))
    out["worst_pips"] = _r(min(net))
    out["commission_gbp"] = _r(sum(float(t.get("commission_gbp") or 0.0) for t in ordered))
    out["exit_reasons"] = dict(Counter(str(t.get("exit_reason")) for t in ordered).most_common())
    out["by_family"] = _group(ordered, "family")
    out["by_symbol"] = _group(ordered, "symbol")
    return out


def _group(trades: Sequence[dict], key: str) -> dict:
    groups: dict[str, list] = {}
    for t in trades:
        groups.setdefault(str(t.get(key)), []).append(float(t["net_pips"]))
    return {k: {"trades": len(v), "net_pips": _r(sum(v)), "win_rate": _r(sum(1 for x in v if x > 0) / len(v), 3)}
            for k, v in sorted(groups.items(), key=lambda kv: -len(kv[1]))}


def short_line(s: dict) -> str:
    """One plain line: '42 trades, 38% won, -12.3 pips net (-0.29 a trade), PF 0.85'."""
    if not s.get("trades"):
        return "no trades"
    pf = s.get("profit_factor")
    pf_txt = f"PF {pf:.2f}" if pf is not None else f"PF n/a ({s.get('profit_factor_note', '')})"
    return (f"{s['trades']} trades, {100 * (s.get('win_rate') or 0):.0f}% won, {s['net_pips']:+.1f} pips net "
            f"({s['expectancy_pips']:+.2f} a trade), {pf_txt}")
