"""Statistics for one strategy's day, in the shape the status page shows.
Used for the Rapid Scalper from its own ledger; the attribution layer
builds the same shape for the existing strategy from its records."""
from __future__ import annotations

import statistics as st
from typing import Optional, Sequence


def strategy_stats(closed: Sequence[dict], open_positions: Sequence[dict], *,
                   label: str, strategy_id: str, currency: str = "GBP",
                   pnl_key: str = "net_pnl", scalper: bool = False) -> dict:
    nets = [float(r.get(pnl_key) or 0.0) for r in closed]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x < 0]
    unrealised = sum(float(p.get("pnl") or p.get("profit") or 0.0) for p in open_positions)
    realised = sum(nets)
    # max intraday drawdown on the realised curve
    peak = 0.0; dd = 0.0; cum = 0.0
    for x in nets:
        cum += x
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    costs = sum(float(r.get("commission") or 0.0) + float(r.get("spread_cost") or 0.0)
                + float(r.get("slippage_cost") or 0.0) for r in closed)
    slip = [abs(float(r.get("entry_slippage_points") or 0.0)) + abs(float(r.get("exit_slippage_points") or 0.0))
            for r in closed if r.get("entry_slippage_points") is not None]
    holds = [float(r.get("duration_seconds") or 0.0) for r in closed if r.get("duration_seconds") is not None]
    out = {
        "id": strategy_id, "label": label, "currency": currency,
        "net_today": round(realised + unrealised, 2),
        "realised": round(realised, 2), "unrealised": round(unrealised, 2),
        "trades": len(nets), "wins": len(wins), "losses": len(losses),
        "win_rate": round(100.0 * len(wins) / len(nets), 1) if nets else None,
        "avg_win": round(st.mean(wins), 2) if wins else None,
        "avg_loss": round(st.mean(losses), 2) if losses else None,
        "largest_win": round(max(wins), 2) if wins else None,
        "largest_loss": round(min(losses), 2) if losses else None,
        "profit_factor": (round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) else
                          (None if not wins else float("inf"))),
        "total_costs": round(costs, 2) if closed and any(r.get("commission") is not None for r in closed) else None,
        "est_slippage_points": round(st.mean(slip), 2) if slip else None,
        "avg_hold_seconds": round(st.mean(holds), 1) if holds else None,
        "open_positions": len(open_positions),
        "max_drawdown": round(dd, 2),
    }
    if out["profit_factor"] == float("inf"):
        out["profit_factor"] = "no losses"
    if scalper:
        out.update({
            "avg_duration_seconds": out["avg_hold_seconds"],
            "exits_under_10s": sum(1 for h in holds if h < 10),
            "exits_under_30s": sum(1 for h in holds if h < 30),
            "reached_protected": sum(1 for r in closed if r.get("reached_protected")),
            "reached_runner": sum(1 for r in closed if r.get("reached_runner")),
            "max_favourable_r": round(max((float(r.get("peak_r") or 0) for r in closed), default=0.0), 2),
            "max_adverse_r": round(max((float(r.get("mae_r") or 0) for r in closed), default=0.0), 2),
            "captured_vs_available_pct": _captured(closed),
            "exit_reasons": _count(closed, "exit_reason"),
        })
    return out


def _captured(closed: Sequence[dict]) -> Optional[float]:
    pairs = [(float(r.get("net_pnl") or 0), float(r.get("peak_profit") or 0)) for r in closed
             if (r.get("peak_profit") or 0) > 0]
    if not pairs:
        return None
    return round(100.0 * st.mean(max(0.0, min(1.0, n / p)) for n, p in pairs), 1)


def _count(rows: Sequence[dict], key: str) -> dict:
    out: dict = {}
    for r in rows:
        k = r.get(key) or "?"
        out[k] = out.get(k, 0) + 1
    return out


def combine(strategies: Sequence[dict], label: str = "Overall") -> dict:
    """Overall = every strategy's money added together."""
    out = {"id": "overall", "label": label, "currency": strategies[0]["currency"] if strategies else "GBP"}
    for k in ("net_today", "realised", "unrealised", "trades", "wins", "losses", "open_positions"):
        out[k] = round(sum(float(s.get(k) or 0) for s in strategies), 2)
    out["trades"] = int(out["trades"]); out["wins"] = int(out["wins"]); out["losses"] = int(out["losses"])
    out["open_positions"] = int(out["open_positions"])
    out["win_rate"] = round(100.0 * out["wins"] / out["trades"], 1) if out["trades"] else None
    costs = [s.get("total_costs") for s in strategies if s.get("total_costs") is not None]
    out["total_costs"] = round(sum(costs), 2) if costs else None
    out["max_drawdown"] = round(sum(float(s.get("max_drawdown") or 0) for s in strategies), 2)
    out["by_strategy"] = [{"id": s["id"], "label": s["label"], "net_today": s.get("net_today")} for s in strategies]
    rows = [t for s in strategies for t in (s.get("trades_today") or [])]
    out["trades_today"] = sorted(rows, key=lambda t: str(t.get("closed") or ""))
    return out
