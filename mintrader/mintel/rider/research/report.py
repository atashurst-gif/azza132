"""The research report: report.md (plain English) and summary.json (every
number), plus trades.csv (the baseline's trades).

Every figure in them is computed from the replays and the event study that
were just run; where something could not be measured the report says so
instead of showing a number. A SYNTHETIC run is labelled SYNTHETIC - NOT
PERFORMANCE in the title, the verdict and every section heading that shows
a figure.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
from pathlib import Path
from typing import Optional, Sequence

from . import REAL_SOURCE, SYNTHETIC_LABEL, SYNTHETIC_SOURCE
from .stats import short_line, summarise

UTC = dt.timezone.utc


# ---------------------------------------------------------------- verdict --

def verdict(normal: dict, stress15: Optional[dict], wf: dict, coverage_ok: bool, coverage_statement: str,
            min_trades: int = 30, synthetic: bool = False) -> dict:
    """The plain verdict. Codes: INSUFFICIENT_DATA, NO_EDGE, FRAGILE,
    NOT_CONFIRMED_OUT_OF_SAMPLE, UNPROVEN, PASSED."""
    flags: list[str] = []
    n = normal.get("trades", 0)
    net = normal.get("net_pips") or 0.0
    fragile = bool(stress15 is not None and net > 0 and (stress15.get("net_pips") or 0.0) < 0)
    if fragile:
        flags.append("FRAGILE")
    oos = (wf or {}).get("oos_result") or {}
    if not coverage_ok:
        code = "INSUFFICIENT_DATA"
        text = f"INSUFFICIENT DATA - {coverage_statement}"
        flags.append("INSUFFICIENT DATA")
    elif n < min_trades:
        code = "INSUFFICIENT_DATA"
        text = (f"INSUFFICIENT DATA - only {n} trades in the replay (need at least {min_trades} before any "
                f"result means anything).")
        flags.append("INSUFFICIENT DATA")
    elif net <= 0:
        code = "NO_EDGE"
        text = (f"NO EDGE - the baseline lost {net:+.1f} pips after every cost over {n} trades "
                f"({normal.get('expectancy_pips', 0):+.2f} a trade).")
    elif fragile:
        code = "FRAGILE"
        text = (f"FRAGILE - {net:+.1f} pips after normal costs, but {stress15.get('net_pips', 0):+.1f} with 1.5x "
                f"spread and slippage: the edge is thinner than the cost of trading it.")
    elif not (wf or {}).get("enough_days"):
        code = "UNPROVEN"
        text = (f"UNPROVEN - {net:+.1f} pips after costs over {n} trades and it survives 1.5x costs, but there "
                f"were not enough days for an out-of-sample test.")
    elif (oos.get("net_pips") or 0.0) <= 0 or not oos.get("trades"):
        code = "NOT_CONFIRMED_OUT_OF_SAMPLE"
        text = (f"NOT CONFIRMED OUT-OF-SAMPLE - positive over the whole period ({net:+.1f} pips) but the "
                f"walk-forward's out-of-sample days gave {oos.get('net_pips') or 0.0:+.1f} pips "
                f"over {oos.get('trades', 0)} trades.")
    elif oos.get("trades", 0) < min_trades:
        code = "UNPROVEN"
        text = (f"PROMISING BUT UNPROVEN - positive after costs ({net:+.1f} pips), survives 1.5x costs and "
                f"positive out-of-sample ({oos.get('net_pips'):+.1f} pips), but only {oos.get('trades')} "
                f"out-of-sample trades.")
    else:
        code = "PASSED"
        text = (f"PASSED THIS TEST - {net:+.1f} pips after every cost over {n} trades, still positive with 1.5x "
                f"costs, and positive out-of-sample ({oos.get('net_pips'):+.1f} pips over {oos.get('trades')} "
                f"trades). A few days of history is still not proof; the demo account is the next test.")
    out = {"code": code, "text": text, "flags": flags, "fragile": fragile}
    if synthetic:
        out = {"code": "SYNTHETIC", "flags": ["SYNTHETIC - NOT PERFORMANCE"], "fragile": fragile,
               "text": (f"{SYNTHETIC_LABEL} - this run used made-up prices to prove the research machinery works "
                        f"end to end. It says NOTHING about whether the bot makes money."),
               "pipeline_check": {"code": code, "text": f"(on synthetic data, meaningless for money) {text}"}}
    return out


# ---------------------------------------------------------------- summary --

def _stress_line(normal: dict, st: dict, label: str) -> str:
    return f"{label}: {st.get('net_pips', 0):+.1f} pips ({(st.get('expectancy_pips') or 0):+.2f} a trade, {st.get('trades', 0)} trades)"


def build_summary(plan, symbols: Sequence[str], days: Sequence[str], cov, trades: dict, counters: dict, wf: dict,
                  es: Optional[dict], stresses: Sequence, cost, notes: Sequence[str], stamp: str,
                  fetch_report: Optional[dict], cpu_seconds: float, ticks_replayed: int) -> dict:
    synthetic = bool(plan.synthetic)
    normal_label = cost.label
    oos_days = set(wf.get("oos_days", []))
    variants_out = []
    for v in plan.variants:
        tv = trades.get(v.method_id, {})
        n = summarise(tv.get(normal_label, []), len(days))
        st = {c.label: summarise(tv.get(c.label, []), len(days)) for c in stresses}
        s15 = next((st[c.label] for c in stresses if c.spread_mult == 1.5 and c.latency_ms == cost.latency_ms), None)
        fragile = bool(s15 is not None and (n.get("net_pips") or 0) > 0 and (s15.get("net_pips") or 0) < 0)
        parts = [_stress_line(n, st[c.label], c.label) for c in stresses]
        cs = "; ".join(parts) + ("; FRAGILE" if fragile else "")
        o = summarise([t for t in tv.get(normal_label, []) if t["day"] in oos_days], len(oos_days)) if oos_days else None
        variants_out.append({"method_id": v.method_id, "description": v.description, "hypothesis": v.hypothesis,
                             "params": v.params(plan.rider_cfg), "grid": v.coords, "normal": n, "stress": st,
                             "fragile": fragile, "cost_sensitivity": cs,
                             "oos_days_result": o,
                             "oos_line": (short_line(o) + " (this variant on the walk-forward's out-of-sample days; "
                                          "not chosen there)") if o is not None else "no out-of-sample days",
                             "counters": counters.get(v.method_id, {}).get(normal_label, {})})
    base = variants_out[0] if variants_out else {"normal": summarise([]), "stress": {}}
    s15 = next((base["stress"].get(c.label) for c in stresses if c.spread_mult == 1.5), None)
    wf_out = {k: v for k, v in wf.items() if k != "oos_trades"}
    v = verdict(base["normal"], s15, wf_out, cov.sufficient, cov.statement(), plan.min_trades, synthetic)
    if es is not None:
        es = {k: x for k, x in es.items()}
        for part in ("runs_vs_ordinary", "bursts"):
            if part in es:
                es[part] = {k: x for k, x in es[part].items() if k != "effects"}
    limits = [
        "Each day is replayed on its own with a fresh bot (seeded with the previous days' one- and five-minute bars, "
        "as live seeds from the broker's bars); a trade open at midnight is followed to its end, but the next day's "
        "replay does not know about it.",
        "Fills: the first tick at or after the delay, at the ask (buy) or bid (sell), plus slippage; stops fill at the "
        "stop or worse. Real fills can differ, especially around news.",
        "The bot's own learning (historical match, 2 of 100 points) stays neutral in replay.",
        "Cost stress keeps the SAME entry signals and multiplies the spread and slippage of every fill and every tick "
        "FlowLock X sees; commission is not multiplied.",
        "Over a weekend the replay closes a still-open trade at Friday's last price (END_OF_DATA) instead of "
        "holding it through the gap.",
        "Tick volume is the broker's count of price updates; there is no order flow in retail MetaTrader FX.",
    ]
    if synthetic:
        limits.insert(0, f"{SYNTHETIC_LABEL}: every price in this run was generated by a seeded random model.")
    return {
        "title": "Rapid Momentum Rider - research report" + (f" ({SYNTHETIC_LABEL})" if synthetic else ""),
        "label": SYNTHETIC_LABEL if synthetic else "",
        "synthetic": synthetic,
        "data_source": SYNTHETIC_SOURCE if synthetic else REAL_SOURCE,
        "generated_utc": dt.datetime.now(UTC).replace(microsecond=0).isoformat(),
        "code_stamp": stamp,
        "markets": list(symbols),
        "days_requested": list(plan.days),
        "days_replayed": list(days),
        "coverage": cov.to_dict(),
        "fetch": fetch_report,
        "settings": {
            "account_currency": plan.account_currency,
            "gbp_per_pip": plan.rider_cfg.user_pip_value_gbp,
            "commission_per_lot_side": plan.rider_cfg.commission_per_lot_side,
            "commission_currency": plan.rider_cfg.commission_currency,
            "slippage_pips_per_side": cost.slippage_pips,
            "latency_ms": cost.latency_ms,
            "scan_interval_seconds": plan.rider_cfg.scan_interval_seconds,
            "stresses": [c.to_dict() for c in stresses],
            "run_settings": [s.to_dict() for s in plan.settings],
            "primary_run_setting": plan.settings[plan.primary].to_dict(),
            "min_trades": plan.min_trades,
        },
        "verdict": v,
        "headline": {"method_id": base.get("method_id", "RMR-BASE"), "normal": base["normal"], "stress": base["stress"],
                     "fragile": base.get("fragile", False), "counters": base.get("counters", {})},
        "variants": variants_out,
        "walk_forward": wf_out,
        "event_study": es,
        "notes": list(notes)[:50],
        "limits": limits,
        "compute": {"cpu_seconds": round(cpu_seconds, 1), "workers": plan.workers, "ticks_per_variant": ticks_replayed},
    }


# ---------------------------------------------------------------- markdown --

def _pct(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{100 * x:.1f}%"


def _num(x: Optional[float], fmt: str = "{:+.1f}") -> str:
    return "n/a" if x is None else fmt.format(x)


def _pf(s: dict) -> str:
    pf = s.get("profit_factor")
    if pf is None:
        return f"n/a ({s.get('profit_factor_note') or 'no trades'})"
    return f"{pf:.2f}"


def headline_table(s: dict, money_ccy: str = "GBP") -> list[str]:
    return [
        "| Measure | Value |", "|---|---|",
        f"| Historical trades tested | {s.get('trades', 0)} |",
        f"| Trades per day | {_num(s.get('trades_per_day'), '{:.1f}')} |",
        f"| Win rate (after costs) | {_pct(s.get('win_rate'))} |",
        f"| Net pips after every cost | {_num(s.get('net_pips'))} |",
        f"| Net money at {money_ccy} 1 a pip | {_num(s.get('net_money'), '{:+.2f}')} |",
        f"| Expectancy (pips a trade) | {_num(s.get('expectancy_pips'), '{:+.2f}')} |",
        f"| Profit factor | {_pf(s)} |",
        f"| Average win (pips) | {_num(s.get('avg_win_pips'))} |",
        f"| Average loss (pips) | {_num(s.get('avg_loss_pips'))} |",
        f"| Max drawdown (pips) | {_num(s.get('max_drawdown_pips'), '{:.1f}')} |",
        f"| MFE capture (realised / best reached) | {_pct(s.get('mfe_capture'))} |",
        f"| Average MFE / MAE (pips) | {_num(s.get('avg_mfe_pips'), '{:.1f}')} / {_num(s.get('avg_mae_pips'), '{:.1f}')} |",
        f"| Commission paid ({money_ccy}) | {_num(s.get('commission_gbp'), '{:.2f}')} |",
    ]


def render_markdown(sm: dict) -> str:
    syn = sm.get("synthetic")
    tag = f" - {SYNTHETIC_LABEL}" if syn else ""
    L: list[str] = [f"# {sm['title']}", ""]
    if syn:
        L += [f"> **{SYNTHETIC_LABEL}.** Every price in this report was made up by a seeded random generator to "
              f"prove the research machinery works. None of these numbers says anything about real trading.", ""]
    L += [f"- **Data source:** {sm['data_source']}",
          f"- **Markets:** {', '.join(sm['markets']) or 'none'}",
          f"- **Days replayed:** {len(sm['days_replayed'])} ({', '.join(sm['days_replayed']) or 'none'})",
          f"- **Generated:** {sm['generated_utc']}", ""]
    v = sm["verdict"]
    L += [f"## Verdict{tag}", "", f"**{v['text']}**", ""]
    if v.get("flags"):
        L += [f"Flags: {', '.join(v['flags'])}", ""]
    if v.get("pipeline_check"):
        L += [f"Verdict logic exercised on the synthetic numbers: {v['pipeline_check']['text']}", ""]
    cov = sm["coverage"]
    L += ["## Data coverage", "", cov["statement"], "",
          f"Ticks in the requested days: {cov['ticks_total']:,}. Rules: a market-day counts with at least "
          f"{cov['rules']['min_ticks_per_day']:,} ticks and no hole longer than {cov['rules']['max_gap_minutes_in_busy_hours']:g} "
          f"minutes between 07:00 and 20:00 UTC; a day counts when most of its markets do.", ""]
    bad = [r for d in cov["per_day"].values() for r in d.values() if not r["usable"]]
    if bad:
        L += ["Left out:", ""] + [f"- {r['symbol']} {r['day']}: {r['why']}" for r in bad[:30]] + [""]
    st = sm["settings"]
    h = sm["headline"]
    L += [f"## Headline: the baseline (the core as built){tag}", "",
          f"Settings fixed BEFORE this research (RMR-BASE). Costs: the spread of every tick, "
          f"{st['slippage_pips_per_side']:g} pips slippage a side, commission {st['commission_per_lot_side']:g} "
          f"{st['commission_currency']} a lot a side, a {st['latency_ms']:g} ms order delay. Size: "
          f"{st['account_currency']} {st['gbp_per_pip']:g} a pip.", ""]
    L += headline_table(h["normal"], st["account_currency"]) + [""]
    n = h["normal"]
    if n.get("exit_reasons"):
        L += ["How trades ended: " + ", ".join(f"{k} {c}" for k, c in n["exit_reasons"].items()), ""]
    if n.get("by_family"):
        L += ["By entry family: " + "; ".join(f"{k}: {x['trades']} trades, {x['net_pips']:+.1f} pips"
                                              for k, x in list(n["by_family"].items())[:8]), ""]
    if n.get("by_symbol"):
        L += ["By market: " + "; ".join(f"{k}: {x['trades']} trades, {x['net_pips']:+.1f} pips"
                                        for k, x in list(n["by_symbol"].items())[:12]), ""]
    L += [f"## Cost and latency stress{tag}", "",
          "The same entry signals, with the spread and slippage multiplied (or the order delay lengthened).", "",
          "| Costs | Trades | Net pips | Pips a trade | Win rate | Profit factor |", "|---|---|---|---|---|---|",
          f"| normal | {n.get('trades', 0)} | {_num(n.get('net_pips'))} | {_num(n.get('expectancy_pips'), '{:+.2f}')} | "
          f"{_pct(n.get('win_rate'))} | {_pf(n)} |"]
    for label, s in h["stress"].items():
        L.append(f"| {label} | {s.get('trades', 0)} | {_num(s.get('net_pips'))} | "
                 f"{_num(s.get('expectancy_pips'), '{:+.2f}')} | {_pct(s.get('win_rate'))} | {_pf(s)} |")
    L += ["", ("**FRAGILE:** positive at normal costs but negative at 1.5x." if h.get("fragile") else
               "Not flagged FRAGILE (1.5x costs did not turn a positive result negative)."), ""]
    wf = sm["walk_forward"]
    L += [f"## Out-of-sample (walk-forward){tag}", "", wf.get("statement", ""), ""]
    if wf.get("enough_days"):
        L += ["| Fold | In-sample | Purged | Out-of-sample | Chosen | Why | Out-of-sample result |",
              "|---|---|---|---|---|---|---|"]
        for f in wf["folds"]:
            L.append(f"| {f['fold']} | {' to '.join(f['in_sample'])} ({f['in_sample_days']} d) | "
                     f"{', '.join(f['purged'])} | {', '.join(f['out_of_sample'])} | {f['chosen']} | {f['why']} | "
                     f"{short_line(f['out_of_sample_result'])} |")
        L += ["", "Out-of-sample, all folds together (this is the result of the tuning, not the best in-sample "
                  "figure):", ""] + headline_table(wf["oos_result"], st["account_currency"]) + [""]
        b = wf.get("baseline_on_oos_days") or {}
        L += [f"For comparison, the untuned baseline on the same out-of-sample days: {short_line(b)}.", ""]
    L += [f"## Every variant tested (research registry){tag}", "",
          "Recorded in the registry. The best line below is NOT the result: picking the best of several "
          "variants after the fact flatters it. Only the walk-forward above chooses fairly.", "",
          "| METHOD_ID | Trades | Win rate | Expectancy | PF | Drawdown | MFE capture | 1.5x costs | Out-of-sample days |",
          "|---|---|---|---|---|---|---|---|---|"]
    for row in sm["variants"]:
        s = row["normal"]
        s15 = next((x for k, x in row["stress"].items() if k.startswith("1.5x")), {})
        o = row.get("oos_days_result") or {}
        L.append(f"| {row['method_id']}{' (FRAGILE)' if row['fragile'] else ''} | {s['trades']} | {_pct(s.get('win_rate'))} | "
                 f"{_num(s.get('expectancy_pips'), '{:+.2f}')} | {_pf(s)} | {_num(s.get('max_drawdown_pips'), '{:.1f}')} | "
                 f"{_pct(s.get('mfe_capture'))} | {_num(s15.get('expectancy_pips'), '{:+.2f}')} | "
                 f"{short_line(o) if o else 'n/a'} |")
    L += [""] + [f"- **{row['method_id']}**: {row['description']} Hypothesis: {row['hypothesis']}" for row in sm["variants"]]
    L += [""]
    es = sm.get("event_study")
    L += [f"## Event study: what happens just before a clean run?{tag}", ""]
    if not es:
        L += ["Not run (no usable days).", ""]
    else:
        s0 = es.get("setting", {})
        L += [f"A SUCCESSFUL MOMENTUM RUN here: {s0.get('name', '')}, buying at the ask / selling at the bid "
              f"(the spread is paid). {es['runs']} runs, {es['baseline']} ordinary moments, "
              f"{es['bursts_ok'] + es['bursts_failed']} fast bursts over {es['days']} day(s).", "",
              "How often runs start (per market per day):", "",
              "| Run | Runs a market-day | Share of seconds that start one |", "|---|---|---|"]
        for r in es.get("base_rates", []):
            L.append(f"| {r['setting']} | {r['runs_per_market_day']:.1f} | {_pct(r['share_of_seconds'])} |")
        L += ["", "What changed before the run (only measured effect sizes; d = Cohen's d, AUC 0.5 = no difference):", ""]
        L += [ln if ln.startswith("  ") else f"- {ln}" for ln in es.get("what_changed", [])]
        L += [""]
        oc = es.get("runs_vs_ordinary", {}).get("outcomes", {})
        if oc:
            L += ["Outcomes after the moment (mid move the run's way, pips): runs / ordinary moments", "",
                  "| After | Runs (mean) | Ordinary (mean) |", "|---|---|---|"]
            for k, lab in (("10", "10 s"), ("30", "30 s"), ("60", "1 min"), ("300", "5 min")):
                x = oc.get(k, {})
                L.append(f"| {lab} | {_num(x.get('a_mean'), '{:+.2f}')} | {_num(x.get('b_mean'), '{:+.2f}')} |")
            L += [""]
        lat = es.get("latency", [])
        L += ["### Latency: do runs survive retail execution speed?", ""]
        if lat:
            L += ["Bursts that became runs when entered at once, entered later instead (at the touch, plus slippage):", "",
                  "| Delay | Still a successful run | Average cost of the delay (pips) |", "|---|---|---|"]
            for r in lat:
                L.append(f"| {r['latency_ms']} ms | {_pct(r['still_successful'])} | {_num(r['avg_delay_cost_pips'], '{:+.2f}')} |")
            L += [""]
        else:
            L += ["No bursts became runs - nothing to measure.", ""]
    L += ["## Method and honest limits", ""] + [f"- {x}" for x in sm.get("limits", [])] + [""]
    if sm.get("notes"):
        L += ["Notes from the run:", ""] + [f"- {x}" for x in sm["notes"][:20]] + [""]
    comp = sm.get("compute", {})
    reused = comp.get("jobs_from_cache", 0)
    L += [f"Compute: {comp.get('cpu_seconds', 0) / 60:.1f} CPU-minutes over {comp.get('workers', 1)} worker(s)"
          + (f"; {reused} of {comp.get('jobs', 0)} jobs reused from the cache of an earlier run." if reused else ".")
          + f" Replay code fingerprint {sm.get('code_stamp', '')}.", ""]
    if syn:
        L += [f"**{SYNTHETIC_LABEL}.**", ""]
    return "\n".join(L)


def trades_csv(trades: Sequence[dict]) -> str:
    cols = ["day", "symbol", "side", "family", "score", "signal_ms", "fill_ms", "exit_ms", "latency_ms", "duration_s",
            "entry", "initial_stop", "final_stop", "exit_price", "stop_pips", "volume", "gbp_per_pip", "gross_pips",
            "commission_gbp", "net_money", "net_pips", "r_multiple", "mfe_pips", "mae_pips", "capture", "exit_reason",
            "exit_state", "stop_moves", "entry_slip_pips", "spread_pips_at_fill", "cost_ratio", "entry_reason"]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(cols)
    for t in sorted(trades, key=lambda x: (x.get("fill_ms") or 0, x.get("symbol", ""))):
        w.writerow([t.get(c) for c in cols])
    return out.getvalue()


def write_outputs(out_dir: Path, summary: dict, base_trades: Sequence[dict]) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    md = render_markdown(summary)
    paths = {"report.md": out_dir / "report.md", "summary.json": out_dir / "summary.json",
             "trades.csv": out_dir / "trades.csv"}
    paths["report.md"].write_text(md)
    paths["summary.json"].write_text(json.dumps(summary, indent=1, default=str))
    paths["trades.csv"].write_text(trades_csv(base_trades))
    return {k: str(p) for k, p in paths.items()}
