# Rapid Momentum Rider - research report

- **Data source:** REAL broker ticks (MetaTrader ticks_range: every tick, bid and ask)
- **Markets:** EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, USDCHF, EURJPY, GBPJPY
- **Days replayed:** 0 (none)
- **Generated:** 2026-10-09T08:52:29+00:00

## Verdict

**INSUFFICIENT DATA - History insufficient: 0 usable day(s) of ticks out of the 10 requested; at least 5 are needed before any result can be judged.**

Flags: INSUFFICIENT DATA

## Data coverage

History insufficient: 0 usable day(s) of ticks out of the 10 requested; at least 5 are needed before any result can be judged.

Ticks in the requested days: 1. Rules: a market-day counts with at least 5,000 ticks and no hole longer than 30 minutes between 07:00 and 20:00 UTC; a day counts when most of its markets do.

Left out:

- EURUSD 2026-09-25: only 0 ticks (need 5,000)
- GBPUSD 2026-09-25: only 0 ticks (need 5,000)
- USDJPY 2026-09-25: only 0 ticks (need 5,000)
- AUDUSD 2026-09-25: only 0 ticks (need 5,000)
- USDCAD 2026-09-25: only 0 ticks (need 5,000)
- USDCHF 2026-09-25: only 0 ticks (need 5,000)
- EURJPY 2026-09-25: only 0 ticks (need 5,000)
- GBPJPY 2026-09-25: only 0 ticks (need 5,000)
- EURUSD 2026-09-28: only 0 ticks (need 5,000)
- GBPUSD 2026-09-28: only 0 ticks (need 5,000)
- USDJPY 2026-09-28: only 0 ticks (need 5,000)
- AUDUSD 2026-09-28: only 0 ticks (need 5,000)
- USDCAD 2026-09-28: only 0 ticks (need 5,000)
- USDCHF 2026-09-28: only 0 ticks (need 5,000)
- EURJPY 2026-09-28: only 0 ticks (need 5,000)
- GBPJPY 2026-09-28: only 0 ticks (need 5,000)
- EURUSD 2026-09-29: only 0 ticks (need 5,000)
- GBPUSD 2026-09-29: only 0 ticks (need 5,000)
- USDJPY 2026-09-29: only 0 ticks (need 5,000)
- AUDUSD 2026-09-29: only 0 ticks (need 5,000)
- USDCAD 2026-09-29: only 0 ticks (need 5,000)
- USDCHF 2026-09-29: only 0 ticks (need 5,000)
- EURJPY 2026-09-29: only 0 ticks (need 5,000)
- GBPJPY 2026-09-29: only 0 ticks (need 5,000)
- EURUSD 2026-09-30: only 0 ticks (need 5,000)
- GBPUSD 2026-09-30: only 0 ticks (need 5,000)
- USDJPY 2026-09-30: only 0 ticks (need 5,000)
- AUDUSD 2026-09-30: only 0 ticks (need 5,000)
- USDCAD 2026-09-30: only 0 ticks (need 5,000)
- USDCHF 2026-09-30: only 0 ticks (need 5,000)

## Headline: the baseline (the core as built)

Settings fixed BEFORE this research (RMR-BASE). Costs: the spread of every tick, 0.2 pips slippage a side, commission 3.5 USD a lot a side, a 250 ms order delay. Size: GBP 1 a pip.

| Measure | Value |
|---|---|
| Historical trades tested | 0 |
| Trades per day | n/a |
| Win rate (after costs) | n/a |
| Net pips after every cost | +0.0 |
| Net money at GBP 1 a pip | +0.00 |
| Expectancy (pips a trade) | n/a |
| Profit factor | n/a (no trades) |
| Average win (pips) | n/a |
| Average loss (pips) | n/a |
| Max drawdown (pips) | 0.0 |
| MFE capture (realised / best reached) | n/a |
| Average MFE / MAE (pips) | n/a / n/a |
| Commission paid (GBP) | 0.00 |

## Cost and latency stress

The same entry signals, with the spread and slippage multiplied (or the order delay lengthened).

| Costs | Trades | Net pips | Pips a trade | Win rate | Profit factor |
|---|---|---|---|---|---|
| normal | 0 | +0.0 | n/a | n/a | n/a (no trades) |
| 1.5x spread and slippage | 0 | +0.0 | n/a | n/a | n/a (no trades) |
| 2x spread and slippage | 0 | +0.0 | n/a | n/a | n/a (no trades) |
| 1000 ms delay | 0 | +0.0 | n/a | n/a | n/a (no trades) |
| 2000 ms delay | 0 | +0.0 | n/a | n/a | n/a (no trades) |

Not flagged FRAGILE (1.5x costs did not turn a positive result negative).

## Out-of-sample (walk-forward)

Not enough days for a walk-forward test: 0 days, need at least 7 (4 in-sample, 1 purged, 2 out-of-sample).

## Every variant tested (research registry)

Recorded in the registry. The best line below is NOT the result: picking the best of several variants after the fact flatters it. Only the walk-forward above chooses fairly.

| METHOD_ID | Trades | Win rate | Expectancy | PF | Drawdown | MFE capture | 1.5x costs | Out-of-sample days |
|---|---|---|---|---|---|---|---|---|
| RMR-BASE | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |
| RMR-TRIG-55 | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |
| RMR-TRIG-65 | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |
| RMR-CURVE-LOOSE | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |
| RMR-CURVE-TIGHT | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |
| RMR-CHOP-OFF | 0 | n/a | n/a | n/a (no trades) | 0.0 | n/a | n/a | n/a |

- **RMR-BASE**: The core exactly as built: trigger at score 60, balanced giveback curve, anti-chop gate on. Hypothesis: Pre-registered baseline: early entries on clean, fast, confirmed moves, managed by FlowLock X, make money after spread, slippage and commission.
- **RMR-TRIG-55**: Trigger at score 55 (ready at 50): enter earlier on weaker evidence. Hypothesis: Getting on earlier catches more of each run; the extra false starts may cost more than that.
- **RMR-TRIG-65**: Trigger at score 65 (ready at 60): demand stronger evidence before entering. Hypothesis: Fewer, cleaner entries win more often; they may also be later and capture less of each run.
- **RMR-CURVE-LOOSE**: FlowLock X giveback curve 'loose' (gives back 70% young, 45% mature, 30% fading). Hypothesis: More room keeps the big rides alive; the cost is giving back more on the ordinary ones.
- **RMR-CURVE-TIGHT**: FlowLock X giveback curve 'tight' (gives back 45% young, 22% mature, 12% fading). Hypothesis: Protecting profit harder raises MFE capture on ordinary scalps; it may cut the big rides short.
- **RMR-CHOP-OFF**: The anti-chop gate switched off (everything else as the baseline). Hypothesis: The anti-chop gate saves money: without it there are more trades and a worse result after costs.

## Event study: what happens just before a clean run?

Not run (no usable days).

## Method and honest limits

- Each day is replayed on its own with a fresh bot (seeded with the previous days' one- and five-minute bars, as live seeds from the broker's bars); a trade open at midnight is followed to its end, but the next day's replay does not know about it.
- Fills: the first tick at or after the delay, at the ask (buy) or bid (sell), plus slippage; stops fill at the stop or worse. Real fills can differ, especially around news.
- The bot's own learning (historical match, 2 of 100 points) stays neutral in replay.
- Cost stress keeps the SAME entry signals and multiplies the spread and slippage of every fill and every tick FlowLock X sees; commission is not multiplied.
- Over a weekend the replay closes a still-open trade at Friday's last price (END_OF_DATA) instead of holding it through the gap.
- Tick volume is the broker's count of price updates; there is no order flow in retail MetaTrader FX.

Compute: 0.0 CPU-minutes over 1 worker(s). Replay code fingerprint a634018cf935.
