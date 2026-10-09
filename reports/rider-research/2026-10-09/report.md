# Rapid Momentum Rider - research report

- **Data source:** REAL broker ticks (MetaTrader ticks_range: every tick, bid and ask)
- **Markets:** EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, USDCHF, EURJPY, GBPJPY
- **Days replayed:** 10 (2026-09-25, 2026-09-28, 2026-09-29, 2026-09-30, 2026-10-01, 2026-10-02, 2026-10-05, 2026-10-06, 2026-10-07, 2026-10-08)
- **Generated:** 2026-10-09T15:07:04+00:00

## Verdict

**NO EDGE - the baseline lost -1422.8 pips after every cost over 1073 trades (-1.33 a trade).**

## Data coverage

10 of 10 requested days had usable ticks.

Ticks in the requested days: 7,968,471. Rules: a market-day counts with at least 5,000 ticks and no hole longer than 30 minutes between 07:00 and 20:00 UTC; a day counts when most of its markets do.

## Headline: the baseline (the core as built)

Settings fixed BEFORE this research (RMR-BASE). Costs: the spread of every tick, 0.2 pips slippage a side, commission 3.5 USD a lot a side, a 250 ms order delay. Size: GBP 1 a pip.

| Measure | Value |
|---|---|
| Historical trades tested | 1073 |
| Trades per day | 107.3 |
| Win rate (after costs) | 34.2% |
| Net pips after every cost | -1422.8 |
| Net money at GBP 1 a pip | -1421.91 |
| Expectancy (pips a trade) | -1.33 |
| Profit factor | 0.46 |
| Average win (pips) | +3.3 |
| Average loss (pips) | -3.7 |
| Max drawdown (pips) | 1423.4 |
| MFE capture (realised / best reached) | 8.8% |
| Average MFE / MAE (pips) | 2.9 / 2.4 |
| Commission paid (GBP) | 993.06 |

How trades ended: THESIS_DEAD 464, STOP 395, PROTECT_LEVEL 214

By entry family: breakout: 433 trades, -585.2 pips; sweep: 210 trades, -210.0 pips; first_retest: 162 trades, -195.0 pips; session_breakout: 121 trades, -242.2 pips; opening_range: 90 trades, -152.4 pips; squeeze_release: 25 trades, -24.9 pips; velocity_burst: 24 trades, -16.1 pips; pullback: 8 trades, +3.1 pips

By market: USDJPY: 220 trades, -350.5 pips; EURJPY: 193 trades, -383.6 pips; GBPJPY: 185 trades, -262.4 pips; EURUSD: 162 trades, -114.5 pips; GBPUSD: 141 trades, -205.2 pips; USDCHF: 87 trades, -53.6 pips; AUDUSD: 55 trades, -32.4 pips; USDCAD: 30 trades, -20.6 pips

## Cost and latency stress

The same entry signals, with the spread and slippage multiplied (or the order delay lengthened).

| Costs | Trades | Net pips | Pips a trade | Win rate | Profit factor |
|---|---|---|---|---|---|
| normal | 1073 | -1422.8 | -1.33 | 34.2% | 0.46 |
| 1.5x spread and slippage | 1060 | -1876.2 | -1.77 | 31.6% | 0.36 |
| 2x spread and slippage | 1045 | -2308.6 | -2.21 | 28.3% | 0.29 |
| 1000 ms delay | 1057 | -1413.5 | -1.34 | 34.0% | 0.46 |
| 2000 ms delay | 1049 | -1431.8 | -1.36 | 33.7% | 0.45 |

Not flagged FRAGILE (1.5x costs did not turn a positive result negative).

## Out-of-sample (walk-forward)

2 fold(s); out-of-sample days: 2026-10-02, 2026-10-05, 2026-10-07, 2026-10-08. The result below is ONLY the chosen settings' trades on days they were not chosen on.

| Fold | In-sample | Purged | Out-of-sample | Chosen | Why | Out-of-sample result |
|---|---|---|---|---|---|---|
| 1 | 2026-09-25 to 2026-09-30 (4 d) | 2026-10-01 | 2026-10-02, 2026-10-05 | RMR-CURVE-LOOSE | best in-sample expectancy averaged with its grid neighbours (-1.328 pips a trade) - note: even this lost money in-sample | 242 trades, 27% won, -462.5 pips net (-1.91 a trade), PF 0.31 |
| 2 | 2026-09-25 to 2026-10-05 (7 d) | 2026-10-06 | 2026-10-07, 2026-10-08 | RMR-CURVE-LOOSE | best in-sample expectancy averaged with its grid neighbours (-1.353 pips a trade) - note: even this lost money in-sample | 195 trades, 35% won, -210.4 pips net (-1.08 a trade), PF 0.49 |

Out-of-sample, all folds together (this is the result of the tuning, not the best in-sample figure):

| Measure | Value |
|---|---|
| Historical trades tested | 437 |
| Trades per day | 109.2 |
| Win rate (after costs) | 30.4% |
| Net pips after every cost | -672.9 |
| Net money at GBP 1 a pip | -671.36 |
| Expectancy (pips a trade) | -1.54 |
| Profit factor | 0.38 |
| Average win (pips) | +3.1 |
| Average loss (pips) | -3.6 |
| Max drawdown (pips) | 687.2 |
| MFE capture (realised / best reached) | 3.4% |
| Average MFE / MAE (pips) | 2.7 / 2.4 |
| Commission paid (GBP) | 399.42 |

For comparison, the untuned baseline on the same out-of-sample days: 438 trades, 31% won, -667.6 pips net (-1.52 a trade), PF 0.39.

## Every variant tested (research registry)

Recorded in the registry. The best line below is NOT the result: picking the best of several variants after the fact flatters it. Only the walk-forward above chooses fairly.

| METHOD_ID | Trades | Win rate | Expectancy | PF | Drawdown | MFE capture | 1.5x costs | Out-of-sample days |
|---|---|---|---|---|---|---|---|---|
| RMR-BASE | 1073 | 34.2% | -1.33 | 0.46 | 1423.4 | 8.8% | -1.77 | 438 trades, 31% won, -667.6 pips net (-1.52 a trade), PF 0.39 |
| RMR-TRIG-55 | 1523 | 32.2% | -1.48 | 0.40 | 2255.6 | 7.4% | -1.87 | 620 trades, 30% won, -974.4 pips net (-1.57 a trade), PF 0.35 |
| RMR-TRIG-65 | 750 | 34.1% | -1.37 | 0.47 | 1027.5 | 6.0% | -1.79 | 307 trades, 32% won, -476.7 pips net (-1.55 a trade), PF 0.39 |
| RMR-CURVE-LOOSE | 1068 | 33.8% | -1.32 | 0.46 | 1412.0 | 8.6% | -1.75 | 437 trades, 30% won, -672.9 pips net (-1.54 a trade), PF 0.38 |
| RMR-CURVE-TIGHT | 1077 | 34.3% | -1.34 | 0.46 | 1444.7 | 8.4% | -1.77 | 439 trades, 31% won, -662.8 pips net (-1.51 a trade), PF 0.39 |
| RMR-CHOP-OFF | 1089 | 34.4% | -1.32 | 0.46 | 1440.8 | 8.8% | -1.77 | 446 trades, 32% won, -649.7 pips net (-1.46 a trade), PF 0.41 |

- **RMR-BASE**: The core exactly as built: trigger at score 60, balanced giveback curve, anti-chop gate on. Hypothesis: Pre-registered baseline: early entries on clean, fast, confirmed moves, managed by FlowLock X, make money after spread, slippage and commission.
- **RMR-TRIG-55**: Trigger at score 55 (ready at 50): enter earlier on weaker evidence. Hypothesis: Getting on earlier catches more of each run; the extra false starts may cost more than that.
- **RMR-TRIG-65**: Trigger at score 65 (ready at 60): demand stronger evidence before entering. Hypothesis: Fewer, cleaner entries win more often; they may also be later and capture less of each run.
- **RMR-CURVE-LOOSE**: FlowLock X giveback curve 'loose' (gives back 70% young, 45% mature, 30% fading). Hypothesis: More room keeps the big rides alive; the cost is giving back more on the ordinary ones.
- **RMR-CURVE-TIGHT**: FlowLock X giveback curve 'tight' (gives back 45% young, 22% mature, 12% fading). Hypothesis: Protecting profit harder raises MFE capture on ordinary scalps; it may cut the big rides short.
- **RMR-CHOP-OFF**: The anti-chop gate switched off (everything else as the baseline). Hypothesis: The anti-chop gate saves money: without it there are more trades and a worse result after costs.

## Event study: what happens just before a clean run?

A SUCCESSFUL MOMENTUM RUN here: +10 before -5 pips within 5 min, buying at the ask / selling at the bid (the spread is paid). 2399 runs, 9600 ordinary moments, 5278 fast bursts over 10 day(s).

How often runs start (per market per day):

| Run | Runs a market-day | Share of seconds that start one |
|---|---|---|
| +5 before -2.5 pips within 2 min | 115.7 | 5.9% |
| +10 before -5 pips within 5 min | 30.2 | 3.9% |
| +20 before -8 pips within 10 min | 6.2 | 1.5% |
| +30 before -12 pips within 15 min | 2.1 | 0.8% |
| +50 before -20 pips within 30 min | 0.6 | 0.4% |

What changed before the run (only measured effect sizes; d = Cohen's d, AUC 0.5 = no difference):

- Measured at the START of each run - in hindsight the earliest entry that would have worked, usually the bottom of a small dip (so very short-term speed points against the run there):
  - in the 30 seconds before a run, speed the run's way (ATR-normalised, 1 s to 3 min blended) fell (-0.598 on average; ordinary moments +0.004; d=-0.74, AUC 0.29)
  - in the 30 seconds before a run, the MOMENTUM_OPPORTUNITY_SCORE pointing the run's way fell (-15.153 on average; ordinary moments +0.145; d=-0.67, AUC 0.31)
  - in the 30 seconds before a run, the last 5 seconds' move the run's way (in ATRs) fell (-1.136 on average; ordinary moments +0.014; d=-0.65, AUC 0.28)
  - in the 30 seconds before a run, the last 15 seconds' move the run's way (in ATRs) fell (-0.782 on average; ordinary moments +0.018; d=-0.54, AUC 0.33)
  - in the 30 seconds before a run, acceleration the run's way fell (-1.349 on average; ordinary moments +0.038; d=-0.51, AUC 0.34)
  - already 30 seconds before: the one-minute ATR in pips averaged 4.062 before runs against 1.721 at ordinary moments (d=+1.89)
  - already 30 seconds before: volatility expansion averaged 0.230 before runs against 0.151 at ordinary moments (d=+0.43)
  - already 30 seconds before: the last minute's range (in ATRs) averaged 1.227 before runs against 0.984 at ordinary moments (d=+0.36)
- Of 5278 fast bursts, 11% carried on into a full run (entered at once at the touch). What the ones that carried on had at the burst:
  - the one-minute ATR in pips: 4.523 against 3.699 for bursts that died (d=+0.49, AUC 0.63)
  - speed the run's way (ATR-normalised, 1 s to 3 min blended): 1.160 against 1.386 for bursts that died (d=-0.23, AUC 0.42)
  - the last 15 seconds' move the run's way (in ATRs): 1.961 against 2.356 for bursts that died (d=-0.20, AUC 0.39)

Outcomes after the moment (mid move the run's way, pips): runs / ordinary moments

| After | Runs (mean) | Ordinary (mean) |
|---|---|---|
| 10 s | +0.49 | +0.00 |
| 30 s | +1.23 | -0.00 |
| 1 min | +2.20 | +0.01 |
| 5 min | +9.23 | +0.04 |

### Latency: do runs survive retail execution speed?

Bursts that became runs when entered at once, entered later instead (at the touch, plus slippage):

| Delay | Still a successful run | Average cost of the delay (pips) |
|---|---|---|
| 0 ms | 93.0% | +0.20 |
| 250 ms | 91.6% | +0.34 |
| 500 ms | 90.6% | +0.38 |
| 1000 ms | 88.0% | +0.48 |
| 2000 ms | 87.0% | +0.65 |

## Method and honest limits

- Each day is replayed on its own with a fresh bot (seeded with the previous days' one- and five-minute bars, as live seeds from the broker's bars); a trade open at midnight is followed to its end, but the next day's replay does not know about it.
- Fills: the first tick at or after the delay, at the ask (buy) or bid (sell), plus slippage; stops fill at the stop or worse. Real fills can differ, especially around news.
- The bot's own learning (historical match, 2 of 100 points) stays neutral in replay.
- Cost stress keeps the SAME entry signals and multiplies the spread and slippage of every fill and every tick FlowLock X sees; commission is not multiplied.
- Over a weekend the replay closes a still-open trade at Friday's last price (END_OF_DATA) instead of holding it through the gap.
- Tick volume is the broker's count of price updates; there is no order flow in retail MetaTrader FX.

Compute: 367.4 CPU-minutes over 3 worker(s). Replay code fingerprint 0e41cf74d12e.
