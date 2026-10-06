# Nightly review: Tuesday 6 October 2026

All figures are the broker's, after commission. Equity is £2,026.55, and £2,000 + 67.47 - 40.92 matches it exactly.

## What we ended on

| | Trades | Wins | Win rate | Net after commission | Commission |
|---|---|---|---|---|---|
| **Trend & Breakout** | 12 | 4 | 33% | **-40.92** | 14.12 |
| Rapid Scalper (PAPER, not in the account) | 48 | 18 | 38% | -31.75 | 7.08 |
| **Account** | 12 | 4 | 33% | **-40.92** | 14.12 |

| Since the reset | Trades | Wins | Net | Commission |
|---|---|---|---|---|
| 5 Oct (from 12:30 UK) | 4 | 3 | +67.47 | 1.82 |
| 6 Oct | 12 | 4 | -40.92 | 14.12 |
| **Total** | **16** | **7** | **+26.55** | **15.94** |

## Trend & Breakout: what worked and what didn't

| Time (UK) | Trade | Approach | Score | Net | R | How it ended |
|---|---|---|---|---|---|---|
| 03:37 | XAUCHF sell | momentum, fast market | 70.9 | -14.16 | -1.03 | stop (30 seconds) |
| 08:12 | US30 buy | session expansion | 71.7 | **+35.49** | +2.32 | trailed in profit |
| 08:43 | GBPNZD buy | session expansion | 80.1 | -12.80 | -1.09 | stop |
| 12:27 | EURUSD buy | momentum, fast market | 71.7 | -21.27 | -1.06 | stop |
| 12:43 | GBPUSD buy | momentum, fast market | 84.0 | -11.03 | -0.96 | stop (2 min) |
| 13:06 | EURUSD buy | momentum, fast market | 72.5 | +20.31 | +1.67 | trailed in profit |
| 13:19 | XAGAUD buy | momentum, fast market | 72.2 | -17.30 | -1.00 | stop (1 min) |
| 13:33 | XAUCHF buy | momentum, fast market | 71.3 | -17.06 | -1.01 | stop |
| 13:47 | GBPCHF buy | momentum, fast market | 71.6 | +17.82 | +1.21 | trailed in profit |
| 14:57 | US500 buy | momentum, fast market | 73.8 | +7.91 | +0.50 | trailed in profit |
| 15:19 | US500 buy | momentum, fast market | 82.1 | -10.06 | -1.02 | stop |
| 17:09 | USDCHF sell | session expansion | 70.1 | -18.77 | -1.00 | stop (4 min) |

| Breakdown | Today | Since the reset (2 days) |
|---|---|---|
| **Indices** (no commission) | 3 trades, +33.34 | **6 trades, 4 wins, +79.21** |
| FX | 6 trades, -25.74 (12.40 commission) | 7 trades, -4.14 (14.22 commission) |
| Gold/silver crosses | 3 trades, 0 wins, -48.52 | 3 trades, -48.52 |
| Score 70-80 | 9 trades, -7.03 | **12 trades, 6 wins, +67.82** |
| Score 80+ | 3 trades, 0 wins, -33.89 | **4 trades, 0 wins, -41.27** |
| Momentum continuation, fast market | 9 trades, 3 wins, -44.84 | 13 trades, +22.63 |
| Session expansion, trend | 3 trades, 1 win, +3.92 | 3 trades, +3.92 |
| Exit: stop / trailed in profit / target | 8 (-122.45) / 4 (+81.53) / 0 | |

- **Wins against losses:** the average win was +1.42 R (£20.38) and the average loss -1.02 R (£15.31). Winning only a third of trades at that ratio loses money.
- **Hours (UK):** 08:00 was +22.69. Between 12:27 and 13:47, 6 trades lost -28.53. Four of the losers went straight to the stop within four minutes.
- **Commission:** £1.18 a trade on average. FX paid 12.40 of the 14.12.

**Which rules are producing the result** (2 days, 16 trades, so treat this as a lead, not proof):
- **Commission-free indices are carrying it:** +79.21 on 6 trades, positive both days. Everything else lost -52.66 on 10 trades.
- **The 70+ entry floor holds up, but higher scores aren't better.** Scores above 80 have won 0 of 4, which matches what happened before the reset.
- **The ladder:** no trade that reached +1 R ended as a loss on either day. Today's four winners were all trailed out in profit.
- **The eight markets switched off, and twin off:** still can't be judged, because nothing traded that these rules would have stopped.

**One correction about risk.** Each trade does not risk a flat 0.5% (£10). The bot sizes stronger setups higher, up to your 1.5% ceiling, using the settings you have. "Strong" trades risked £13-18 each and the 80+ trades about £10. That's why today's losses averaged £15 rather than £10. I've corrected the strategy document. Nothing has been changed; this is your setting to decide.

## Costs

| Bot | Commission | Spread | Slippage | Cost per trade | Share of the result before fees |
|---|---|---|---|---|---|
| Trend & Breakout | 14.12 | not split out in the report | not split out | 1.18 commission | the result before fees was already -26.80 |
| Rapid Scalper (paper) | 7.08 | 24.87 | 1.12 | 0.69 (it was 1.37 on the old design) | before spread the price result was +0.20, so costs were the whole loss |

## Uptime

The bot was up for 100% of the day (123 of 123 checks), with no outages. Both bots were running all day.

**Right now (23:37 UK):** up, MetaTrader connected, build ccdea84b1f, nothing open.

## Rapid Scalper (PAPER): first day of the new "Pullback" design

**48 trades between 04:00 and 20:50 UK, 18 wins, -31.75.**

| How trades ended | Trades | Net |
|---|---|---|
| Cut: the 1- and 5-minute moves turned against it | 27 | -133.77 (average -0.53 R) |
| No follow-through after 5 minutes | 2 | -0.81 |
| Momentum faded (in profit) | 5 | +82.65 |
| Profit trail | 12 | +1.77 |
| Price broke the last two 1-minute bars (in profit) | 2 | +18.41 |

| Market | Trades | Net |
|---|---|---|
| US30 | 12 | +6.55 |
| Gold | 20 | -1.94 |
| US500 | 1 | -4.17 |
| UK100 | 3 | -13.89 |
| DE40 | 12 | -18.30 |

- **By hour (UK):** no hour was clearly good. The best were 09:00 (+13.33), 07:00 (+11.48) and 13:00 (+10.16). The worst were 11:00 (-18.59) and 04:00 (-13.60).
- **Better than the old design:**
  - Costs per trade halved (0.69 against 1.37).
  - Fewer losers went straight against it: 14 of 30, against 29 of 32 before.
  - It rode three trades to the runner stage, for +64.73.
- **Not good enough yet:**
  - On price it was dead level (+0.20), so all of the loss was costs.
  - 48 trades is too many for "one trade per pullback".
- **A flaw in my own design.** The new entry allows the 5-minute move to be against the trade, because that's what a pullback looks like. But the early cut fires when the 1- and 5-minute moves are against it and the trade is 0.4 R down. So the cut is half-triggered from the moment of entry, and it closed 27 trades for -133.77. The fix is to cut only when the 20-minute trend has turned. I'm not making it tonight: two changes in two days would muddle what we learn. If Wednesday shows the same pattern, I'll make it.
- **It has not earned going live.** There's no hour or market with a clear positive result after costs yet.

## What we can change

1. **Done (standing rule):** breakout acceptance in a fast market is switched off. It was negative on all 3 days it traded (17 Sep, 30 Sep, 1 Oct; -17.39). The rule set is now **"Commission First (version 3)"**. The measuring clock still runs from the reset. This approach hasn't traded since the reset, so expect little difference.
2. **Not changed, but watching (2 days only):**
   - indices against everything else;
   - scores above 80;
   - the 12:30-13:45 UK losses on FX and metals.
   No market meets the switch-off rule yet. GBPNZD and USDCHF have been negative on most days, but they've traded fewer than 6 days. XAUJPY has been negative on 6 of 9 days, but it's up +3.47 overall, so it stays on, as with earlier cases like it.
3. **Your call:** whether stronger setups should keep risking up to £18, or be held at £10.

## Plan for Wednesday

1. Install version 3 with the command below.
2. Trend & Breakout: no other change. I'll watch indices against FX and metals, and scores above 80, for a third day.
3. Rapid Scalper: stays in paper for a second day of the pullback design. If the early cut again costs more than it saves, I'll change it to cut only when the 20-minute trend turns.
