# Review: Thursday 8 October 2026

**Account: −£37.29 on the broker's day, after commission (balance £1,978.65 → £1,941.36).** Trend & Breakout made +£9.14. Every other bot's real trades lost. Crowd Fader's silver short then stopped out at 23:36 UK for −£9.30, after the broker's day had rolled at 22:00 UK, so the balance is now **£1,932.06**. That is −£67.94 since the reset to £2,000 on 5 Oct.

All figures are the broker's own. No exit was estimated (no "?" reasons). A second checker recomputed all 27 headline figures independently and agreed with every one.

## What we ended on

| Bot | Mode | Trades | Won | Net |
|---|---|---|---|---|
| Trend & Breakout | LIVE | 8 | 5 (62%) | **+9.14** |
| Rapid Scalper | LIVE 15:29–16:18 UK | 5 | 0 | −8.72 |
| Momentum Runner | LIVE | 1 | 0 | −8.78 |
| Band Breaker | LIVE | 2 | 0 | −9.23 |
| Crowd Fader | LIVE | 1 | 0 | −19.70 |
| **Account** | | **17** | **5 (29%)** | **−37.29** |
| *Practice, not money:* Rapid Scalper | PAPER | 12 | 3 | −7.48 |
| *Practice:* Momentum Runner | PAPER | 2 | 1 | +17.83 |
| *Practice:* Crowd Fader | PAPER | 2 | 0 | −13.81 |
| *Practice:* Band Breaker | PAPER | 0 | – | 0 |

The balance check works to the penny: 1,978.65 − 37.29 = 1,941.36.

**Every day since the reset:**

| | 5 Oct (from 12:30 UK) | 6 Oct | 7 Oct | 8 Oct |
|---|---|---|---|---|
| Trend & Breakout | +67.47 (4) | −40.92 (12) | −48.09 (18) | +9.14 (8) |
| Other bots, LIVE | 0 | 0 | +0.19 (placed by hand) | −46.43 (9) |
| **Account** | **+67.47** | **−40.92** | **−47.90** | **−37.29** |
| Balance at day end | 2,067.47 | 2,026.55 | 1,978.65 | 1,941.36 |

**How the day went.** Trend & Breakout was +19.33 by 13:42 UK, and the balance peaked at about £2,003 at 15:35 UK. Then two things did most of the damage.

**US500 short at 18:27 UK.** This was one idea traded three times. Trend & Breakout lost 9.31 on it, the Runner (riding the same entry) lost 8.78 and Band Breaker lost 8.01: **−26.10, 70% of the day's loss.**

**Gold and silver short at 16:15 UK by Crowd Fader.** Gold lost 19.70. The smallest size, 0.01 lots, with a 26-point stop risks twice the usual £10. Silver lost 9.30 after the roll.

## What is working (Trend & Breakout)

| Since the reset (42 trades, 4 days) | Trades | Won | Net |
|---|---|---|---|
| **Indices** (no commission) | 14 | 9 | **+79.12** |
| FX | 15 | 7 | −17.07 (minor pairs +36.74, majors −53.81) |
| Gold | 8 | 2 | −54.95 |
| Silver | 5 | 2 | −19.50 |
| Session expansion in a trend | 9 | 5 | **+24.91** (no losing day) |
| Momentum continuation in a fast market | 33 | 15 | −37.31 (3 of 4 days down) |
| Score 70–76 | 27 | 14 | +16.40 |
| Score 76–82 | 9 | 5 | +7.72 |
| Score 82–88 | 6 | 1 | **−36.52** (every day down) |
| Exit: target hit | 3 | 3 | +61.23 |
| Exit: trailed out in profit | 17 | 17 | +180.24 |
| Exit: stopped at a loss | 22 | 0 | −253.87 |

- **8 Oct:** session expansion made +11.61 on 3 trades and momentum continuation lost 2.47 on 5. Average win was 0.89 R against an average loss of 1.01 R (R = the amount risked to the stop).
- **Since the reset:** average win 1.09 R against average loss 1.02 R, with 47.6% winners. That is break-even before costs. Commission (£28.76) turned +16.36 into −12.40.

**Which rules are doing the work, and how sure we can be:**
- **Entry 70+:** strong. Before the reset, scores under 70 lost 92.62 over 173 trades. But within 70+, a higher score is not better: 82–88 has lost on 8 of 12 days over all history.
- **Markets switched off:** strong. They lost 207.55 over 112 trades and lost on all 10 days they traded.
- **Commission-free indices:** the main source of profit, moderately sure. All history: +86.32 over 102 trades.
- **The ladder** (locks profit in steps): since 30 Sep, no trade that reached +1 R has ended in a loss (73 trades). The cost is that winners keep only 64% of their best point.
- **£10 cap:** losses have averaged 9.54 since 7 Oct, against 14.43 before it. It works, but rests on 2 days.
- **Momentum continuation off on gold and silver (from 8 Oct):** those trades lost 77.14 on 6–7 Oct. Moderately sure: before the reset the same trades made money.
- **Twin off:** weak evidence (2 days).

**Standing rules, applied to all history (436 trades, 15 days):**
- **Momentum continuation in a fast market on FX majors meets the switch-off rule:** 15 trades, down on 5 of 7 days, −20.60 after 16.38 commission. It is switched off in version 5, coming with tonight's build.
- **Squeeze release** also meets it (3 days, 2 down, −2.47). It hasn't traded since 30 Sep.
- **No market still switched on meets the exclusion rule.** EURUSD is the closest: down on 5 of 8 days. One more losing day would make it 6 of 9 and trigger the rule.

## Costs

| Bot (8 Oct) | Commission | Per trade | Share of the result before fees |
|---|---|---|---|
| Trend & Breakout | 4.90 on 8 | 0.61 | 35% of +14.04 |
| Rapid Scalper LIVE | 0.42 on 5 | 0.08 | lost before fees anyway. Spread and slippage (its estimate) were 2.74 of the −8.11 price result |
| Runner, Band Breaker | 0 (indices) | 0 | – |
| Crowd Fader | 0.06 | 0.06 | – |

Since the reset, Trend & Breakout has paid 28.76 in commission, **176% of what it made before fees**. FX alone paid 24.98 to make +7.91 before fees. Spread and slippage for Trend & Breakout and the other bots are not in the reports.

## Uptime

- 88% of the pulses that arrived said the bot was up. But **only 57% of the expected pulses arrived**: the Mac was asleep or offline for long stretches.

| When (UK) | Length | Why |
|---|---|---|
| 01:06–07:33 | 6 h 27 m | Mac repeatedly asleep (9 wake notices between 04:13 and 06:27), MetaTrader reconnect failing |
| 07:49–08:20 | 31 m | the bot's page and then the MetaTrader bridge not answering |
| 08:35–09:03 | 28 m | safe mode: MetaTrader not connected to the broker |
| 09:13–09:57, 14:03–15:10 | 44 m, 67 m | no pulse; reason not in the data. Band Breaker missed its 15:00 check |
| 15:29–15:39, 16:32–16:42 | ~10 m each | restarts when the Scalper went LIVE and then back to PAPER |
| **19:34–22:00** | **2 h 26 m** | **Mac asleep.** Band Breaker's USTEC trade was closed 63 minutes late and Crowd Fader stopped reading |

## Rapid Scalper

- **LIVE (5 trades, 0 won, −8.72):** every trade was a failed burst, closed within 2.5 seconds (median). 4 of the 5 never showed any profit. Micro-breakout has 0 wins in 8 tries across live and practice.
- **PAPER (12 trades, 3 won, −7.48):** wins average 2.28 against losses of 1.59, but it kept only 12.5% of its best points.
- It is being replaced by the Rapid Momentum Rider.

## Momentum Runner (every ride against the trade it copies)

| Entry (UK) | Trend & Breakout's exit | Runner | Mode |
|---|---|---|---|
| 7 Oct JP225 03:03 | +0.51 R | −1.00 R | PAPER |
| 7 Oct US30 11:03 | +1.41 R | **+7.67 R** (+56.67) | PAPER |
| 7 Oct US30 12:30 | −1.00 R | −1.00 R | PAPER |
| 7 Oct JP225 13:27 | −1.02 R | −1.00 R | PAPER |
| 7 Oct JP225 14:38 | +0.44 R | −1.00 R | PAPER |
| 8 Oct US30 07:38 | +0.46 R | **+2.57 R** (+24.96) | PAPER |
| 8 Oct US30 11:26 | +0.46 R | −1.00 R | PAPER |
| 8 Oct US500 18:27 | −1.01 R | −1.01 R (−8.78) | **LIVE** |
| **8 rides** | **+0.25 R (−0.09)** | **+4.23 R (+29.19)** | |

- 2 of the 8 rides reached the 3 R trail, both on US30.
- **Be clear about what this rests on:** without the one 7 Oct US30 ride, the Runner is behind (−3.44 R against −1.17 R). That is how the Runner is meant to work, a few big runs paying for many small losses. Two more checks point the same way:
  - Across all 14 index entries since the reset (the 5–6 Oct ones replayed against the price), the Runner's exit made **+114.89** against **+79.12** for Trend & Breakout's own exit.
  - Across 97 older index trades, the replay gives +29.1 R against +10.4 R.
- Its one LIVE ride doubled the US500 loss (−18.09 on one idea).

## Band Breaker

- 2 LIVE trades: US500 short at 13:30 New York time, stopped for −8.01; USTEC short at 14:30, closed at the session end for −1.22, but 63 minutes late because the Mac was asleep.
- USTEC's signal was refused 3 times because even the smallest size would have risked £17–26.
- 2 trades can't show whether wins are about twice the size of losses. **Over its days: −9.23, not yet positive.**

## Crowd Fader

- The key works, with no errors and no refused orders.
- Gold and silver leaned SHORT all day (scores around 71 and 61). US500, EURUSD, oil and Brent were never stretched. JP225 and GBPUSD had too few wallets to read.
- All 4 trades were short metals and all 4 hit their stop:
  - LIVE: gold −19.70, silver −9.30 after the roll.
  - PAPER: −13.81 on 2.
- Gold risked 19.70, twice the usual £10, because 0.01 lots is the smallest size.
- **Not positive yet.**

## Crowd context (evidence only)

The Crowd Fader's reads start at 08:54 UK on 8 Oct. Only 5 trades had a read: 3 of them were the same US500 decision, so only 3 separate reads. Too thin to say anything.

## What we can change

1. **Standing rule:** momentum continuation in a fast market on FX majors is switched off (version 5, in tonight's build).
2. **Your new line-up** (comes with the install): Momentum Runner, Rapid Momentum Rider (£1/pip) and Financial Ian LIVE; Trend & Breakout (still picking the Runner's entries), Band Breaker and Crowd Fader on PAPER. This also ends the "one idea, three positions" problem from the US500 short.
3. **Keep the Mac awake.** It slept for 2 h 26 m in the evening and most of the night. On the Mac, go to **System Settings → Battery → Options** and turn on **"Prevent automatic sleeping on power adapter when the display is off"**. Keep it plugged in with the lid open. The Windows VPS scripts remove this problem altogether.
4. **Higher scores aren't better.** Scores of 82–88 keep losing, so the extra size for the best trades is being tied to proven results, not raw score. The broken calibration warning (an impossible −137,976 R) is being fixed in the same build.
5. **Small record-keeping gaps:**
   - Crowd Fader's own rows leave out part of the commission (−19.64 against the broker's −19.70).
   - The Scalper's rows say −8.53, the broker says −8.72.
   - Trend & Breakout's journal summary says +11.59 against the broker's +9.14 (half the commission missing).
   - The page and the reports use the broker's figures.

## Plan for Friday 9 October

- Install the new build when I send the command. It brings the line-up above, the Rapid Momentum Rider at £1/pip and Financial Ian.
- Financial Ian stays in "no data" (it can't trade) until a futures data feed is connected.
- Keep the Mac awake all day.
- Watch the Rider's first live trades and the Runner's rides.
