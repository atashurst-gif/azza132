# Nightly review: Monday 5 October 2026 (first day after the reset to £2,000)

All figures are the broker's, after commission. Results count from the reset at 12:30 UK. The overnight trades before the reset are kept only as evidence.

## What we ended on

| | Trades | Wins | Win rate | Net after commission | Commission |
|---|---|---|---|---|---|
| **Trend & Breakout, version 2** | 4 | 3 | 75% | **+67.47** | 1.82 |
| Rapid Scalper (PAPER, simulated, not in the account) | 36 | 21 | 58% | -41.12 | 4.80 |
| **Account** | 4 | 3 | 75% | **+67.47** | 1.82 |

The account balance is £2,067.47 and the broker's equity agrees. No position is open. This is the first day since the reset, so there is nothing earlier to compare it with.

**Correction to my own earlier figure:** I had GBPCHF at +22.51 with about 0.60 commission. The broker's record is **+21.60 with 1.82 commission**. The total of +67.47 was right; the per-trade split was wrong, and the record is now fixed.

## Trend & Breakout, version 2: what is working

| Time (UK) | Market | Score | Result | R | How it ended |
|---|---|---|---|---|---|
| 12:44 | GBPCHF buy | 74.7 | +21.60 | +1.53 | target |
| 14:37 | US30 sell | 72.2 | +30.71 | +2.05 | target |
| 14:44 | US30 sell | 84.2 | -7.38 | -1.00 | stop |
| 15:04 | US500 buy | 79.2 | +22.54 | +2.27 | trailed stop, in profit (it had reached +3.24 R) |

| Breakdown | Trades | Net |
|---|---|---|
| Approach: momentum continuation in a fast market | 4 | +67.47 (the only approach that traded) |
| Indices (no commission) | 3 | +45.87 |
| FX | 1 | +21.60 (1.82 commission) |
| Score 70-75 / 75-80 / 80+ | 2 / 1 / 1 | +52.31 / +22.54 / -7.38 |
| Exit: target / trailed in profit / stop | 2 / 1 / 1 | +52.31 / +22.54 / -7.38 |

- The average win was **+1.95 R** and the average loss was **-1.00 R**. Commission averaged 0.46 per trade.
- No trades after 15:30 UK. Nothing scored 70 or more after that.

**Which rules are producing the result:**
- **Commission-free indices.** Three of the four trades were on indices, and they made 68% of the profit. This looks like the strongest rule.
- **Entry 70+.** All four trades cleared it. Wins came from 72-79; the one loss was the highest score (84). Scores above 80 are not better on their own.
- **The ladder.** It turned the US500 trade from +3.2 R at its peak into +2.27 R kept, not a loss.
- **Eight markets off and twin off.** Neither can be judged from one day: nothing was traded that these rules would have stopped.

**How sure:** 4 trades on 1 day. That is promising but not proof. The standing rule is not to change anything on one or two days.

**Useful contrast (before the reset, same day, old rules):** 8 overnight trades between 02:00 and 04:00 UK lost **-26.31** (4.14 commission). They were all FX and silver in the Asian session, and 5 of the 8 hit their stop at a loss. Under version 2 the winners came at 12:00-16:00 UK on indices. Time of day may matter as much as the market. It's worth watching, but it's too early to act on.

## Costs

| Bot | Commission | Spread | Slippage | Cost per trade | Costs as a share of the result before fees |
|---|---|---|---|---|---|
| Trend & Breakout v2 | 1.82 | in price (not split out in the report) | not split out | 0.46 | 2.6% of +69.29 |
| Rapid Scalper (paper) | 4.80 | 22.75 | 0.94 | 0.79 | the result before fees was already negative |

## Uptime since the reset (12:30 UK to 23:40 UK)

| When (UK) | Length | What |
|---|---|---|
| 12:29 | about 6 min | page not answering while the new build started; trader alive |
| 16:28 | about 6 min | page not answering; trader alive, so trading was not affected |
| 17:38-18:09 and 21:31-22:15 | 31 and 44 min | no pulse uploaded: unknown, probably the Mac asleep or busy. The bot was flat both times |
| 22:09 and 22:57 | moments | restarts (scalper and bot restarted); fine since |

**Right now (23:40 UK):** UP, MetaTrader connected, bridge answering, build **6909a35ca3**, flat.

The Mac is still on 6909a35ca3, so the commission display and the one-screen page (5e4035b472) are **not installed yet**.

## Rapid Scalper (PAPER)

**36 simulated trades, 14:12-15:54 UK, -41.12.** These ran on the build before the scalper rebuild. Since the rebuild started at 22:09 UK it has made 0 trades, because it is night-time.

| Ended by | Trades | Net |
|---|---|---|
| Idea failed (cut) | 10 | -65.51 |
| First stop | 4 | -35.66 |
| Profit trail | 8 | +41.24 |
| Momentum fading | 3 | +14.08 |
| Spread emergency | 11 | +4.73 |

| Market | Trades | Net |
|---|---|---|
| US30 | 14 | -13.90 |
| XAUUSD | 8 | -13.60 |
| DE40 | 10 | -9.90 |
| UK100 | 1 | -7.51 |
| US500 | 3 | +3.80 |

- **By hour (UK):** 14:00, 16 trades, -21.30; 15:00, 20 trades, -19.82.
- **On price alone, before spread, it still lost (-13.57).** So it does not beat its costs. The problem is direction, not only costs.
- **It has not earned LIVE.** It needs several days of the rebuilt version (looks at 1, 5 and 20 minutes and rides winners) showing a positive result after costs.

## What we can change

**Nothing yet: not enough evidence.**
- Version 2 is working on one day and 4 trades, so no rule changes.
- No approach or market meets the switch-off rules.

## Plan for Tuesday

1. Install **5e4035b472** for the one-screen page and the commission display. The bot keeps running the same version-2 rules.
2. Trend & Breakout: version 2 unchanged. Watch whether indices again carry the result, and whether trades with scores above 80 keep doing worse than 70-80.
3. Rapid Scalper: stays in PAPER on the rebuilt logic. Tomorrow is its first full day, and I'll judge it on that.
