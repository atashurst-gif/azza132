# Nightly review: Wednesday 7 October 2026

**Tonight's upload did not arrive, and nothing has come from the Mac since 13:33 UK.** The ten-minute pulse (a separate job from the bot) stopped at the same moment, which points at the Mac itself: asleep, off, or offline. The bot was installed and restarted at 13:38 UK; the next pulse never came. Everything below is from the last upload, at 13:33 UK. Please check the Mac first thing.

## What we ended on (to 13:33 UK)

| | Trades | Wins | Net after commission |
|---|---|---|---|
| **Trend & Breakout** | 11 | 5 | **-26.81** (the broker's figure) |
| Rapid Scalper (PAPER, version 2 until 13:38) | 16 | 5 | +3.56 after 15.16 of costs |
| Momentum Runner (PAPER) | 3 closed, 1 open | 0 | -27.77 closed; US30 open at +60.89 |
| Band Breaker (PAPER) | 0 | - | its session had not opened yet |
| **Account** | 11 | 5 | **-26.81**; equity 1,998.95 |

| Since the reset | Trades | Net |
|---|---|---|
| 5 Oct (from 12:30 UK) | 4 | +67.47 |
| 6 Oct | 12 | -40.92 |
| 7 Oct (to 13:33 UK) | 11 | -26.81 |
| **Total** | **27** | **-0.26** |

## Trend & Breakout (first half day on the 10 cap)

| Time (UK) | Trade | Approach | Net |
|---|---|---|---|
| 03:11 | JP225 | momentum, fast market | +5.12 |
| 08:52 | EURCHF | session expansion | +10.41 |
| 08:59 | EURCAD | momentum | -11.27 |
| 09:04 | EURUSD | momentum | -10.63 |
| 11:26 | US30 | session expansion | +10.43 |
| 11:38 | XAUCHF | momentum | -7.95 |
| 11:51 | GBPCHF | momentum | +3.99 |
| 12:31 | US30 | momentum | -8.39 |
| 13:31 | XAUEUR | momentum | +4.12 |
| 13:31 | JP225 | momentum | -9.52 |
| 13:33 | XAUJPY | momentum | -10.33 |

(Per-trade figures are the bot's records; the day total is the broker's. The upload with scores, R and commission per trade did not arrive.)

- **The 10 cap worked:** every loss was about 10, and the two best wins were about +1 R (10.41, 10.43). Yesterday's losses averaged 15.
- **By approach:** session expansion 2 trades, +20.84; momentum continuation 9 trades, -31.56.
- **By market:** indices 4 trades -2.36; FX 4 trades -7.50; gold crosses 3 trades -14.16.
- **Shape:** 5 wins averaging +6.81, 6 losses averaging -9.68. Three wins were small (+4 to +5) because the ladder trailed them out early.
- **What is producing the result, three days in:** indices are still the only group positive over the three days (+76.85 on 10 trades). Momentum continuation in a fast market has now been negative two days running after a strong first day; it is net positive on its history (+91 over 11 days), so under the standing rule and the precedent it stays on.

## Costs

Trend & Breakout's commission by trade is in the upload that did not arrive. The scalper paid 15.16 across 16 trades (0.95 a trade), mostly spread on gold.

## Uptime

77 pulses to 13:33 UK; two short page blips (01:16 and 12:33 UK) with the trader alive both times. After 13:33 UK: nothing. **Right now: unknown.**

## Rapid Scalper (PAPER)

Version 2 (pullback) ran until 13:38 UK: 16 trades, 5 wins, +3.56 after costs. Average win 11.53 against average loss -4.91 (2.35 to 1). US30 +28.62 on 3 trades; gold -22.73 on 10; DE40 -8.12; US500 +5.79. 11 of 16 were cut early. Version 3 (the High-Velocity Session Rider) took over at 13:38 UK; its first hours are in the missing upload. Not earned LIVE.

## Momentum Runner (PAPER)

| Shadow | The ladder kept | The Runner |
|---|---|---|
| JP225, 03:11 | +5.12 | -10.06 (full stop) |
| US30, 12:31 | -8.39 | -8.36 (full stop) |
| JP225, 13:31 | -9.52 | -9.35 (full stop) |
| US30, 11:03 | +10.43 | **open at +60.89, +8.2 R, peak +9.4 R; the trail has locked about +47** |

Exactly the shape the replay predicted: more trades going to the full stop (no break-even move), paid for by one trade that ran nine times further than the ladder kept. Over the four, the Runner is ahead by roughly 20 on paper if the trail holds. Two days is not a verdict.

## Band Breaker (PAPER)

No trades by 13:33 UK (its session opens 14:30 UK). Whether it traded in the afternoon is in the missing upload.

## What we can change

**Nothing tonight.** The day's full record is missing, and no rule has three days of evidence against it under the standing rules.

## Plan for Thursday

1. **Check the Mac.** Open http://127.0.0.1:8787. If the page is up, the pulse job may need the install command run once more (answer N to "Change them?"). If the Mac was asleep, the bot resumes by itself when it wakes; wake it and confirm the green bar.
2. Run the clock-restore command from this afternoon if it has not been run yet (the Trend & Breakout tab should say "since 2026-10-05 11:30 UTC").
3. Once the Mac is back, I will re-read today's records and update this review.
4. Thursday is the first full day for all three paper bots side by side.
