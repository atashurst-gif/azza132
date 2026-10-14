# Nightly review: Wednesday 14 October 2026 (full working)

## State
- **Build:** the Mac runs 71cbd0be44, which is commit e1088e03, the 14 Oct upgrade. Aaron installed it this evening.
- **Up:** yes. MetaTrader is connected and the bridge is answering.
- **Account:** equity £1,840.54, no open positions. Since the reset: −£159.46.
- **Real money:** `real_trading_enabled: false` (reports/live/readiness.json).

## DEMO
- **Today:** £0.00, nothing on demo. All old bots are on PAPER and no lab idea has been promoted yet.
- **Tonight's daily pick** (21:33 UTC): none. The reason given: "nothing has proved itself". Every one of the top 10 ranked ideas shows "NOT ENOUGH DATA". They have 2 UK days of forward practice and need 3, plus 10–15 trades.
- **Demo execution:** verified only on the simulated broker so far. No real demo order has been placed yet.

## PAPER today

**Original bots** (their own records, after costs):

| Bot | Trades | Wins | Net | Notes |
|---|---|---|---|---|
| Band Breaker | 2 | 2 | +42.03 | US500 +24.19 and USTEC +17.84, both held to the flat |
| Crowd Fader | 4 | 3 | +40.76 | Three targets on gold and silver, plus one later loss |
| Trend & Breakout | 8 | 5 | +23.73 | All momentum continuation in a fast market |
| Momentum Runner | 0 | – | 0 | |
| Financial Ian | 12 | 4 | −18.82 | |
| **Total** | **26** | | **+87.70** | |

**Lab:** 632 ideas practising on 24 markets, with feed OK. Per-idea results come from the daily pick's ranking (reports/live/weekly.json). The lab's pool-wide total isn't published yet, so it isn't reported.

**Top forward records, 13–14 Oct:**
- New York 30-minute opening range on indices, with a swing stop and a 2:1 target: 7 trades, 6 won, +£131.18, mean +1.44 R. A news-avoiding twin is identical, which is expected when no news was near.
- Eight 120- and 240-minute breakout variants on minor pairs, London and New York, all with a 2R trail: **identical** records, 7 trades and +£104.26 each.

## Investigation started tonight
1. **Identical breakout records.** These ideas differ in lookback AND session, and their 2-month past tests differ (e.g. London 120: 338 trades, −£859.82; New York 120: 327 trades, −£715.94). Two days identical makes a bug in the live practice path likely: session, lookback, or a mix-up in the trade records. A workflow is root-causing it with a reproduction and will fix it with a regression test before any of these ideas can be promoted. That matters because the lab-demo engine reuses the practice code.
2. **Shrinkage calibration.** The idea with mean +1.44 R over 7 trades is shrunk to −0.07 R, which puts about 8% weight on its own data. The audit checks this for genuine bugs, such as duplicates counted in the pool or the wrong variance, and works out how many trades a genuinely good idea needs before it can qualify. If it's a strictness choice rather than a bug, the trade-off goes to Aaron.

## Standing rules
- **Trend & Breakout, momentum continuation in a fast market:** −12.41, −37.08 and +23.73 on 12–14 Oct, so 2 of 3 days down. The 3-day rule is not met. Keep.
- **Markets:** no market rule met.

## Live readiness
Stage: none, "No action required yet". Nothing was sent.

## Records
Daily-record rows for 14 Oct were committed in 2fdd6421.
