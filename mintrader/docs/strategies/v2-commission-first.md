# Trend & Breakout, version 2: "Commission First"

Live from the reset to GBP 2,000 on 5 October 2026 (12:30 UK).
Results are counted from that moment; everything before it is the evidence
the rules came from, kept in the journals and in `reports/` on the reports
branch.

## The rules in force

| Rule | Value | Why (evidence) |
|---|---|---|
| Entry score | 70 or more | Under 70: 41 trades, -21 GBP. 70+: 63 trades, +73 GBP (28 Sep - 2 Oct) |
| Reward after costs | at least 1.8 times the risk | unchanged |
| Costs | at most 20% of the stop distance | 281 trades showed no cleaner line |
| Commission | never assumed zero; 6.00 a lot until learned, indices 0 | a new account had no history to learn from |
| Markets off | USDJPY, DE40, XAUGBP, CHFJPY, EURNZD, UK100, GBPJPY, XAUAUD | negative on two-thirds or more of 6+ days; 125 trades, -219.70 |
| Approaches off | session expansion in a squeeze, plus earlier switch-offs | negative on 3+ days traded |
| Twin (double target) | off | lost to its sibling on two of three days |
| Ladder (locks profit at +1R, +2R, +3R, +4R) | on for every trade | no trade that reached +1R ended a loss, 28 Sep - 2 Oct |
| Risk per trade | 0.5% of the account (about 10 GBP) | the user's setting |
| Daily loss stop | 3% of the account | never touched by the reviews |

## What we expect, so a change is noticeable

Replaying every past trade through these rules: 155 trades over ten days,
87 wins, +238 GBP, between 4 and 40 trades a day (about 15 typical), mostly
08:00 UK and 12:00 - 16:00 UK. Momentum continuation in a fast market was
the engine before the reset (47 trades, 30 wins, +80 GBP in the week to
2 October).

## Daily record (broker figures, after commission)

The nightly review adds one line per trading day.

| Day | Trades | Wins | Net | Commission | Best approach | Notes |
|---|---|---|---|---|---|---|
| 5 Oct (from 12:30 UK) | 4 | 3 | +67.47 | 1.82 | Momentum continuation, fast market (4 of 4) | GBPCHF +21.60 (1.82 commission), US30 +30.71, US30 -7.38, US500 +22.54 (trailed, peak +3.2R); indices +45.87 of it; nothing after 15:30 UK. Scalper paper -41.12 on 36 (old build). Overnight before the reset, old rules: 8 FX/silver trades 02:00-04:00 UK, -26.31 |

## Rapid Scalper (separate, in PAPER)

Simulated only; never in the account. It goes LIVE only when its result on
price clearly beats its costs over several days, and then possibly only in
the hours that earn (`entry_hours_utc`).
