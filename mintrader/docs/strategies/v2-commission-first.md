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
| Markets off | USDJPY, DE40, XAUGBP, CHFJPY, EURNZD, UK100, GBPJPY, XAUAUD; from 8 Oct (version 4) USDCHF and XAUJPY | negative on two-thirds or more of 6+ days; 125 trades, -219.70; USDCHF 5 of 6 days (-38.77), XAUJPY 7 of 10 (-14.09) |
| Approaches off | session expansion in a squeeze, plus earlier switch-offs; from 7 Oct (version 3) breakout acceptance in a fast market | negative on 3+ days traded |
| Approach off in one kind of market | from 8 Oct (version 4): momentum continuation on gold and silver crosses | negative on the last three days it traded there (-7.89, -48.52, -28.62; 6 days, 3 negative, net -52.94) while on indices (+52.71) and FX (+38.63) it is the engine |
| Twin (double target) | off | lost to its sibling on two of three days |
| Ladder (locks profit at +1R, +2R, +3R, +4R) | on for every trade | no trade that reached +1R ended a loss, 28 Sep - 2 Oct |
| Risk per trade | 0.5% of the account at the least (about 10 GBP), rising with the setup's tier and confidence towards the 1.5% ceiling; on 5-6 Oct STRONG setups risked 13-18 GBP, EXCEPTIONAL ones about 10 | the user's settings (base 0.5%, maximum 1.5%) |
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
| 6 Oct | 12 | 4 | -40.92 | 14.12 | Indices again: US30 +35.49 (session expansion), US500 +7.91/-10.06 | Indices 3 trades +33.34; FX 6 trades -25.74 (12.40 commission); gold/silver crosses 3 trades -48.52. Scores 80+ 0 of 3. 6 trades between 12:27 and 13:47 UK: -28.53. Stops 8 (-122.45), trailed in profit 4 (+81.53), no target hit. Scalper paper (Pullback v2) -31.75 on 48 |
| 7 Oct | 18 | 8 | -48.09 | 7.92 | Session expansion in a trend: 3 trades +9.38 (EURCHF +9.83, US30 +10.43) | Momentum continuation 15 trades -57.47; metals 8 trades -28.62, FX 5 -20.97, indices 6 +5.97 (4 wins); the 10 cap held every loss to about 10 (avg -9.43) but wins averaged only +5.78 (0.7 R); 8 trades in the 13:00 UK hour lost -30.11; no target reached, 8 trailed out in profit, 10 stopped; since the reset -21.54 over 34 trades. Uploads stopped 13:38-07:47 UK (the GitHub token was dropped by re-entering settings); the bot itself ran all day |


## Rapid Scalper (separate, in PAPER)

From 7 October (evening): "High-Velocity Session Rider (version 3)" -
seconds-level acceleration in scalpable markets within session windows,
failed scalps cut in seconds, winners protected by a rising floor and
ridden with a momentum-driven trail. docs/strategies/rapid_scalper.md.
Before that, from 6 October: "Pullback (version 2)" - gold and commission-free indices
only, with the 20-minute trend, entered after a one-minute pullback, stop
beyond the pullback. Why: docs/strategies/rapid_scalper.md.

Simulated only; never in the account. It goes LIVE only when its result on
price clearly beats its costs over several days, and then possibly only in
the hours that earn (`entry_hours_utc`).

## Momentum Runner (separate, in PAPER)

From 7 October: a paper shadow of this bot's index trades, same entry and
size, stop left alone until +3 R then trailed 3 R behind the best price.
Never in the account. Why, and what would earn it real money:
docs/strategies/momentum_runner.md.

## Band Breaker (separate, in PAPER)

From 7 October: intraday index momentum from the published noise-band rule
(Zarattini, Aziz, Barbon 2024), New York session, flat before the close.
Never in the account. docs/strategies/band_breaker.md.
