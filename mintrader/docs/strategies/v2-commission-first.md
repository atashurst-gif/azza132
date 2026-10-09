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
| Approach off in one kind of market | from 8 Oct (version 4): momentum continuation on gold and silver crosses; from 9 Oct (version 5) also on the FX majors | gold/silver: negative on the last three days it traded there (-7.89, -48.52, -28.62; 6 days, 3 negative, net -52.94) while on indices (+52.71) and FX (+38.63) it is the engine. FX majors: negative on 5 of 8 days, 17 trades, -13.87 after 17.48 commission (see Version 5) |
| Top-opportunity size | from 9 Oct (version 5): momentum continuation on FX minor pairs risks up to 40 (under the 1.5% ceiling: 30 on 2,000) and at most 0.5 lots; every other trade keeps the 10 cap | 38 trades over 10 days, 8 up and 2 down, +156.31 after commission (+0.82 R a trade); checked again on the bot's own record before every such trade (see Version 5) |
| Twin (double target) | off | lost to its sibling on two of three days |
| Ladder (locks profit at +1R, +2R, +3R, +4R) | on for every trade | no trade that reached +1R ended a loss, 28 Sep - 2 Oct |
| Risk per trade | 0.5% of the account at the least (about 10 GBP), rising with the setup's tier and confidence towards the 1.5% ceiling; on 5-6 Oct STRONG setups risked 13-18 GBP, EXCEPTIONAL ones about 10. From 7 Oct capped at 10 GBP a trade (`risk.max_risk_money`), except a top-opportunity trade (version 5) | the user's settings (base 0.5%, maximum 1.5%) |
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
| 8 Oct | 8 | 5 | +9.14 | 4.90 | Session expansion in a trend: 3 trades +11.61 (XAGAUD +11.80, EURAUD +8.92 at target; XAGEUR -9.11) | Momentum continuation 5 trades -2.47 (GBPUSD +10.41, US30 +4.47 and +3.25; EURUSD -11.29, US500 -9.31); indices 3 trades -1.59, FX 3 +8.04, silver 2 +2.69. The account -37.29: the other bots' live trades -46.43, and one US500 short taken by three bots at 18:27 UK cost -26.10. Up +19.33 by 13:42 UK. The Mac slept 19:34-22:00 UK. Since the reset -12.40 over 42 trades (indices +79.12 on 14, everything else -91.52 on 28). Standing rule met over all history: momentum continuation in a fast market on FX majors (15 trades, down on 5 of 7 days, -20.60) - off from version 5 |

## Version 5 (9 Oct)

"Commission First (version 5)". The measuring clock is unchanged (still
from the 5 Oct reset; `STRATEGY_VERSION` untouched). Every version 4 rule
stays; four things are added.

### The evidence, and where it comes from

All of it is the broker's own record, nothing simulated:

* **Trades**: the nightly reports (`reports/<day>/trades.csv`) from 17 Sep
  to 8 Oct, 436 unique Trend & Breakout trades after removing repeats.
  Money is the broker's net after commission. The files for 29 Sep and
  2 Oct were not among them, so those two days (25 trades) are missing from
  every money table below; the replay has them.
* **The replay** (`python -m mintel.ops.potential`, run on Aaron's Mac on
  7 Oct): 435 closed trades to 7 Oct, each walked through the broker's
  one-minute bars for the 8 hours after its entry under ten riding rules.
  In R; commission left out.
* **The Momentum Runner's rows** for 7 and 8 Oct (`reports/<day>/runner.json`).

Everything before the 5 Oct reset was traded under older rules, so the
tables say how a kind of trade has behaved, not what version 4 would have
made. "Days" are the days a segment traded; "down" counts the days it lost.

**The whole record:** 436 trades, 47% winners, **-7.58 after 159.06 of
commission** (+151.48 before it). Since the reset: 42 trades, -12.40. The
98 of the 436 that version 4's rules would still allow ("today's rules"
below) made +259.54 (13 days, 2 down); under version 5, 81 trades, +273.41
(1 day down). Those rules were chosen by looking at this same record, so
both figures flatter.

#### Approach in each market condition (all 436)

| Approach / condition | Trades | Won | Net | Per trade | Avg R | Days | Down | Status |
|---|---|---|---|---|---|---|---|---|
| Momentum continuation / news | 13 | 69% | +62.25 | +4.79 | +1.07 | 3 | 0 | on |
| Session expansion / trend | 50 | 54% | +55.74 | +1.11 | +0.22 | 13 | 4 | on |
| Sweep reversal / fast market | 35 | 57% | +36.53 | +1.04 | +0.30 | 9 | 4 | off since 30 Sep |
| Momentum continuation / fast market | 113 | 53% | +31.46 | +0.28 | +0.17 | 12 | 6 | on (not gold, silver, now not FX majors) |
| Retest / squeeze | 16 | 50% | +5.52 | +0.34 | -0.16 | 9 | 5 | off |
| Session expansion / squeeze | 7 | 57% | +3.36 | +0.48 | +0.26 | 4 | 2 | off |
| Session expansion twin / trend | 15 | 60% | -5.39 | -0.36 | -0.02 | 2 | 1 | off (twin) |
| Breakout acceptance / trend | 9 | 44% | -8.52 | -0.95 | -0.49 | 5 | 3 | off |
| Retest / fast market | 6 | 33% | -12.85 | -2.14 | -0.37 | 5 | 3 | off |
| Breakout acceptance / fast market | 3 | 0% | -17.39 | -5.80 | -1.20 | 3 | 3 | off |
| Trend pullback / trend | 15 | 40% | -30.91 | -2.06 | -0.34 | 3 | 3 | off |
| Retest / trend | 101 | 38% | -36.66 | -0.36 | -0.07 | 8 | 6 | off |
| Momentum continuation / trend | 48 | 31% | -74.50 | -1.55 | -0.30 | 4 | 4 | off |

(Squeeze release 3 trades -2.47 and failed-breakout reclaim 2 trades -13.75
are left out of the table; both are small.)

#### Kind of market

| Kind | Trades | Won | Net | Commission | Per trade | Days | Down | In markets and approaches still on |
|---|---|---|---|---|---|---|---|---|
| Indices | 102 | 47% | +86.32 | 0.00 | +0.85 | 15 | 6 | 37 trades, +119.84, 3 of 12 days down |
| FX minor pairs | 141 | 45% | +28.75 | 92.99 | +0.20 | 14 | 5 | 53 trades, +181.09, 1 of 12 days down |
| Gold | 110 | 46% | -11.53 | 7.44 | -0.10 | 12 | 7 | 7 trades, +5.66 |
| Silver | 9 | 44% | -27.39 | 3.70 | -3.04 | 4 | 3 | 2 trades, +2.69 |
| FX majors | 74 | 49% | -83.73 | 54.93 | -1.13 | 13 | 10 | 24 trades, -13.03, 6 of 10 days down |

Commission: 0.74 a trade on the majors and 0.66 on the minors, against
0.07 on gold and nothing on indices. On the majors the fees were bigger
than the whole gross result (-28.80 before fees, -83.73 after).

#### Score bands and tiers (all 436)

| Score | Trades | Won | Net | Per trade | Avg R | Days | Down |
|---|---|---|---|---|---|---|---|
| 60-64 | 105 | 47% | -43.56 | -0.41 | -0.07 | 11 | 7 |
| 64-70 | 68 | 38% | -49.06 | -0.72 | -0.17 | 10 | 7 |
| 70-76 | 147 | 51% | **+104.71** | +0.71 | +0.17 | 14 | 5 |
| 76-82 | 68 | 49% | +34.51 | +0.51 | +0.16 | 14 | 7 |
| 82+ (EXCEPTIONAL) | 48 | 42% | **-54.18** | -1.13 | -0.03 | 13 | 11 |

Inside today's rules: 70-76 52 trades +214.33 (1 day down of 12); 76-82
25 trades +57.65; 82+ 21 trades -12.44 (6 of 8 days down, though +0.18 R a
trade before commission). Under version 5: 82+ 15 trades, +3.65. **A higher score has not meant a better trade.**
The best band is the lowest one still traded and the top band is the
worst, as the 6 Oct review first saw on four trades. So the raw score is
NOT used as "opportunity" for sizing (below). The 82+ band is not switched
off: inside today's rules its R is still positive and its losses in money
came partly from EXCEPTIONAL trades being sized bigger before the 10 cap.

#### Hour (UK, by entry)

| Hours | Trades | Won | Net | Days | Down | Inside today's rules |
|---|---|---|---|---|---|---|
| 00-06 | 99 | 40% | -33.97 | 11 | 7 | 13 trades, +23.09 |
| 07-11 | 154 | 45% | -3.82 | 13 | 6 | 34 trades, +70.57 |
| 12-16 | 151 | 52% | +23.75 | 13 | 7 | 47 trades, +163.25 |
| 17-23 | 32 | 50% | +6.46 | 11 | 5 | 4 trades, +2.63 |

Single hours are too thin to act on: 03:00 UK 14 trades -46.72, of which
the 5 today's rules allow -13.30; 07:00 UK 29 trades -33.19, of which 1 is
still allowed (+4.47). No time rule.

#### How trades ended

| Final state | Trades | Won | Net | Avg R |
|---|---|---|---|---|
| Strong flow (trailed or target) | 87 | 92% | +552.78 | +1.28 |
| Normal flow | 81 | 79% | +372.57 | +0.87 |
| Decay (locked in) | 42 | 88% | +92.03 | +0.60 |
| Proving | 53 | 40% | -128.70 | -0.35 |
| Reversal | 42 | 2% | -180.21 | -0.96 |
| Never got going (initial) | 131 | 0% | -716.05 | -1.02 |

Targets hit 85 times (+663.17), stops 346 times (-688.39).

* **Winners and their peaks.** Winners kept +1.16 R of a +1.61 R best
  (72%); since the profit floor (25 Sep evening) +1.11 R of +1.86 R (60%).
  Of 59 winners that were +2 R ahead, 7 kept less than 1 R.
* **Losers that had been +1 R ahead:** 29 of 155 before the profit floor
  (-68.77); **none of 76 since** - 77 trades reached +1 R after 25 Sep and
  not one ended in a loss. The floor works; it is unchanged.
* **Stopped within five minutes:** 72 of the 214 full stops (-422.22), in
  every kind of market (FX minors 23, gold 18, indices 17, majors 12). Most
  of the money lost is lost early, by trades that never got going. Not
  acted on: nothing in this data separates them before the entry.

#### The replay: does any riding rule beat the ladder? (R, 435 trades to 7 Oct)

| Kind | Trades | Ladder (as traded) | Trail 1R | Trail 2R | Trail 3R | Wide stop, trail 1.5R | Hold 2h |
|---|---|---|---|---|---|---|---|
| Indices | 97 | +10.4 | +13.2 | +20.0 | **+29.1** | +11.6 | -11.5 |
| FX minors | 145 | **+11.0** | -12.9 | -24.3 | -34.8 | +26.8 (at 1.5x the risk) | -4.5 |
| FX majors | 72 | -3.2 | -0.4 | -5.3 | -20.1 | +1.5 (1.5x risk) | -4.2 |
| Gold | 116 | -2.9 | +2.7 | -13.9 | -18.2 | -11.9 | -2.6 |

On indices, by approach (3 R trail against the ladder):

| Index trades | Trades | Ladder | Trail 3R | Days down under the trail |
|---|---|---|---|---|
| Momentum continuation | 33 | +4.8 | **-14.4** | 7 of 9 |
| Everything else (retest, session expansion, squeeze release, sweep...) | 64 | +5.5 | **+43.5** | 3 of 13 |

So the 3 R trail's whole edge on indices came from approaches other than
momentum continuation; on momentum continuation every riding rule lost.
On FX and metals no rule beats the ladder once the wider stop's extra risk
is counted. **The ladder stays for Trend & Breakout everywhere**; the
riding is the Momentum Runner's job, and from version 5 it skips momentum
continuation (docs/strategies/momentum_runner.md).

### HIGHEST OPPORTUNITY: the definition

A segment - one approach in one kind of market, in the conditions it is
still allowed to trade - is "highest opportunity" when its own closed
trades show:

1. at least **20 trades over at least 4 trading days**;
2. **positive expectancy after costs**: net money per trade above zero
   (broker figures, commission included) and average R above zero;
3. **more winning days than losing ones** (the standing rules judge by
   losing days, so the definition does too).

The score is not part of it (see the score table).

Every segment that meets the sample size, in markets and conditions still on:

| Segment | Trades | Days | Up / down days | Net | Per trade | Avg R | Highest opportunity? |
|---|---|---|---|---|---|---|---|
| **Momentum continuation, FX minor pairs** (fast + news markets) | **38** | **10** | **8 / 2** | **+156.31** (29.89 commission) | **+4.11** | **+0.82** | **yes** |
| Momentum continuation, indices (fast market) | 26 | 10 | 4 / 6 | +42.82 | +1.65 | +0.16 | no: more losing days than winning |

The qualifying segment is not one lucky day: without 1 Oct (13 trades,
+75.21) it is still 25 trades, +81.10, +0.62 R. Every score band inside it
is positive (70-76: 18 trades +106.39; 76-82: 10, +29.19; 82+: 10,
+20.73). Spread over 14 pairs; GBPCHF (+69.47 on 9), EURCHF (+33.79 on 5)
and AUDJPY (+25.21 on 7) carry most of it, EURCAD (-17.16 on 2) and NZDCHF
(-8.11 on 2) are its losers. Measured spread of the per-trade result: the
mean is about 2.9 standard errors above zero in money and 3.6 in R - a
real signal on this sample, not proof.

### The top-opportunity size

Aaron, 8 Oct: *"increase their pip size to 0.5 maximum for trades showing
the highest opportunity."* Read as: **up to 0.5 LOTS**.

| | Normal trade | Top-opportunity trade |
|---|---|---|
| Who | everything else | momentum continuation on an FX minor pair, while that segment's own record still qualifies |
| Money at risk at the stop | at most 10 (`risk.max_risk_money`) | up to 40 (`risk.top_risk_money`), but never above 1.5% of the account (`risk.max_risk_pct`): **30 on a 2,000 account** |
| Lots | whatever the 10 buys | at most **0.5** on FX and metals (`risk.top_max_lots`); indices have no lot cap (a lot is a different thing there) but the same money cap |
| The stop | where the chart puts it | the same: a bigger size never moves the stop |
| Daily loss stop (3%), drawdown, exposure, margin, reject-storm and order-rate breakers | apply | apply, unchanged |
| Correlated exposure cap (2% = 40 on 2,000 per currency bet) | applies | applies: a second CHF-short trade beside a 30 one gets 10 at most |
| Total exposure cap (4% = 80) | applies | applies |

How it is decided (`RiskManager.top_opportunity`, called from
`Trader.act` before sizing): the setup's approach and kind of market
(`contracts.infer_group`, as the scanner uses it) must be listed in
`risk.top_segments`; then the segment's newest 40 closed trades in the
journal (`risk.top_lookback_trades`; markets in `excluded_symbols` and
conditions in `disabled_tactic_regimes` left out) must pass the three tests
above (`top_min_trades` 20, `top_min_days` 4). When they do not - too few
trades, losing money, or more losing days than winning - the trade is
sized like any other and the reason is written to the ENTRY event
(`sizing_reasons`, and `top_opportunity: true/false`). So the extra size
switches itself off if the segment stops earning. It is off under the
CONSERVATIVE aggression setting, and `risk.top_size_enabled: false` turns
it off entirely. Nothing about it can see the last trade's result; there
is no size increase after a loss.

The Momentum Runner copies the real trade's volume, so it would scale with
a top-size index trade - but never above that volume (pinned by a test).
With version 5's defaults the top segment is an FX segment and the Runner
rides indices only, so today the two never meet.

**Note for Aaron:** with the 1.5% ceiling, 40 is reachable only once the
account is above about 2,667. Raising `max_risk_pct` to 2.0 would let a
top trade risk the full 40 on 2,000; that is the operator's boundary and
is left as it was.

### Rule changes in version 5

| Change | Setting | Evidence |
|---|---|---|
| Momentum continuation off on the FX majors | `scan.disabled_tactic_groups` + `("MOMENTUM_CONTINUATION", "FX_MAJOR")` | standing rule (negative on 3+ days traded): in the majors still traded (GBPUSD, EURUSD, AUDUSD, NZDUSD, USDCAD) in the conditions it still trades: 17 trades, negative on 5 of 8 days (17 Sep +6.73, 22 Sep -4.81, 23 Sep +7.27, 1 Oct +7.07, 5 Oct -5.97, 6 Oct -11.99, 7 Oct -11.29, 8 Oct -0.88), net -13.87 after 17.48 commission (+3.61 before it); the last four days all down. EURUSD -20.13 on 8. The minors stay on |
| Top-opportunity size | `risk.top_*` | above |
| Calibration that one broken trade cannot poison | `engine/opportunity.py`, `engine/journal.py` | below |
| Momentum Runner skips momentum continuation | `runner.skip_tactics` | docs/strategies/momentum_runner.md |

**The calibration bug.** On 8 Oct the log said `band 76-82 (E=-136390.07R)
underperforms 70-76`. One trade's R was enormous because its "1 R" was
almost nothing: when the bot restarts and a position's FlowLock tracker
has to be rebuilt, it took the broker's CURRENT stop as the initial one -
and by then the ladder had pulled that stop up to the entry. The score
calibration (which feeds the confidence used in sizing), Kelly and the
historical-match evidence all averaged that R. Fixed three ways:
(1) a rebuilt tracker takes the stop the trade was opened with from the
journal (`Trader._original_stop`); (2) the calibration measures each
trade's R against that opening stop, leaves out a trade whose planned risk
was (almost) nothing or sat on the profit side, and Kelly and the
historical match average R held to -2..+6; (3) the calibrator holds every
R to -2..+6, ignores values that are not numbers and believes a band's
average only from 5 trades (`min_band_samples`). A test reproduces the
8 Oct outlier (tests/test_version5.py).

### Looked at and NOT changed

* **Markets:** no market still on is negative on two-thirds of 6+ days.
  Closest: EURUSD 5 of 8 (62%, -11.49), GBPAUD 3 of 5 (+0.52). Watch.
* **82+ scores:** worst band on record (above), but inside today's rules
  +0.18 R a trade before fees; not switched off. Watch.
* **Momentum continuation in a fast market overall** is negative on 6 of
  12 days but +31.46, and +155.74 on the indices and FX minors that remain;
  by the 25 Sep precedent a net-positive line stays.
* **Exits:** the ladder stays (replay above). The quick stop-outs are the
  biggest single loss line but nothing on hand tells them apart in advance.
* **Hours:** nothing robust.

### NOT YET EVALUATED

* **The top size itself.** In R the qualifying segment earned +0.82 a
  trade; whether a bigger size keeps that (wider spreads on bigger orders,
  the correlated cap trimming clusters such as the thirteen trades of
  1 Oct) is unknown until it trades. The nightly review should show top
  trades separately (`top_opportunity` on the ENTRY event).
* **The Runner's "enter only once the real trade has proved itself at
  +x R"**: built (`runner.confirm_r`, off by default) and replayable on
  Aaron's Mac from the broker's minute bars with one command:
  `python -m mintel.ops.potential --config ~/MarketBot/data/config.json --runner`.
  The data here has no bars, so it has not been run.

## On PAPER from the 9 October line-up

Aaron's line-up puts Trend & Breakout on PAPER (the weaker of it and the
Momentum Runner on the same index entries; the figures behind the decision
are in docs/strategies/tnb_paper.md) with `"tnb": {"mode": "PAPER"}`. The rules
above are unchanged and still decide every trade on real prices and the
real account; only the orders are simulated (docs/strategies/tnb_paper.md).
From the switch, the record above is a paper record. A real trade still
open at the switch is wound down at the real broker to its natural end.

**Not part of version 5's record: runner-feed trades.** While on paper the
scanner may also offer, on indices only, approaches its own rules have
switched off, so the Momentum Runner keeps receiving them
(`runner.feed_tactics`, docs/strategies/momentum_runner.md). Those paper
trades are tagged `runner_feed` in the journal and must be left out of
this record (`mintel.runner.feed.is_feed_trade`). The list is **empty** on
the evidence (no approach qualified on trades today's rules still allow),
so for now there are none.

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

## Momentum Runner (separate; LIVE from 8 October)

From 7 October: a paper shadow of this bot's index trades, same entry and
size, stop left alone until +3 R then trailed 3 R behind the best price.
LIVE on the demo from 8 October with its own orders (magic 990511), and it
stays LIVE with this bot on paper: it still opens its own real order beside
each fresh index entry. Its results are its own, never part of this
record. Why, and its settings: docs/strategies/momentum_runner.md.

## Band Breaker (separate, in PAPER)

From 7 October: intraday index momentum from the published noise-band rule
(Zarattini, Aziz, Barbon 2024), New York session, flat before the close.
Never in the account. docs/strategies/band_breaker.md.
