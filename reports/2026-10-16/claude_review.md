# Nightly review: Friday 16 October 2026 (full working)

## State
- **Build:** the Mac runs 2cb8120aee, which is commit 632d6112 (the streak fix), the newest on claude/eager-newton-ra06qu. Two installs today:
  - 09:12 UK: 913e4f796d, with the "Fast" rule and Trend & Breakout version 6;
  - later in the day: 2cb8120aee.
- **Up:** 123 of 125 pulses were UP (98%). There were two single-pulse gaps:
  - 09:11 UK: the restart for the install;
  - 20:55 UK: the page was slow to answer.
- **Silent since 22:17 UK:** the 22:10 UK day upload arrived. The last pulse was at 22:17 UK, and nothing has come since, so tonight's 22:30 UK pick hasn't been reported. The Mac is probably asleep or off.
  - weekly.tick makes the day's pick at its next check, as long as that is before the release (Monday 06:30 UK). So a Friday pick made over the weekend still releases on Monday.
- **Account:** equity £1,840.54. No open positions and nothing traded with real money. Since the reset: −£159.46.
- **Real money:** `real_trading_enabled: false`.

## DEMO
- **Today:** £0.00. Nothing is on demo.
- **Tonight's pick:** not reported (see above). Last night's (15 Oct, v1) was none.
- **Demo execution:** verified only on the simulated broker. No real demo order has been placed yet.

## PAPER today (the bots' own paper records, after costs)

| Bot | Trades | Wins | Net | Notes |
|---|---|---|---|---|
| Band Breaker | 4 | 2 | +30.25 | USTEC −3.58 and −8.91 (trailed). Then US500 +20.97 and USTEC +21.77, both shorts held to the 15:55 NY flat (about +2.3 R). Third profitable day in a row: 14–16 Oct +42.03, +8.29, +30.25 |
| Momentum Runner | 0 | – | 0 | Nothing to ride |
| Financial Ian | 12 | 4 | −11.59 | Wins +0.66 to +3.39; losses −0.50 to −4.27. 7 closed by its thesis rule, 5 by stops |
| Trend & Breakout | 6 | 3 | −18.98 | 7.88 simulated commission |
| Crowd Fader | 4 | 1 | −20.18 | XAGUSD −9.51 and −9.90, XAUUSD −15.79 (all stops); XAUUSD +15.02 at target |
| **Total** | **26** | **10** | **−20.50** | |

**Trend & Breakout:**
- Momentum continuation in a fast market: 2 trades, −9.65.
  - JP225 SELL at 00:01 UTC, −13.94, stopped in under half a minute. This was before the 09:12 UK restart onto version 6.
  - GBPNZD +4.29.
- Session expansion in a trend: 4 trades, −9.33. GBPUSD +3.14 and −15.25, XAUUSD −1.44, GBPNZD +4.22.
- **Version 6 is working:** no momentum continuation on an index after the restart.

## The lab
- 632 ideas are practising (`practise_all`) on 24 markets, with the feed OK.
- The upload lists only the top 12 by today's result (566 left out). All 12 were in profit, and all are New York ideas on the indices or major pairs:
  - **Trend pullback, 15-minute bars (20/50 averages), indices, 0.5 swings:** +£208.23 from 24 trades. The 0.25-swing sibling made +£182.16 from 23.
  - **120-minute New York breakout on indices, 1-swing trail:** +£135.93 from 22. The news-avoiding twin is identical, which is expected when no news was near.
  - **60-minute New York breakout on indices, 2:1 target:** +£127.29 from 22 (twins).
  - **60-minute New York breakout on majors, 1-swing trail:** +£120.72 from 19 (twins).
- These are one day's practice. Their whole forward records come with the pick.
- **Sanity check:** the identical pairs differ only by the news option, so that is expected. No ideas with different sessions or lookbacks matched.

## Standing rules: one met → Trend & Breakout version 7

**The market rule is met for five markets.** The rule takes a market off when it is negative on two-thirds or more of 6+ traded days. The count is the coach's S2 (`mintel/coach/rules.py`, the rule as code), run on the Mac's whole Trend & Breakout record, real trades:

| Market | Days traded | Days negative | Net |
|---|---|---|---|
| CADJPY | 6 | 4 | −10.82 |
| EURCAD | 6 | 5 | −36.85 |
| EURUSD | 12 | 8 | −30.41 |
| GBPAUD | 7 | 5 | −4.59 |
| GBPCAD | 6 | 4 | −10.38 |

→ **"Commission First (version 7)":** these five are added to `universe.excluded_symbols`.
- Every version 6 rule is kept, and the measuring clock is unchanged.
- Trend & Breakout is on PAPER, so no real order changes. The lab keeps every market.

**What this count is.** It is the literal rule as versions 4 and 5 read it: every trade in the market, DEMO trades, the Mac's whole record. Most of these markets' losing trades came from approaches already switched off. Of EURUSD's trades on the reports copy, none would be allowed today. EURCAD's evidence is mostly behaviour today's rules still allow. So version 7 follows the rule and its precedent. It does not prove these markets lose under version 6. Counting markets only on today's-rules trades would be a refinement that needs Aaron's word.

**My miss.** The coach has listed these five since 12 Oct, yet my 13–15 Oct reviews said "no market rule met". They counted from the reports branch's `trades.csv` copy, which has fewer days: it starts on 17 Sep, while the Mac's demo record goes back to about 14–16 Sep, and it lacks 29 Sep and 2 Oct. From tonight the market check reads the coach's S2 list.

**Approaches**, counted as for versions 4–6 without the five markets (the trades today's rules allow; DEMO to 8 Oct + PAPER since, combined as the rule input):

| Line | Combined | Verdict |
|---|---|---|
| Momentum continuation, minor pairs | 12 days, 2 down, +96.45 (DEMO +165.54, PAPER −69.09) | Stays |
| Momentum continuation in news | 3 days, 1 down, +5.70 | Not met |
| Session expansion, minor pairs | 7 days, 1 down, +29.47 | Not met |
| Session expansion in a trend | 10 days, 2 down, +74.58 | Stays |

**Watch:** JP225 is DEMO 1 day (−0.54) and PAPER 4 days, 3 negative (−34.46). All of it is momentum continuation, which has been off on the indices since version 6. On S2's count it needs 2 more days before the rule can apply.

**Verification:**
- tests/test_version7.py was added, and the version 4–6 name pins were updated. One dashboard test used EURCAD as a made-up minor-pair trade; it now uses EURAUD, because excluded markets leave the Formula 1 segment by design.
- Full suite: only the known failures. The day-one stop-floor and Rapid Scalper period tests fail on the unchanged code too, and the end-to-end trade test passes on its own.
- Three independent checks:
  - The first caught my count of markets on the wrong basis and an exception applied to one trade.
  - The second confirmed the code is sound and the Mac's settings cannot override the list. It noted that the coach's count includes trades from approaches already off; that is stated above.
  - The third caught that I had recounted the approach lines with only one of the five markets removed, and that I had misdescribed the missing days.
- All corrected above.

## Road to Profit (paper, not money)
- **Closest to demo:** NY opening range on indices, +£113.08 from 11 trades over 3 days as of 15 Oct. It needs 15 trades.
- **Today's lab leaders:** the New York index ideas above.
- **No projection:** £25/£50/£100 a day on £1,500 is not projected from 3 days of practice. The demo bar exists to prove it first.

## Live readiness
Stage: none, fingerprint 8ed204f367dce7e8, notify false. Nothing was sent.

## Records
- The 16 Oct daily-record rows are in all five strategy docs (commit a81d767b).
- Version 7 is in mintel/config.py, tests/test_version7.py, the Version 7 section of docs/strategies/v2-commission-first.md and the ledger. Commit a81d767b, build stamp 5855514ceb.

## For Aaron
Install version 7 any time before Monday 06:30 (markets are shut for the weekend). If the Mac is closed or off, opening it lets Friday's pick run.

## Addendum, Saturday 17 October: version 7 superseded by version 8

**The pick ran.**
- `weekly.json`, uploaded when the Mac woke at 06:22 UTC, shows that Friday's plan was made at 21:34 UTC on 16 Oct (22:34 UK). The last pulse before the gap was 21:17 UTC.
- The plan is PLANNED, with release at 05:30 UTC on 19 Oct (Mon 06:30 UK). Its picks are two lab twins:
  - opening range, indices, New York 30 min, swing stop, 2:1;
  - the same idea with "avoids news".
- Each twin has 15 trades over 4 UK days: net +£131.92, profit factor 2.74, win rate 60%, shrunk +0.03 R a trade against the best clock trade's −0.14 R.
- Both twins take the same entries when no news is near. So each signal is taken twice, each at its own demo tier. This is inside the diversity cap (2 per INDEX/BOTH) and the 10% combined-risk cap.
- Real demo orders get their first check against the broker on Monday.

**Version 8 replaces version 7.**
- Aaron (17 Oct): "Keep the approaches off and continue with how we should be if they weren't there".
- The market rule is now counted like the approach rule: only the trades today's rules still allow (`engine.tactics.allowed_by_rules`, which mirrors the scanner, with the market list set aside).
- Claude's reading, not Aaron's words: today's rules include the 15-minute news window (10 Oct), so a trade from before 10 Oct entered inside it does not count either. Every allowed trade before 10 Oct was screened against the regular release times of both its currencies. Six were entered in the 15 minutes before a slot. Leaving them out changes no verdict except session expansion on the minor pairs. There, the 9 Oct GBPCAD ticket 960000007, entered 8 minutes before Canada's jobs data, is the only flagged trade, so it is left out by hand. Counted (the count of versions 4 to 6), that line is 9 days, 3 down, −5.93, which meets the approach rule; without it, 8 days, 2 down, +26.54. Aaron is told both figures.
- The coach does not apply the news window: the Mac's calendar archive cannot say it reliably for the days before 10 Oct (next point).
- **A news bug, fixed (pre-existing).** The public calendar feed has no ids, so the archive kept ONE row for all the releases sharing a time, with the first one's currency. The 15-minute window could therefore miss, say, US payrolls when Canada's jobs data shared its minute, and the lab's news-avoiding ideas could too. Each release now has its own row, from the first refresh after the install that reaches the feed (commit 169c1d69 and its review fixes).
- On the reports copy (combined DEMO+PAPER, labelled), none of the five meets the rule:
  - EURUSD: none of its 13 trades is allowed today;
  - CADJPY: 1 day;
  - GBPAUD: 3 days, 2 down;
  - GBPCAD: 3 days, 2 down;
  - EURCAD: 4 days, 3 down.
- So they are back on, and `excluded_symbols` is version 6's ten again. Every approach switch-off is unchanged.
- Version 7 never ran on the Mac. Version 8's trading rules are exactly those of the Mac's current build (2cb8120aee). The news fix makes the existing news rules see every release.
- The coach's S2 now uses the same approach filter, with twins flagged. It publishes `market_rule` in `coach.json`: each market's combined days, negative days and real/practice split. The nightly review takes the GBPCAD news-window trade out of it by hand (GBPCAD's days and down days are the same either way).
- **EURCAD may meet** on the Mac's full record, which has DEMO days the reports copy lacks. Its 7 Oct trade (−12.07, 2 minutes before 08:00 UTC) is one of the six, beside a slot with no known high-importance release, so it counts. The next review reads `market_rule` to settle it.

**The earlier "My miss" paragraph.** Its point about the coach's list still holds. But that list counted every trade, which is the reading version 8 drops.

**Build:** version 8, the news fix and their review fixes are commits 6b51858b, 169c1d69 and a616d489 on claude/eager-newton-ra06qu (build stamp 6b998f2625 at a616d489). Any later commit before the install changes the stamp. Install before Monday 06:30 UK.

**Reviews:** two adversarial review rounds (seven reviewers, each finding checked by a skeptic) and a final check. The first round found the archive bug. The second found the coach's news check could not be relied on (it was taken out) and a one-refresh gap in the fix (closed). Full suite: only the known flaky tests (the day-one stop floor and the Formula 1 daily-stop tests), which fail the same way on a81d767b.
