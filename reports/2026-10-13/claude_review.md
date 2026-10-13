# Nightly review: Tuesday 13 October 2026 (full working)

All figures come from the 22:10 UK upload and the 22:35 UK pulse.

## State
- **Mac build:** 0d07d26946, up 97% of the day (122 of 126 pulses). There were four single-pulse gaps where the page did not answer, at 00:14, 06:29, 08:22 and 11:32 UK, each during a process restart. No sleep.
- **Bot modes:** every bot is on PAPER. Nothing is on DEMO that should not be. Open real positions: 0.
- **The Mac does NOT yet have:**
  - the weekly pick (c037ef93);
  - every idea practising (c33a978a);
  - the TRADING SQUAD page (72d7376a).

  There is no reports/live/weekly.json and no practise_all in lab.json. Aaron needs to run the install before Friday's 22:30 pick.

## DEMO (the broker's figures)
- **Today:** −£12.43 on 2 trades. Both were Financial Ian's, just after midnight UK, before the 00:15 install moved everything to paper.
- **Equity:** £1,840.54.
- **Since the 5 Oct reset:** −£159.46.

## PAPER today (the bots' own records, after costs)

| Test | Trades | Wins | Net |
|---|---|---|---|
| Crowd Fader | 3 | 2 | +23.54 |
| Momentum Runner | 1 | 0 | −8.84 |
| Financial Ian | 10 | 2 | −17.56 |
| Trend & Breakout | 5 | 0 | −62.80 |
| Band Breaker | 0 | – | 0 |
| **All** | **19** | **4 (21%)** | **−65.66** |

- **Trend & Breakout:** the page at 22:35 showed 7 trades, −74.49 (two more closed after the upload; their detail is in tomorrow's file).
  - **By approach:** momentum continuation in a fast market lost 3 trades (−37.08); session expansion in a trend lost 2 (−25.72).
  - **By market:** US30, US500, JP225 and XAUCHF. All five were stopped.
- **Streaks:** Crowd Fader has 2 profitable days (12 Oct +30.87, 13 Oct +23.54). Nothing else is on a streak.

## Lab
- **Tested:** 398 ideas, 0 passed; 234 still to test tonight at 23:10; 266 waiting for more history.
- **Practice:** on this build the lab still gates practice, so no idea practises live yet. The new build puts all ~640 in.

## Standing rules
- **Momentum continuation in a fast market (Trend & Breakout):** negative on 12 and 13 Oct, 2 days, so the 3-day switch-off is not met yet. Watch it tomorrow.
- **Markets:** no market rule met.

## Changes tonight
- No strategy change; the evidence is too thin (one to two days).
- Daily-record rows added (commit aa196d3e).

## The bar for Friday
- No test is near it: the most trades in 28 days is Ian with about 22 practice trades, and he is losing.
- Crowd Fader has 9 practice trades over 3 days (+34.97).
- First picks are unlikely before next week, once all ~640 ideas practise.
