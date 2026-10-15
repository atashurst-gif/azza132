# The lab - the night of Thursday 15 Oct 2026

Prices: 24 markets, 71 complete trading days (2026-06-17 to 2026-10-15).
Past (used for choosing): 2026-07-08 to 2026-09-11. Fresh (kept back, looked at once): 2026-09-14 to 2026-10-14; 0 of 30 fresh looks used in stretch F-2026-10.
Short history (under 90 days): results are weaker; the thresholds are never lowered.

The funnel: 632 ideas, 398 tried on past prices, 0 passed; 0 fresh looks, 0 passed; 0 on the bench, 0 practising, 0 ready for live, 398 binned; 266 waiting for the calendar.
Last night: 0 tested in 4 min (0.0 s each); 0 fresh looks, 0 passed.
The honest count: 398 tests in all (398 on past prices, 0 fresh). The best of that many always looks good, so every result is judged against that.

## Families

- Opening range (OPENING_RANGE): 144 ideas, 72 tested this stretch, 0 passed, PBO 0.60; fits noise: its passes are held back
- Breakout (RANGE_BREAK): 108 ideas, 54 tested this stretch, 0 passed, PBO 0.33
- Momentum burst (MOMENTUM_CONTINUATION): 108 ideas, 54 tested this stretch, 0 passed, PBO 0.07
- Trend pullback (TREND_PULLBACK): 48 ideas, 48 tested this stretch, 0 passed, PBO 0.80; fits noise: its passes are held back
- Back to the average (VWAP_FADE): 108 ideas, 54 tested this stretch, 0 passed, PBO 0.53; fits noise: its passes are held back
- Yesterday's high and low (PREV_DAY_LEVELS): 48 ideas, 48 tested this stretch, 0 passed, PBO 0.80; fits noise: its passes are held back; no setting to nudge: the fresh test decides
- Squeeze release (SQUEEZE_RELEASE): 36 ideas, 36 tested this stretch, 0 passed, PBO 0.13
- After the news (NEWS_MOVE): 32 ideas, 0 tested this stretch, 0 passed, PBO n/a; asleep until the calendar has enough history
- Clock trade (control) (CONTROL_TIME_OF_DAY): 32 ideas, 32 tested this stretch, 0 passed, PBO 0.53; fits noise: its passes are held back; no setting to nudge: the fresh test decides

The clock-trade control passed the past test 0 times in 32: it is expected to fail, so this shows whether the gates let noise through.

Sources: data/lab/lab.sqlite (tests, events, families, practice_trades, looks, roster, runs); data/lab/summary.json; data/lab/cut.json.
