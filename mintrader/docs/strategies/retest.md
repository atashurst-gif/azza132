# Retest (strategy 1)

The rule set that produced the first clearly profitable day.

| Pinned build | Tag | Day | Result |
|---|---|---|---|
| 22b81b14f9 | `retest-2026-09-18` | 2026-09-18 | +44.42 GBP after fees, 46 trades, 48% winners, win/loss size 1.63, 15 of 22 winners reached target |

Rules in force that day (all defaults in `mintel/config.py` at the tag):
NORMAL setups (score 60+) or better; expected move at least 1.8x the risk
after costs; costs at most 20% of the stop; stop at least 1 ATR from entry;
no trailing before +1R, then 40% giveback; a third banked at 2.5R;
counter-trend reversals only outside trends; NEWS_CONTINUATION off;
MOMENTUM_CONTINUATION needs a STRONG score; no limit on trade count.

## Changes since, each to be judged against this day
- 2026-09-19: cost gate 20% -> 12% of the stop; profit floor: past +1R the
  stop never sits more than 65% of the best gain back.

## Reverting
Install the tagged build:
`git checkout retest-2026-09-18` (or download the tag's zip) and run the
Start icon. The measuring clock is unaffected.
