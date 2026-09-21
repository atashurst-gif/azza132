# Retest (strategy 1)

The rule set that produced the first clearly profitable day.

| Pinned build | Branch | Day | Result |
|---|---|---|---|
| 22b81b14f9 | `retest-2026-09-18` | 2026-09-18 | +44.42 GBP after fees, 46 trades, 48% winners, win/loss size 1.63, 15 of 22 winners reached target |

Rules in force that day (all defaults in `mintel/config.py` at the tag):
NORMAL setups (score 60+) or better; expected move at least 1.8x the risk
after costs; costs at most 20% of the stop; stop at least 1 ATR from entry;
no trailing before +1R, then 40% giveback; a third banked at 2.5R;
counter-trend reversals only outside trends; NEWS_CONTINUATION off;
MOMENTUM_CONTINUATION needs a STRONG score; no limit on trade count.

## Changes since, each to be judged against this day
- 2026-09-19: cost gate 20% -> 12% of the stop; profit floor past +1R.
  REVERTED 2026-09-21: the first day on it ran at a 27% win rate. The main
  line is back on the 2026-09-18 rules.

## Reverting
Install the pinned build by downloading
https://github.com/atashurst-gif/azza132/archive/refs/heads/retest-2026-09-18.zip
and running the Start icon from it. The measuring clock is unaffected.
