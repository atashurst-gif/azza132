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

- 2026-09-22: MOMENTUM_CONTINUATION switched off in TREND regime only
  (five days, 77 trades, ~-118 GBP, never a positive day; 22 Sep 1 win in
  13). Unchanged in HIGH_VOL and NEWS where it is break-even to positive.

- 2026-09-23: first full day with MOMENTUM_CONTINUATION out of trends:
  +16.66 GBP over 40 trades, BREAKOUT_RETEST in TREND +42.76. TREND_PULLBACK
  switched off (three days traded, all negative: 6/15, about -31 GBP).

- 2026-09-24: NO RULE CHANGE. The bot was down from about 10:32 UK to the
  evening (MetaTrader stopped answering its pipe; the trader exited when it
  could not reach the bridge; the watchdog ran out of restarts). Fixed in
  code, not in the rules: see docs/ops/always-watching.md. Results for this
  day cover only the morning.

- 2026-09-24 (evening, standing rule): BREAKOUT_RETEST switched off in
  HIGH_VOL only. Negative on three of the five days it traded there
  (17 Sep -4.46, 22 Sep -11.32, 23 Sep -3.82; 21 Sep +1.45, 24 Sep +5.30),
  6 trades, net -12.85 GBP. Unchanged in TREND and SQUEEZE.
  Note for the record: the same "three negative days" test also catches
  BREAKOUT_RETEST in TREND (negative on 4 of 6 days, but net +4.45 with
  +42.76 on 23 Sep) and in SQUEEZE (negative on 4 of 6, net +3.34).
  Neither was switched off: both are net positive and the TREND line is
  the strategy itself. That is a decision for the owner, not the rule.

- 2026-09-25: hard profit floor. Once a trade has been +1R ahead its stop
  never sits below +0.5R (lock_at_r 1.0, lock_floor_r 0.5). Evidence, 174
  trades 18-24 Sep: trailed-out winners peaked +1.45R and kept +0.50R; 18
  losers had been +1R ahead (-37 GBP). Same trades with the floor: +12 ->
  about +95 GBP (optimistic: peaks can be one tick). Early exits were
  tested and rejected: cutting at -0.5R would have turned +12 into -25,
  because 31 of 74 winners first went 0.5R against. Nothing else changed.

- 2026-09-25 (evening, standing rule): BREAKOUT_RETEST switched off in
  SQUEEZE. Negative on five of the seven days it traded (17, 18, 21, 24,
  25 Sep), 14 trades, net about +1 GBP. Retest in TREND is now net
  -21 GBP over 18-25 Sep (82 trades, 28 wins) and negative on four of six
  days; NOT switched off tonight because 11 of its 17 losers on the 25th
  had been +0.5R ahead and 7 had been +1R ahead - exactly what the profit
  floor installed on the 25th at 22:26 UK addresses. Judged again after
  Monday. MOMENTUM_CONTINUATION in HIGH_VOL is negative on three of five
  days but net +4.56 GBP with two of the losses under 1 GBP: left on.

## Reverting
Install the pinned build by downloading
https://github.com/atashurst-gif/azza132/archive/refs/heads/retest-2026-09-18.zip
and running the Start icon from it. The measuring clock is unaffected.
