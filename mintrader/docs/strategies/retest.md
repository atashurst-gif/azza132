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

- 2026-09-28 (evening): TWO FINDINGS. (1) The switch-offs of 24 and 25 Sep
  never took effect: "BREAKOUT_RETEST" is the name a signal reports under,
  the tactic is BREAKOUT_ACCEPTANCE, and the switch-off only checked the
  tactic's name. Retest kept trading in HIGH_VOL and SQUEEZE (one SQUEEZE
  trade on 28 Sep, +4.22). Fixed: the check now covers the reported name.
  (2) Standing rule applied to BREAKOUT_RETEST in TREND: negative on five
  of seven days (21, 22, 24, 25, 28 Sep), 86 trades, 28 wins, net -36.41
  GBP; on the first day with the profit floor 0 wins in 4, -15.37. OFF.
  The Retest strategy as a whole is therefore off from 29 Sep; what runs
  is Session expansion in TREND (+19.47 over seven days), Sweep in
  HIGH_VOL (+20.91), Momentum in NEWS (+51 over its two days) and in
  HIGH_VOL (+4.56), Breakout acceptance (un-retested) in TREND, and the
  squeeze tactics.
  Profit floor, first day: 4 trades reached +1R and none of them lost
  (Friday: 10 of 10 did); trailed winners kept 0.77R of a 1.52R peak
  (was 0.50R of 1.45R).

- 2026-09-29 (morning): the RUNNER. A trade in STRONG_FLOW (momentum still
  with it past +1R) has its broker target pushed out to twice the original
  distance; when price reaches the original target the bot banks 70% and
  lets 30% run under the trail with at least +1R locked. If the flow decays
  before the target, the original target is put back. Every arm, disarm and
  run is journaled (RUNNER_ARMED / RUNNER_DISARMED / RUNNER_RUN) so the
  review can say what the runners earned. Evidence for it: none yet - the
  broker's target closed everything, so the record never showed what price
  did afterwards. This measures it. Time-of-day: NO change. For the
  approaches still on, mornings 07-12 UK were +42 GBP over seven days and
  evenings -12; the morning losses everyone remembers came from Retest and
  Momentum in trends, both now off.

- 2026-09-29 (evening): FAILED_BREAKOUT_RECLAIM switched off (two days,
  two trades, no wins, both dead on arrival, about -12 GBP; the owner's
  call, ahead of the three-day rule). THE TWIN added: every second
  SESSION_EXPANSION signal is taken as SESSION_EXPANSION_2X - same entry,
  stop and size, target twice as far, and a ladder of locked profit
  (+1R keeps +0.5R, +2R keeps +1.2R, +3R keeps +2R, +4R keeps +3R)
  instead of the runner. Same signals, alternate exits, judged on equal
  opportunity; the day review lists the two side by side. Session
  expansion in TREND is the best approach on record (+19 GBP over seven
  days, 4 of 5 on the 29th), which is why it carries the experiment.

- 2026-09-30 (evening): standing rule - BREAKOUT_ACCEPTANCE in TREND off
  (negative 3 of 6 days, net about -7 GBP) and LIQUIDITY_SWEEP_REVERSAL in
  HIGH_VOL off (negative 5 of 9 days, the last five in a row, 23-30 Sep).
  The ladder now applies to every trade (ladder_for_all): +2R keeps +1.2R,
  +3R keeps +2R, +4R keeps +3R. Evidence: three winners on the 30th peaked
  at 1.9-2.2R and closed on the +0.5R floor; over 18-28 Sep five trades
  reached +2R and closed below +1.2R, worth about +14 GBP with this step.
  The twin's first day: 4 of 6, +8.18 GBP, against 2 of 7, -11.86 GBP on
  the normal exit for the same signals.

- 2026-10-02 (midday): standing rule - SESSION_EXPANSION in SQUEEZE off.
  Negative on three of the four days it traded: 25 Sep -3.77 (0/1),
  29 Sep -4.66 (0/1), 1 Oct +4.60 (2/3), 2 Oct -11.71 (0/3 by noon).
  Eight trades, two wins, net about -16 GBP. Session expansion in TREND,
  the best approach on record, is untouched.

- 2026-10-04 (weekend review of 28 Sep - 2 Oct, 115 trades):
  * Entry floor raised to STRONG (score 70). Under-70 entries: 41 trades,
    19 wins, about -21 GBP (negative on 28 and 30 Sep, positive only on
    the 1st). 70 and over: 63 trades, 37 wins, about +73 GBP. Momentum
    continuation already had a 70 floor; this extends it to everything.
  * The twin is off. Same signals, alternate exits, three days: twin
    -6.47 on 18 trades against +10.10 on 21 for the normal exit, and it
    lost the head-to-head on two of the three days. The ladder for all
    trades keeps the "hold the winner" idea in a form that worked (no
    trade all week that reached +1R ended in a loss).
  * Noted, not acted on: 02:00-05:00 UTC was 0 of 7 for -28 GBP, but all
    on 30 Sep; 16:00 UTC was 0 of 4 on the 28th and 1 of 1 on the 1st.
    CHFJPY 0 of 4, XAUJPY 1 of 5, UK100 1 of 4 this week; CHF crosses and
    AUDJPY carried the week. One week is not enough for market rules.
  * Momentum continuation in HIGH_VOL is the engine: 47 trades, 30 wins,
    +80 GBP. Session expansion in TREND: 21 trades, 11 wins, +5.83.

- 2026-10-04 (evening): the status page's live ledger counted only the
  closing deal of each position, so every Trend & Breakout trade showed
  half its commission. Found by reconciling 2 Oct against MetaTrader per
  bot: page -29.66, broker -31.14, each of eleven trades short by exactly
  half its commission; the scalper's figures were right. Both the page
  and the daily-loss measure now count the entry deal too. The nightly
  review was already correct. `python -m mintel.ops.reconcile` prints the
  broker's per-bot record for any day and must match the History tab.

- 2026-10-04 (night): markets, from every trade since 17 Sep (396, the
  broker's net after commission).
  * By market type: FX pairs 201 trades, +79.52 before fees, commission
    122.40 (0.61 a trade), net -42.88. Gold and gold crosses 107 trades,
    commission 0.06 a trade, net +28.13. Indices 88 trades, no
    commission, net +14.74. FX's edge on price is eaten by its fees.
  * New rule for markets, same shape as the standing rule for approaches:
    negative on at least two-thirds of the days traded, over six or more
    days, and it is switched off (`universe.excluded_symbols`). Out:
    USDJPY, DE40, XAUGBP, CHFJPY, EURNZD, UK100, GBPJPY, XAUAUD - 125
    trades, -219.70 together. This is a judgement made in hindsight on
    under three weeks; the nightly review will watch the markets that
    remain, and any of these can be switched back on.
  * The best performers stay and are untouched: AUDJPY +49.75 (6 of 7
    days positive), XAUCHF +47.80 (6/9), US500 +39.24 (4/7), EURCHF
    +33.79 (2/2), US30 +29.21 (7/10), XAUUSD +17.46 (4/6).

- 2026-10-05: fresh GBP 2,000 account. The measuring clock restarts
  (new STRATEGY_VERSION). Commission is never assumed to be zero any
  more: until a market's commission has been seen in the bot's own deals
  it uses 6.00 a lot round turn (measured 1-5 Oct at about 5.50 on FX,
  gold and silver), and indices cost nothing. On a new account the old
  rule meant the first trades in every market were judged as free.
  Looked at and NOT changed: a tighter cost cap. Grouping 281 trades by
  commission as a share of their risk gave no clean line (10-15%: -47.06
  on 104 trades; 15-20%: +17.86 on 40), so the 20% cap stays.
  Risk is a percentage of the account, so on 2,000 each trade risks
  about 10 at the 0.5% default and the daily loss stop is about 60 at 3%.

- 2026-10-06 (night, standing rule): BREAKOUT_ACCEPTANCE switched off in
  HIGH_VOL. Negative on all three days it traded (17 Sep -3.59, 30 Sep
  -6.08, 1 Oct -7.72; 3 trades, 0 wins). Rule set renamed "Commission First
  (version 3)"; the measuring clock is unchanged (still from the 5 Oct
  reset). Looked at and NOT changed (two days only): indices +79.21 on 6
  trades since the reset against -52.66 on 10 trades everywhere else;
  scores of 80+ 0 wins in 4 (-41.27) against 70-80 6 in 12 (+67.82).
  XAUJPY is negative on 6 of 9 days but net +3.47: left on, as net-positive
  lines have been before. Risk per trade is NOT a flat 0.5%: STRONG setups
  have been sized at 0.65-0.9% (13-18 GBP on 2,000).

## Reverting
Install the pinned build by downloading
https://github.com/atashurst-gif/azza132/archive/refs/heads/retest-2026-09-18.zip
and running the Start icon from it. The measuring clock is unaffected.
