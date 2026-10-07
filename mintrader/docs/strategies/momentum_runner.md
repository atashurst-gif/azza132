# Momentum Runner (the third bot) - PAPER shadow on the indices

**Same entry as Trend & Breakout on an index; the stop left alone until the
trade is 3 R ahead, then trailed 3 R behind the best price.**

Started 7 October 2026, in PAPER. It never places an order and its results
are never added to the account.

## Why it exists

On 7 October every closed trade since 17 September (435) was replayed
against the broker's own one-minute bars for the eight hours after each
entry (`python -m mintel.ops.potential`), and ten plain riding rules were
scored against what the ladder actually kept:

| Indices only (97 trades) | Per trade | Total |
|---|---|---|
| The ladder, as traded | +0.11 R | +10.4 R |
| Trail 1 R behind the best price | +0.14 R | +13.2 R |
| Trail 2 R | +0.21 R | +20.0 R |
| **Trail 3 R** | **+0.30 R** | **+29.1 R** |
| A wider (15) stop, any trail | +0.12 to +0.15 R | +11 to +14 R |
| Hold for a fixed time | about 0 | about 0 |

On FX and metals no rule beat the ladder; on the 98 biggest winners overall
nothing beat the ladder either. The extra money is in index trades that run
for hours after a dip deeper than 1 R, which the ladder cannot hold through.

Two caveats, stated at the time: three weeks of data, and the best of ten
rules picked in hindsight flatters the winner. Hence a paper trial first.

## How it works

| | |
|---|---|
| Entry | the real trade's fill, taken the moment Trend & Breakout opens an index position (`US30`, `US500`, any `INDEX` market not excluded) |
| Size | the real trade's size, so the money is like for like |
| Stop | the real trade's ORIGINAL stop (from the FlowLock tracker, not the trailed one at the broker) |
| Riding | nothing moves until the best price is +3 R; then the stop trails 3 R behind it and never retreats |
| Out | at the stop (counted AT the stop, never better; a buy on the bid, a sell on the ask), or at the price after 8 hours |
| The real trade | unaffected; the shadow outlives it when the ladder closes first |
| Costs | commission is zero on indices at this broker; spread is paid through the bid/ask marking |
| Records | `data/runner.sqlite`, `data/runner-status.json`; its own tab on the page; `reports/<date>/runner.json` nightly |
| Settings | `"runner": {"enabled": true, "trail_r": 3.0, "window_hours": 8.0, "markets": []}` in `config.json` (all optional) |
| Code | `mintel/runner/`, hooked in from `Trader.manage` and never able to raise into it |

## What would earn it real money

Two weeks of paper in which its net beats Trend & Breakout's net on the same
index trades, with the losses no worse than the replay suggested (more
trades ending at the full -1 R, since there is no break-even move). Then
the decision is Aaron's; running it live would mean a second position on
the same signal, so the sizing of both would have to be looked at together.

## Daily record

| Day | Shadows | Wins | Net (paper) | The ladder on the same trades | Notes |
|---|---|---|---|---|---|
| 7 Oct (to 13:33 UK) | 4 | 0 closed, 1 open | -27.77 closed; US30 open +60.89 at +8.2 R (trail locks about +47) | -2.36 on the same four (JP225 +5.12, US30 -8.39, JP225 -9.52, US30 +10.43) | three shadows ran to the full stop where the ladder lost slightly less or won small; the fourth reached +9.4 R, nine times what the ladder kept |
