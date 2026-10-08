# Band Breaker (the fourth bot) - intraday index momentum

**A band of normal noise around the open; a half-hour close outside it is
the trade; the stop at the far band or the day's VWAP, trailed only in the
trade's favour; flat five minutes before the cash close.**

Started 7 October 2026 in PAPER on US500, US30 and USTEC, New York session.
LIVE on the demo account from 8 October 2026: it places its own orders
(magic 990611, comment `BB`), every one with the stop at the broker, and
the broker's own figure for each closed trade is its result. In PAPER it
never places an order and its results are never added to the account.

## Where the idea comes from

Zarattini, Aziz and Barbon (2024), "Beat the Market: An Effective Intraday
Momentum Strategy for S&P500 ETF (SPY)", Swiss Finance Institute research
paper 24-97, SSRN 4824172. On SPY from 2007 to early 2024 it reports
+19.6% a year and a Sharpe ratio of 1.33 net of costs (they charge
commission and slippage). It has been replicated independently several
times (GitHub: codecat-ops, francesco-nicolo, nyyyaomi; QuantConnect forum),
and one replication notes the edge has been thin since 2025. That is the
honest state of the evidence: a strong published record, a weaker recent
one, which is exactly why it starts in PAPER here.

Why this and not the better-known 5-minute opening range breakout: the
ORB paper's result reproduces gross on five indices but a careful
replication on CFD-style costs finds it nets to about zero, and it hits
only one trade in four. The noise-band rule trades less, holds longer and
survives its own cost assumptions.

Why it suits this account: the indices cost no commission at this broker,
the bot is flat overnight (no swap), and the rules are fully mechanical.

## The rules, as built (`mintel/bandbreaker/engine.py`)

| | |
|---|---|
| Markets | US500, US30, USTEC (any the broker lacks is skipped) |
| Session | 09:30-16:00 New York; nothing outside it; flat by 15:55 |
| Noise | for each half hour t of the session, the average over the last 14 sessions of the absolute move from that day's open to the close at t |
| Band at t | upper = max(open, previous close) x (1 + noise_t); lower = min(open, previous close) x (1 - noise_t) |
| Checks | at every half-hour close from 10:00 to 15:30 |
| Entry | half-hour close above the upper band: long; below the lower band: short; inside: nothing. A close through the other band reverses |
| Stop | long: the higher of the lower band and the day's VWAP; short: the lower of the upper band and VWAP. It must clear the price we would be closed at, else the band alone. Re-set every minute, only ever in the trade's favour |
| Exit | the stop (counted AT the stop, never better), the reversal, or 15:55 |
| Size | 10 at the initial stop (`risk_money`), in whole broker steps |
| Limits | at most 3 entries a day per market |
| Costs | PAPER: fills at the touch plus one point; a buy marked on the bid, a sell on the ask; no commission on indices. LIVE: whatever the broker fills and charges |
| Records | `data/bandbreaker.sqlite` (every trade row carries its `mode`), `data/bandbreaker-status.json`; its own tab on the page; `reports/<date>/bandbreaker.json` nightly |
| Settings | `"bandbreaker": {...}` in `config.json` (all optional; `enabled`, `mode`, `magic`, `markets`, `lookback_days`, `risk_money`, ...) |

Differences from the paper, stated: the paper sizes to a 2% daily
volatility target with up to 4x leverage; here every trade risks a flat 10
at its stop, like the other bots. The paper trades SPY; here it is the
broker's index CFDs, marked on bid and ask.

## Going LIVE

Aaron's call, 8 October 2026: LIVE on the demo account. The rules above do
not change; only where the orders go.

- **Its own orders, its own number.** Every order carries magic `990611`
  with the comment `BB`. The engine only ever reads positions with that
  magic, so it cannot see or touch Trend & Breakout's trades (990311), the
  Rapid Scalper's (990411) or any other bot's, and they cannot touch its.
- **The stop is at the broker, or there is no trade.** An order is sent
  with `sl` = the computed stop and no target. If the broker does not show
  the stop on the new position the executor closes it again at once and the
  check is logged as `order refused: ...`. A refused order is not retried:
  that half-hour check is spent, and the day's count of refusals is on the
  status page. The per-minute stop re-set (only ever in the trade's favour)
  is sent to the broker; if the broker refuses it the engine keeps the old
  stop (the one the broker holds) and tries again next minute.
- **The broker's figure is the figure.** When a position is gone from the
  broker's list and the broker has a closing deal for it, the row is closed
  once from that deal: exit price and net = profit + commission + swap. The
  reason is the broker's own word: `STOP` when the deal says the stop took
  it, `MANUAL` when it was closed by hand in the terminal, `BROKER_CLOSED`
  for any other word; with no word at all it is `STOP`, the only level the
  trade had. The engine's own exits (a reversal through the other band,
  flat at 15:55, outside the session) are market closes at the broker and
  take the deal's figure too. If the broker's history has not caught up
  after the executor's retries the row is booked at the fill price with a
  `?` on the reason, so the review can see it was estimated. Running P&L
  on the page is the broker's too.
- **One empty read is not a close.** The adapter answers an error from
  MetaTrader with an empty list, so a ticket missing from one read, with
  no closing deal, stays open and managed (note: `not in the broker's list
  and no closing deal yet; kept and checked again`). Only after three reads
  in a row over at least a minute, still with no deal, is it booked at the
  last price as `STOP?`. If it then turns up at the broker after all, the
  sweep (next point) closes it and the broker's figure replaces the
  estimate on the same row, reason `ORPHAN_CLOSED`.
- **The stop at the broker is the stop.** Every pass the engine reads the
  stop the broker holds; one that is better for the trade than its own
  (tightened by hand, or trailed just before a restart) is adopted and
  logged, so the per-minute trail can only ever move the broker's stop in
  the trade's favour, never back.
- **The sweep.** Once a minute (and at once after an order whose outcome
  is unknown: the fill went in but the bridge dropped the read after it,
  logged as `order sent, state unknown`, counted as a refusal) the engine
  reads every position with its magic. One with no open row here is closed
  and booked with the broker's figure, reason `ORPHAN_CLOSED` (fail closed;
  never adopted). So a fill the engine lost track of is never left to run
  unmanaged for longer than a minute.
- **Restart.** Open rows are checked against the broker: one still there is
  managed on (with the broker's stop if that is the better one); one the
  broker closed while the bot was down is closed from its deal; one missing
  with no deal is kept and checked on the first passes. The sweep then
  closes anything with this magic and no row (`orphan closed`); closing an
  orphan never touches the position managed on the same market. The
  half-hour checks already taken today come back from the checks table, so
  a restart three minutes after a close cannot run the same check, and send
  the same order, twice. If two open rows share one market (the trace of an
  earlier fault) the first is managed and the second is closed at the
  broker and booked as `DUPLICATE_CLOSED`, or listed as `unmanaged` if it
  cannot be. A row opened in the other mode (a LIVE row seen by a PAPER
  run, or the reverse) is left exactly as it is and listed under
  `unmanaged` on the status page: go flat before switching modes.
- **Ticket numbers.** PAPER rows number from 700000001 on, continuing the
  paper rows only; LIVE rows carry the broker's own ticket. If the broker
  ever hands out a number already on record, the position is closed again
  at once and the check logged `order refused: ticket N already on record`:
  no live position without a row of its own, and no paper row overwritten.
- **The shared account gate.** Before every new entry the engine asks the
  main bot whether entries are allowed. While the daily-loss stop, a
  drawdown or exposure breaker, or safe mode holds, it does not enter and
  the market note reads `entries paused by the account gate`. Managing an
  open position (the stop, the flat at 15:55) is never gated.
- **Switching back.** `python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper band_breaker`
  puts it back in PAPER; `mode: "OFF"` in `config.json` stops it sending
  anything (and managing anything: the stop at the broker still protects an
  open position, but nobody trails it or flattens it at 15:55, so go flat
  first).

## What would have earned it real money

Two weeks of PAPER with a positive result after spread, a win rate and
average win/loss in line with the paper's shape (fewer wins than losses,
wins about twice the size), and no day lost to a bug. Aaron chose to go
LIVE on the demo on day two instead; the record below is the evidence
either way.

## Daily record

| Day | Trades | Wins | Net (paper) | By market | Notes |
|---|---|---|---|---|---|
| 7 Oct | 0 | - | 0 | - | first day: no trade, and no record of what it saw at each half hour. From 8 Oct every check is logged (close, band, VWAP, decision) so a quiet day can be told from a bug |
| 8 Oct | 2 (LIVE) | 0 | -9.23 LIVE (the broker's figure) | US500 -8.01 (STOP), USTEC -1.22 (SESSION_CLOSE) | US500 short at 13:30 NY stopped at 15:24 NY; USTEC short at 14:30 NY closed at 16:58 NY, 63 minutes after the 15:55 flat rule, because the Mac was asleep (the 15:00 and 15:30 NY checks never ran). USTEC's signal was refused three times before that (the smallest size would have risked 17-26). Earlier the same day, review fixes before the session: the 15:30 check now fires (it compared seconds and skipped the exact half hour); the bar still forming at a check can no longer count as closed; a tick with a crossed spread or over two minutes old is ignored; the day's entry count survives a restart. LIVE on the demo from today (see "Going LIVE"). Second review, same day: one empty read of the broker's list no longer books a trade as closed; the stop the broker holds is adopted when it is the better one; a fill the engine lost track of is swept within a minute; the half-hour checks survive a restart; closing an orphan never drops the position managed on the same market; a hand close is labelled MANUAL, not STOP |
