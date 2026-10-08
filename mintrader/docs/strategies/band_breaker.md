# Band Breaker (the fourth bot) - intraday index momentum, PAPER

**A band of normal noise around the open; a half-hour close outside it is
the trade; the stop at the far band or the day's VWAP, trailed only in the
trade's favour; flat five minutes before the cash close.**

Started 7 October 2026 in PAPER on US500, US30 and USTEC, New York session.
It never places an order and its results are never added to the account.

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
| Costs | fills at the touch plus one point; a buy marked on the bid, a sell on the ask; no commission on indices |
| Records | `data/bandbreaker.sqlite`, `data/bandbreaker-status.json`; its own tab on the page; `reports/<date>/bandbreaker.json` nightly |
| Settings | `"bandbreaker": {...}` in `config.json` (all optional; `enabled`, `markets`, `lookback_days`, `risk_money`, ...) |

Differences from the paper, stated: the paper sizes to a 2% daily
volatility target with up to 4x leverage; here every trade risks a flat 10
at its stop, like the other bots. The paper trades SPY; here it is the
broker's index CFDs, marked on bid and ask.

## What would earn it real money

Two weeks of PAPER with a positive result after spread, a win rate and
average win/loss in line with the paper's shape (fewer wins than losses,
wins about twice the size), and no day lost to a bug. Then it is Aaron's
call.

## Daily record

| Day | Trades | Wins | Net (paper) | By market | Notes |
|---|---|---|---|---|---|
| 7 Oct | 0 | - | 0 | - | first day: no trade, and no record of what it saw at each half hour. From 8 Oct every check is logged (close, band, VWAP, decision) so a quiet day can be told from a bug |
| 8 Oct | - | - | - | - | review fixes before the session: the 15:30 check now fires (it compared seconds and skipped the exact half hour); the bar still forming at a check can no longer count as closed; a tick with a crossed spread or over two minutes old is ignored; the day's entry count survives a restart |
