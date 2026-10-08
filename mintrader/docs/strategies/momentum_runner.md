# Momentum Runner (the third bot) - riding the indices

**Same entry as Trend & Breakout on an index; the stop left alone until the
trade is 3 R ahead, then trailed 3 R behind the best price.**

Started 7 October 2026, in PAPER, where it never places an order: it shadows
the real trade on paper. Switched to LIVE on the demo on 8 October 2026
(see "Going LIVE" below): it now opens its own position beside the real
trade, under its own magic number. Its results stay on its own tab and are
never added to the account total.

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
| Costs | PAPER: commission is zero on indices at this broker; spread is paid through the bid/ask marking. LIVE: whatever the broker charged, inside its own figure |
| Records | `data/runner.sqlite` (table `trades`, one row per trade: `ticket` is the Runner's own broker ticket in LIVE, the real trade's in PAPER; `source_ticket` is always the real trade it rode, `0` for an orphan the Runner closed; `mode` says which; table `skipped`, one row per real trade the LIVE Runner said no to for good, with why), `data/runner-status.json`; its own tab on the page; `reports/<date>/runner.json` nightly |
| Settings | `"runner": {"enabled": true, "mode": "LIVE", "magic": 990511, "trail_r": 3.0, "window_hours": 8.0, "markets": []}` in `config.json` (all optional; `mode` is `PAPER`, `LIVE` or `OFF`). The LIVE freshness limits (2 minutes, half an R) are the engine's defaults (`max_entry_age_seconds`, `max_entry_drift_r` in `mintel/runner/shadow.py`) |
| Code | `mintel/runner/`, hooked in from `Trader.manage` and never able to raise into it; orders go through the shared executor `mintel/botexec.py` |

## Going LIVE (8 October 2026, demo account)

Aaron asked for it to trade for real on the demo after one day of paper,
ahead of the two weeks the plan above asked for. What changes, and what
does not:

- **Its own order, beside the real trade.** The moment Trend & Breakout
  opens an index trade, the Runner sends its own market order: same market,
  side and size, the real trade's ORIGINAL stop, no target, at the price of
  the moment. So there are two positions on the same signal; the Runner's
  is a second, separate position, and the real trade is never touched. The
  sizing of the two together is the risk Aaron accepted by switching it on.
- **Only a fresh trade is entered.** The Runner's order goes at the price of
  the moment with the real trade's original stop, so it is only the same
  trade while the real one is fresh: within 2 minutes of its open and with
  the price within half an R of its entry (and the stop still on the right
  side of the price). Later than that (the account gate reopening hours
  on, a restart, a start with a fresh data folder) the Runner would be
  taking a different trade at the same size, a multiple of the money at
  risk or a hair-trigger stop, so it says no for good: "too late to
  enter: ...", written to the `skipped` table so a restart never retries
  it. The day's skips are counted on the status.
- **Magic 990511, comment `RUNNER`.** Every order carries the Runner's own
  magic number, and the Runner only ever reads positions with that number,
  so it can never see or touch Trend & Breakout's (990311) or the other
  bots' positions, and they can never touch its. A `magic` in `config.json`
  that is another bot's number (990311, 990411, 990611, 990711) or not a
  positive number is refused before anything is sent: the Runner does not
  start ("Momentum Runner not started: ... refusing to go LIVE" in the log).
- **The stop is at the broker, or there is no trade.** An order whose stop
  the broker did not take is closed again at once and counted as refused.
  The broker's stop does the stopping; the Runner only moves it (after +3 R,
  3 R behind the best price, never backwards) and closes at the window's
  end. A stop move the broker refuses is rolled back and tried again next
  pass; a close it refuses is kept and retried.
- **The broker's figure is the record.** Every LIVE row's `net_pnl` is the
  broker's own profit + commission + swap for that position, read from its
  history once the position is gone. Whenever that record is not there, the
  row is written from the price instead and its exit reason carries a `?`
  (`TRAIL_STOP?`, `INITIAL_STOP?`, `WINDOW_END?`, `ORPHAN_CLOSED?`) so the
  review can see it was estimated; a row without the `?` is the broker's
  figure, always.
- **A missing position is not a closed one.** The terminal sometimes
  answers a position read with nothing at all (an empty list, not an
  error). One such read judges nothing: a position that is not in the
  list, and has no closing deal in the history, is kept and counted; only
  after three reads in a row without it is the row estimated with a `?`.
  If the position then turns up again, its row is reopened and managed on,
  never closed as an orphan.
- **The shared account gate.** No new entry while Trend & Breakout's daily
  loss stop, a drawdown or exposure breaker, or safe mode holds; the status
  says "entries paused by the account gate". Managing an open position is
  never gated.
- **A refused order is a clean no.** No trade row is written, the note says
  "order refused: ..." and that real trade goes into the `skipped` table so
  the broker is not hammered, not even after a restart. The day's refusals
  are counted on the status.
- **Restarts fail closed.** On start the Runner checks the broker: a row the
  broker closed while the bot was down is finalised from the broker's
  record; a position carrying the Runner's magic that has never been on
  record is closed at once ("orphan closed"), never adopted, and the close
  is written as a row of its own (`exit_reason` `ORPHAN_CLOSED`,
  `source_ticket` 0, the broker's figure), so the money is still in the
  book and in Overall. If the broker cannot be read at start, nothing is
  judged gone: the first pass that can read it does the same check.
- **PAPER is unchanged.** In PAPER nothing is sent; the shadow keeps the real
  trade's own fill and ticket, closes AT its stop (never better), a buy on
  the bid and a sell on the ask. `source_ticket` is filled in both modes so
  the "what the ladder kept on the same trades" comparison works the same.
- **Switching back:** `python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper runner`
  (or set `"mode": "PAPER"` under `"runner"` in `config.json` and restart).
  A LIVE position open at the moment of the switch keeps its stop at the
  broker and is left alone by the paper (or OFF) Runner, which sends
  nothing for it: no stop move, no close. It says so in the status note and
  under `unmanaged` on the page, and only watches: when the broker's stop
  (or a hand) closes the position, the row is finalised from the broker's
  record like any other LIVE row, so the result still reaches the records
  and Overall.

## What would earn it real money

Two weeks in which its net beats Trend & Breakout's net on the same index
trades, with the losses no worse than the replay suggested (more trades
ending at the full -1 R, since there is no break-even move). Running it
live means a second position on the same signal, so the sizing of both has
to be looked at together.

## Daily record

| Day | Trades | Wins | Net (paper to 7 Oct; the broker's figure from 8 Oct) | The ladder on the same trades | Notes |
|---|---|---|---|---|---|
| 7 Oct | 5 | 1 | +20.14 | +1.50 on the same five (JP225 +5.12, US30 +10.43, US30 -8.39, JP225 -9.52, JP225 +3.86) | four shadows to the full stop (-10.06, -8.36, -9.35, -8.76; two of them where the ladder had kept +5.12 and +3.86), one ridden to +7.67 R (+56.67, peak +10.67 R) where the ladder kept +10.43. The shape the replay predicted |
| 8 Oct | 3 | 1 | +9.05 (paper +17.83 on 2; LIVE -8.78 on 1, the broker's figure) | -1.59 on the same three (US30 +4.47, US30 +3.25, US500 -9.31) | US30 ridden to +2.57 R (+24.96, peak +5.57 R) where the ladder kept +4.47; US30 to the full stop (-7.13) after a +1.86 R peak where the ladder kept +3.25; the first LIVE ride, US500 at 18:27 UK, stopped in four minutes (-8.78): with Trend & Breakout's -9.31 one idea cost -18.09. Over 8 rides since 7 Oct: Runner +4.23 R, the ladder +0.25 R on the same trades; without the 7 Oct US30 ride -3.44 R against -1.17 R |
