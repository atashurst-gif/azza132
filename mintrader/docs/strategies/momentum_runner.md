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

## Version 5 (9 October): not on momentum continuation

**The Runner no longer rides momentum continuation** (`runner.skip_tactics`,
default `["MOMENTUM_CONTINUATION"]`; an empty list rides everything as before).

The replay above, split by the approach of the real trade (index trades,
3 R trail against what the ladder kept, broker minute bars, R):

| Index trades | Trades | Ladder | Trail 3R | Trail 2R | Trail 1R | Days down under the 3 R trail |
|---|---|---|---|---|---|---|
| Momentum continuation | 33 | +4.8 | **-14.4** | -10.6 | -2.6 | 7 of 9 |
| Everything else (retest, session expansion, squeeze release, sweep, ...) | 64 | +5.5 | **+43.5** | +30.5 | +15.7 | 3 of 13 |

Every riding rule lost on momentum continuation; the 3 R trail's whole
edge came from the other approaches. The Runner's own record says the same
(broker figure for the LIVE row, paper for the rest):

| Runner trades 7-8 Oct | Trades | Wins | Net | R |
|---|---|---|---|---|
| Momentum continuation | 7 | 1 | -27.48 | -3.44 |
| Session expansion | 1 | 1 | +56.67 | +7.67 |

So it was negative on 7 of 9 days in the replay - well past the standing
rule's three - and the live record agrees. Its trail stays at 3 R: on the
approaches it still rides, 3 R beat 2 R and 1 R (table above). Today's
Trend & Breakout trades indices with momentum continuation (fast and news
markets), session expansion (trend) and squeeze release, so the Runner will
now trade far less often - mostly session expansion. That is the point:
fewer trades, the kind that ran.

**A trade it skips** is said once in the log ("Momentum Runner: US30 not
entered: momentum continuation is not ridden: ...") and nothing is
recorded; the status shows `skip_tactics`. A real trade whose approach is
unknown (no journal row) is ridden, as before.

**Size.** The Runner copies the real trade's volume and never sends more
than that volume - so if Trend & Breakout sizes an index trade up as a
top-opportunity trade (docs/strategies/v2-commission-first.md, version 5),
the Runner's size follows it, up to exactly that volume. With version 5's
defaults the top segment is momentum continuation on FX minor pairs, which
the Runner never rides, so today the two never meet.

## 9 October: Trend & Breakout on PAPER, the Runner LIVE

Aaron's line-up: the Runner (the winner) stays LIVE; Trend & Breakout goes
to PAPER (`"tnb": {"mode": "PAPER"}`, set by the installer's line-up step).
Trend & Breakout keeps deciding on real prices with simulated orders
(docs/strategies/tnb_paper.md), so the Runner still sees every fresh index
entry and opens its **own real order** beside it, exactly as before. The
trader hands the Runner the real broker, never the paper wrapper, so its
orders, stop moves and window-end closes go to MetaTrader under magic
990511. Nothing else about the Runner changes.

"The winner stays, but look at the historics and see if there are any
subtle changes we can make to strengthen the winner." Two were built and
tested; on the evidence, **both are left off** for now.

### The runner feed (built, list empty)

Trend & Breakout's own rules have switched several approaches off in some
market conditions because they lost with ITS exit (the ladder), so the
Runner no longer receives them, although some of them did well under the
Runner's 3 R trail. With Trend & Breakout on paper, `runner.feed_tactics`
lets its scanner still OFFER listed approaches on index markets even where
`scan.disabled_tactic_regimes` or `scan.disabled_tactic_groups` would block
them. Such a trade is a PAPER Trend & Breakout trade (the Runner's order
beside it is the real one), tagged `runner_feed` in the journal
(`entry_state_json`, and the ENTRY event), so Trend & Breakout's own record
can be reported without it (`mintel.runner.feed.is_feed_trade`).

What does not change for a feed trade: excluded markets stay out of the
universe; the score floor, news, spread, cost, reward:risk, exposure caps,
breakers and the daily loss stop all apply; Trend & Breakout's own
tradable signal always comes first (the feed only fills in where its own
rules give nothing tradable); approaches the Runner skips (momentum
continuation) and approaches switched off everywhere are never fed. It
works only while the trader really holds the paper wrapper with index
orders on paper (never in LIVE, never with `tnb.live_groups` holding
indices), so a feed signal can never become a real Trend & Breakout order.
Settings: `"APPROACH"` (any condition) or `"APPROACH:CONDITION"`, e.g.
`"runner": {"feed_tactics": ["BREAKOUT_RETEST:TREND"]}`.

**Why the list is empty.** The rule set for it: an approach (in its market
condition) is fed only if it is positive under the 3 R trail on at least
about 8 trades over at least 4 days, with more days up than down, counting
only trades today's other rules would still allow. On the broker's
minute-bar replay (`potential_2026-10-07.csv`, 97 index trades opened 16 Sep
to 6 Oct, joined by ticket to the daily trade files for score and
condition), the 64 non-momentum index trades made +43.46 R under the 3 R
trail (the ladder +5.51 R). Restricted to what today's rules allow, 3 have
no score on file, 24 were on markets now excluded (DE40, UK100) and 24
scored under 70, leaving 13 (+12.77 R, but up on only 3 of 8 days):

| Approach | Condition | Trend & Breakout's rules | Trades | R (3 R trail) | Days | Up | Down | Ladder R |
|---|---|---|---|---|---|---|---|---|
| Breakout retest | trend | off | 5 | +7.06 | 5 | 2 | 3 | +2.25 |
| Liquidity sweep reversal | fast market (HIGH_VOL) | off | 5 | +3.46 | 4 | 1 | 3 | +2.36 |
| Breakout retest | fast market (HIGH_VOL) | off | 1 | -1.00 | 1 | 0 | 1 | -1.05 |
| Session expansion | trend | on (already ridden) | 2 | +3.25 | 2 | 1 | 1 | +3.75 |

None has 8 trades, and none has more days up than down. The headline
figures quoted on 8 Oct (breakout retest +17.9 R on 36, squeeze release
+13.3 R on 3, liquidity sweep reversal +8.2 R on 8) count trades scored
under 70 and the excluded DE40 and UK100; on markets still traded,
breakout retest in a trend was 12 trades, +17.3 R, 5 days up and 3 down -
it qualifies only if the score floor is ignored. So the list stays empty
until the Runner's own record or a longer replay says otherwise.

### The Runner's top size (built, no segment listed)

Today the Runner copies the real trade's volume. A ride in a segment
listed in `runner.top_segments` - `[approach, kind of market]` or
`[approach, kind of market, condition]` - would instead be sized for
`runner.top_risk_money` (40, the same as `risk.top_risk_money`) at its own
stop, LIVE only:

- never above `risk.max_risk_pct` of the REAL account equity (1.5% = 30 on
  2,000, which binds before 40), the broker's volume limits, or the margin
  checks (80% of free margin, the margin level kept above
  `risk.min_margin_level_pct`); indices have no lot cap, FX and metals at
  most `runner.top_max_lots` (0.5);
- never below the real trade's volume (if the limits leave less, it copies);
- before each such order it re-checks its OWN newest LIVE rides in that
  segment (`top_lookback_rides`, 40): once it has `top_recheck_rides` (10)
  of them and they are negative (net or R), it sizes normally and the log
  says why ("normal size: its own last 10 live rides in ... are
  negative ...");
- a loss never raises a size: the segment list is fixed evidence, a loss
  lowers the equity the 1.5% is taken from, and the re-check can only turn
  the top size off. Each ride's row says whether it was top-sized
  (`top_size`), with the condition it was taken in (`regime`).

**Why no segment is listed.** The rule is Trend & Breakout's own
top-opportunity rule, measured on the Runner's exit: at least 20 trades
over at least 4 days, positive per trade and in R after costs (indices
carry no commission at this broker; the replay pays the spread), and more
winning days than losing. Per approach on indices under the 3 R trail
(momentum continuation is not ridden):

| Breakout retest on indices | Trades | R | R a trade | Days | Up | Down |
|---|---|---|---|---|---|---|
| every index trade | 36 | +17.91 | +0.50 | 9 | 6 | 3 |
| markets still traded | 19 | +24.44 | +1.29 | 9 | 7 | 2 |
| and scored 70+ (today's floor) | 6 | +6.06 | +1.01 | 6 | 2 | 4 |

The other approaches are smaller still (liquidity sweep reversal 5, session
expansion 2 at today's floor; the Runner's own record adds one session
expansion ride, 7 Oct, +7.67 R; its only LIVE ride so far was momentum
continuation). Breakout retest only reaches 20 when trades on markets now
switched off are counted, which the rule does not allow. So the top size
is OFF (`top_segments` empty) and every ride copies the real volume, as
before. Listing a segment later needs the same table, recomputed, to clear
the rule; its numbers then go here and in `mintel/config.py`.

### Not yet evaluated: entering once the real trade has proved itself

`runner.confirm_r` (default 0 = off): wait until the real trade is this
many R ahead, then enter at that price with the stop one of the real
trade's R behind the Runner's own entry (same size, same money at risk),
and ride it the same way. A real trade stopped before it gets there is
never entered; if the price is already more than `max_entry_drift_r`
(half an R) past the mark, the Runner waits for it to come back rather than
chase. In LIVE the two-minute freshness limit does not apply to it (the
mark can come an hour later); the stop must still be on the right side.

**The data on hand cannot evaluate it**: the replay files hold each
trade's peak, not its path, so whether waiting for +0.5 R or +1 R beats
entering with the trade is unknown. It is built, tested on synthetic bars,
and off. To evaluate it on Aaron's Mac from the broker's own minute bars:

    python -m mintel.ops.potential --config ~/MarketBot/data/config.json --runner

which prints, for every index trade, the Runner's result under each
variant - with the trade (every approach / no momentum, as version 5),
a 2 R trail, and confirmed at +0.5 R and +1 R - with trades taken, wins,
total R, R per trade and days down, plus a split by approach; it writes
`data/potential-runner.txt` and `.csv` (`--upload` puts them on the reports
branch). A confirmed entry pays the spread and a bar that reaches both the
mark and the new stop counts as stopped, so it leans pessimistic. Switch
it on only if that table shows it beating "no momentum (version 5)".

## How it works

| | |
|---|---|
| Entry | the real trade's fill, taken the moment Trend & Breakout opens an index position (`US30`, `US500`, any `INDEX` market not excluded) - except momentum continuation, from version 5 |
| Size | the real trade's size, so the money is like for like (a LIVE top size exists for listed segments, `runner.top_segments`; none is listed - see "The Runner's top size") |
| Stop | the real trade's ORIGINAL stop (from the FlowLock tracker, not the trailed one at the broker) |
| Riding | nothing moves until the best price is +3 R; then the stop trails 3 R behind it and never retreats |
| Out | at the stop (counted AT the stop, never better; a buy on the bid, a sell on the ask), or at the price after 8 hours |
| The real trade | unaffected; the shadow outlives it when the ladder closes first |
| Costs | PAPER: commission is zero on indices at this broker; spread is paid through the bid/ask marking. LIVE: whatever the broker charged, inside its own figure |
| Records | `data/runner.sqlite` (table `trades`, one row per trade: `ticket` is the Runner's own broker ticket in LIVE, the real trade's in PAPER; `source_ticket` is always the real trade it rode, `0` for an orphan the Runner closed; `mode` says which; table `skipped`, one row per real trade the LIVE Runner said no to for good, with why), `data/runner-status.json`; its own tab on the page; `reports/<date>/runner.json` nightly |
| Settings | `"runner": {"enabled": true, "mode": "LIVE", "magic": 990511, "trail_r": 3.0, "window_hours": 8.0, "markets": [], "skip_tactics": ["MOMENTUM_CONTINUATION"], "confirm_r": 0.0, "feed_tactics": [], "top_size_enabled": true, "top_segments": [], "top_risk_money": 40.0, "top_max_lots": 0.5, "top_recheck_rides": 10, "top_lookback_rides": 40}` in `config.json` (all optional; `mode` is `PAPER`, `LIVE` or `OFF`). The LIVE freshness limits (2 minutes, half an R) are the engine's defaults (`max_entry_age_seconds`, `max_entry_drift_r` in `mintel/runner/shadow.py`) |
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
| 8 Oct | 3 | 1 | +9.05 (paper +17.83 on 2; LIVE -8.78 on 1, the broker's figure) | -1.59 on the same three (US30 +4.47, US30 +3.25, US500 -9.31) | all three momentum continuation: US30 ridden to +2.57 R (+24.96, peak +5.57 R) where the ladder kept +4.47; US30 to the full stop (-7.13) after a +1.86 R peak, where the ladder kept +3.25; the first LIVE trade, US500, stopped in four minutes (-8.78). Seven momentum trades in two days: one win. From 9 Oct momentum continuation is not ridden (version 5) |
