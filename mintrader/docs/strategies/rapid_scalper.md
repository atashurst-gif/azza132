# Rapid Scalper

**Fast in. Fast out. Cut failed trades quickly and give exceptional winners room to run.**

Rapid Scalper is a second, completely separate strategy that runs beside the
existing bot ("Trend & Breakout"). It shares the broker connection and the
account, and nothing else.

## What "separate" means in practice

| Thing | Trend & Breakout | Rapid Scalper |
|---|---|---|
| Strategy id | `market_intelligence` | `rapid_scalper` |
| Magic number on every order | `990311` | `990411` |
| Settings file | `data/config.json` | `data/scalper.json` |
| Trade records | `data/journal.sqlite` | `data/scalper.sqlite` |
| Status file | `data/status.json` | `data/scalper-status.json` |
| Process | `python -m mintel.run` | `python -m mintel.scalper.run` |
| Heartbeat the watchdog checks | `trader` | `scalper` |
| Code | `mintel/engine/…` | `mintel/scalper/…` |

Each bot only ever reads, modifies or closes positions carrying its own magic
number. The existing bot's code does not import anything from the scalper
package, so its behaviour is unchanged whether the scalper is OFF, PAPER or
LIVE. The tests in `tests/test_rapid_scalper.py` (class `TestIsolation`) prove
this: the scalper never adopts or touches a Trend & Breakout position, and the
existing bot never tracks or trails a scalper position.

The only shared thing is the account. "Overall" on the status page is the two
strategies added together and is the account's definitive daily result.

## Modes

`data/scalper.json` has a `"mode"` field:

- `"OFF"` - the scalper process exits at once and the watchdog does not start it.
- `"PAPER"` - **the default.** It watches real ticks and makes real decisions,
  but fills are simulated (with configurable slippage and latency). No orders
  reach the broker. Trades, stats and the page tab all work exactly as in LIVE.
- `"LIVE"` - real orders. **This is a deliberate edit of the file, never a
  restart or an install.** An unrecognised mode is treated as OFF.

A fresh install writes the default file (PAPER). To go live:

1. Open `~/MarketBot/data/scalper.json`.
2. Change `"mode": "PAPER"` to `"mode": "LIVE"`.
3. Stop and start the bot (the desktop icon, or `mintel_mac.sh stop` / `start`).

The page's Rapid Scalper tab shows the mode at all times.

## Sizing: the ten-pound ceiling

Every trade is sized from the distance to its structural invalidation (the
micro swing plus a spread buffer) so that the **planned** loss at the stop,
including spread, commission and a slippage allowance on both sides, is at or
under `planned_max_trade_risk_gbp` (10.00 by default). If even the broker's
minimum lot would plan more than that, the trade is skipped and logged as
rejected with the reason. The ceiling is a maximum, not a target.

The server-side stop is always attached. If a live order comes back without
one, the position is closed immediately (fail closed). Same for a position
found at the broker with the scalper's magic and no stop.

## Entry: the confidence score

Every candidate gets a score out of 100 built from logged components
(momentum, trend alignment, entry quality, spread, liquidity, volume,
volatility, structure). Order-flow weight is zero because this feed has no
genuine depth-of-market data, and the code says so rather than pretending.
Penalties (late entry, chop, spread expansion) and blockers (spread, liquidity,
news, breakers) are logged beside the score. The breakdown of the best
opportunity is on the page under "Technical detail".

## Management: the state machine

```
INITIAL_RISK -> RISK_REDUCTION -> CAPITAL_SAFE -> PROFIT_PROTECTED -> RUNNER
```

- **Prove-it** (first `prove_it_seconds`): the thesis must show progress or the
  trade exits as `THESIS_FAILED`. Adverse movement beyond
  `thesis_fail_adverse_r` inside the window also exits.
- The hard stop **only ever tightens**. `Manager.enforce_monotonic` raises
  `StopInvariantError` on any backwards attempt, and the fuzz test throws five
  hundred random candidates at it per side.
- A high-water mark is kept per trade; the allowed give-back from it shrinks
  as the state advances (`giveback_early` -> `giveback_established` ->
  `giveback_runner`).
- `MOMENTUM_DECAY` exits when velocity collapses relative to the peak;
  `STRUCTURE_BREAK` when the micro structure fails; `SPREAD_EMERGENCY` when
  the spread blows out while in a trade.
- A runner probability (0-100) decides whether an exceptional winner is given
  room; partial profit-taking is available but off by default.

Exit taxonomy: `INITIAL_STOP, THESIS_FAILED, MOMENTUM_DECAY, PROFIT_TRAIL,
STRUCTURE_BREAK, SPREAD_EMERGENCY, SLIPPAGE_PROTECTION, RISK_KILL_SWITCH,
ACCOUNT_PROTECTION, MANUAL_CLOSE, SYSTEM_SHUTDOWN`.

## The spread is a cost, not a signal (change of 1 October)

The first live hour showed four trades, every one cut inside five seconds as
THESIS_FAILED. The cause was that progress was being judged on the price we
could close at, so a fresh buy already read as "going against us" by one
spread the instant it filled, and stops of around ten points sat inside
ordinary spread noise. Three changes:

- progress and adverse movement inside the proving window are judged on the
  **mid** price; the spread was paid at entry and is not the thesis failing
- a stop must clear **four spreads** as well as `min_stop_points`
  (now 15); anything tighter is refused and logged as rejected
- the proving window is 15 seconds, not 5

The page's trade list now marks every scalper trade PAPER or LIVE.

**The planned loss is the exit.** The early THESIS_FAILED exit now fires only
when the trade is certainly heading for its stop: at least 0.8 R against on
the mid price AND the tape still moving against it (negative velocity or
four or more ticks in a row the wrong way). After the proving window a trade
with no progress is cut only if it is at least 0.5 R under and moving
against. A flat, wobbling or merely negative trade is left alone; the
server-side stop is what takes the loss, and that loss is capped at the
planned maximum. With no live features at all the bot never guesses.

## True numbers and real costs (change of 1 October, later the same day)

- **LIVE figures come from the broker.** The Rapid Scalper tab's money
  (net, realised, trades, wins, losses, averages, costs, profit factor,
  drawdown, and the net on every row of the trade list) is read from
  MetaTrader's own deal history for the scalper's magic number, profit plus
  commission plus swap. Every live close also takes its result from the
  broker's deal rather than from our arithmetic. If the broker cannot be
  asked, the tab says so and shows the bot's own records; it never shows an
  invented zero. PAPER figures are labelled "simulated".
- **Costs are a gate.** A trade is refused, and logged as rejected with the
  reason, when commission + spread + slippage allowance would be more than
  30% of the planned loss, or when the expected move is under three times
  the round-trip cost in points. The first live hour's costs were about
  £2.75 a trade against an average loss of £4.10, which is a trade that
  loses before it starts.

## The settings file holds only the mode (fix of 1 October, evening)

The first build wrote every default into `data/scalper.json`. That froze the
first day's numbers on the Mac (5-second proving window, 0.55 R, 8-point
stops), so the afternoon's rule changes never actually took effect live: the
file's old values overrode the new defaults. Found in the nightly review
from the exit reasons in the broker's records.

Now the bot writes `{"mode": "PAPER"}` only, and on start it collapses an
old full dump back to the mode. New defaults in the code reach the Mac on
the next install. To pin a setting deliberately, add it to the file together
with `"keep_overrides": true` and it will be left alone.

## Lessons of the first live day (changes for 2 October)

Thirteen live trades on 1 October: 3 winners, net -27.23, of which 17.24
was commission. The broker's records showed four things, each now a rule:

1. **Commission must be small next to what the stop risks.** The cost
   gate is now measured against the money at the raw stop distance, not
   the padded planned loss, at 25%. A half-lot GBPUSD trade with a one-pip
   stop paid 2.50 of commission to risk 4.00 at the stop; that trade is
   refused and logged. Minimum stop is 20 points and four spreads.
2. **A market that just lost rests for ten minutes.** Gold took seven of
   the thirteen trades, several within a minute of a loss there.
3. **Three losses in a row pause everything for 30 minutes.** Six in a
   row end the day. Previously four ended the day, which is too blunt for
   a style whose win rate can sit under 40%.
4. **Two minutes between any loss and the next trade** (was 45 seconds).

Fewer trades, each one able to pay its own commission, no pressing after
losses, and the stop as the exit unless the trade is certainly going there.

## Slippage is measured in spreads, and a bad patch is a rest, not a lock (2 October)

On the morning of 2 October the scalper made +17.34 on six gold trades by
00:46 and then sat paused for eight hours: "average slippage 4.8 points".
On gold a point is one cent and the spread is around fifteen points, so
4.8 points was nothing. Worse, the breaker could never clear because it
only re-measures on new fills, and it was blocking the fills. Now the
limit is the larger of 4 points and one average spread, and tripping it
rests the scalper for 15 minutes and then measures afresh.

## Break-even earlier (change of 2 October, evening)

On 2 October four trades were up 0.64 to 0.98 R and then stopped out for
a loss, -30.12 between them, two of them (peaks 0.85 R and 0.98 R) just
short of the 1.0 R where the stop moves to break-even. The day was -1.62
on 24 trades; with break-even at 0.8 R those two are flat and the day is
about +14. The 1 October trades show the same shape at 0.5 R (two full
stops after 0.52 R and 0.57 R peaks). `capital_safe_at_r` is now 0.8.
The runner and profit-protection thresholds are unchanged.

## Weekend review, 4 October

Two live days, 37 trades. Day one -27.23 (costs 108% of gross wins); day
two to 15:32 UK -1.62 on 24 trades (costs 15% of gross wins, profit
factor 0.98, average win 8.59 against average loss 7.39). The exits that
earn are MOMENTUM_DECAY (+64 on 6) and the trailed winners; the early
THESIS_FAILED cuts cost 40 on 6 and fired as written. Break-even now at
0.8 R (above). The slippage rest tripped twice on Friday for average
slippage under one spread, so the yardstick is now 1.5 spreads.

## Commission-free indices could never trade (fix of 4 October)

Every scalper trade so far was gold or FX, never US30, US500, DE40 or
UK100, although all four were in its list. The spread cap (25 points) and
stop range (20 to 120 points) were in broker points, and an index point
at this broker is 0.01. A normal 2-point US30 spread read as 200 points
and was refused every time. Limits for the indices are now in index
points (`symbol_limits`): US30 spread up to 4, stop 6 to 60; US500 1,
1.5 to 15; DE40 3, 5 to 50; UK100 2.5, 4 to 40. The indices carry no
commission at this broker, which is the cost the scalper was losing to.
All the other gates still apply: confidence, the cost gate, the four-
spread minimum stop, the GBP 10 planned loss, the rests after losses.

## The liquidity gate measured the bot, not the market (5 October)

Three live days: 69 trades, +12.60 on price, 50.18 in costs, net -37.58.
Back to PAPER on 5 October. The same morning the rejected-setup log
showed almost every market "too thin (9 ticks/min)" all night - US30,
EURUSD, USDJPY alike. The scalper polls the latest price once per pass
over its 15 markets, so its buffer could never hold more ticks than
passes per minute; a 20-tick floor therefore blocked nearly everything
and only gold slipped through now and then. Every live trade so far was
gold for that reason, not by choice.

Fixes: liquidity is the broker's own tick count per one-minute bar
(median of the last three completed bars); the status file is written
every 2 s instead of every pass; the broker's day figures are cached for
30 s (refreshed at once after a close); rejected setups are logged once
per market per reason per minute (tens of thousands of rows a night
before); the other bot's day, used by account safety, now counts
commission once and includes the entry half. The status page shows how
long one full pass takes ("one full pass (ms)").

It stays in PAPER until its result on price is clearly larger than its
costs over several days. With the gate fixed it will see many more
setups; the cost gate (25% of the money at the stop) and the confidence
bar decide which of them it takes.

## Commission per market, and when to trade (5 October)

Indices now carry no commission in the scalper's plan and results
(`symbol_limits[...]["commission"] = 0`); before, they were charged the
6.00 a lot used for gold and FX, which made them look worse than they are.

`entry_hours_utc` limits when new trades may open (empty = every hour).
Gold in the first hour after midnight UTC: Fri 6 trades +17.34, Mon 6
trades +74.38. Every other hour of the three live days: 57 trades, -128.
Two days is not proof. The scalper stays in PAPER at every hour to
measure it; if it holds, LIVE in that hour alone is
`{"mode": "LIVE", "entry_hours_utc": [0]}` in data/scalper.json.

## Look before moving, ride the highs (5 October, Aaron's rules)

The paper trades after the reset showed the scalper still cutting winners
short. Three reasons, all fixed:

- the profit trail followed the lowest tick of the last few seconds, so a
  normal flicker inside a move closed the trade. It now trails behind the
  last two completed one-minute bars (or the give-back from the high, now
  50%, whichever leaves more room); the stop still only ever tightens
- "momentum gone" fired on five ticks the wrong way. It now needs BOTH the
  1-minute and 5-minute moves to have turned against the trade, and only
  once profit is protected
- it never looked past about 30 seconds of ticks. Every entry now needs
  the last 1, 5 and 20 minutes all moving the trade's way
  (`require_timeframe_alignment`)

Losses are cut sooner, as allowed: a trade still at its initial risk exits
when it is 0.3 R under and the 1- and 5-minute moves have turned against
it, and the "certainly going there" early exit is now at 0.6 R (was 0.8).

## Safety

Circuit breakers (all in `scalper.json`): consecutive losses, scalper daily
loss, account daily loss, total open risk across both bots, margin level,
average slippage, stale ticks, latency, API errors and rejects per hour, and a
reconciliation check. Any tripped breaker blocks new entries and the reason
is shown on the page as "Not trading because". There is no revenge logic:
after a loss there is a cooldown and nothing else changes.

The scalper never touches the existing bot's daily-loss stop.

## Backtesting

`python -m mintel.scalper.backtest --symbol EURUSD --days 5` replays real
ticks from the broker through the same scoring and management code with
costs, then runs the stress set (double spread, extra slippage, delayed
fills, dropped ticks), a parameter neighbourhood and a walk-forward split.

## Switching LIVE from the terminal

One command does it: it writes `"mode": "LIVE"` into `~/MarketBot/data/scalper.json`
(creating the file if it does not exist yet), then installs the current build
and restarts everything so the scalper comes up live:

```
mkdir -p ~/MarketBot/data && f=~/MarketBot/data/scalper.json && { [ -f "$f" ] && sed -i '' 's/"mode"[[:space:]]*:[[:space:]]*"[A-Za-z]*"/"mode": "LIVE"/' "$f" || printf '{"mode": "LIVE"}\n' > "$f"; } && grep -q '"LIVE"' "$f" && echo "Rapid Scalper set to LIVE" && cd ~/Downloads && rm -rf azza132-claude-eager-newton-ra06qu* && curl -fsSL -o bot.zip https://github.com/atashurst-gif/azza132/archive/refs/heads/claude/eager-newton-ra06qu.zip && unzip -q -o bot.zip && rm bot.zip && bash "azza132-claude-eager-newton-ra06qu/mintrader/deploy/mac/Start Trading Bot.command"
```

To go back to PAPER, change the word back and restart. The Start window
prints the Rapid Scalper mode at the end so there is never any doubt.

## Period filter on the page

Above the figures there is a row of periods: Today, Yesterday, This week,
This month, Last 6 months, Last year, and a From/To calendar for any range.
The choice applies to whichever tab is open. Today's figures are the broker's
own ledger, as before. Every other period is added up from the two bots'
journals (`journal.sqlite` and `scalper.sqlite`), read-only, and the page
says which it is showing. Day boundaries follow the Mac's clock.

## Where things show up

- Status page: tabs **Overall | Trend & Breakout | Rapid Scalper**, each with
  its own figures and trade list, and the live scalper panel (status, mode,
  best opportunity, confidence, current position with planned risk and
  protected floor).
- Logs: `logs/scalper.out.log` and the journal tables `trades`, `rejected`,
  `stop_changes`, `slippage`, `events`.
- The nightly bundle includes the scalper's day.
