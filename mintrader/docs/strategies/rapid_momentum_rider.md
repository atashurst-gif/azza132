# Rapid Momentum Rider (magic 990811)

**Find the FX market that has just started running hard, get on early with
a real stop at the broker, ride it while it is strong, and get out fast
when the move dies.**

It replaces the Rapid Scalper (990411, retired; its code and records are
kept). It is a new strategy built from first principles on the proven
plumbing: the MetaTrader connection, the shared order executor
(`mintel/botexec.py`), heartbeats, logging and the pages. Aaron's brief is
`docs/specs/rapid_momentum_rider_SPEC.md`; the code is `mintel/rider/`.

**What has and has not been shown.** Everything below describes how the bot
is built. Its tests run on made-up (synthetic, seeded) price paths. They
show the machinery behaves as designed. They say nothing about whether it
makes money. No performance figure in this document comes from real
prices. Real-tick research is a separate piece of work
(`mintel/rider/research/`), and its figures go in its own reports, each
labelled with where the data came from.

## What it does, in one breath

Every second it looks at every liquid FX pair the broker offers. Those are
the 28 pairs made from USD, EUR, GBP, JPY, CHF, CAD, AUD and NZD. It asks
two questions: "which market is running right now?" and "is this a real
directional move or just noise?". It does not try to predict the market. If
nothing is running, it keeps watching. If something is, it gets on board
with a stop already at the broker. If it is wrong, it gets out quickly. If
it is right, there is no small fixed target: it lets the move run and
raises a profit floor behind it. When the move tires, it tightens the
floor. When the move dies, it takes the profit and goes looking for the
next one.

The aim is many small, controlled failed attempts, normal profitable
scalps, and now and then a very large ride. What it must never do is take
lots of tiny wins and then one huge loss.

## How it finds a run

Each market gets a **MOMENTUM_OPPORTUNITY_SCORE** from 0 to 100, a
direction (LONG, SHORT or NONE) and a reason in plain words. For example
(an illustration of the format, not real readings):

    GBPUSD  SHORT  94  TRIGGER   Very fast clean downward momentum - breaking below the Asian session's low
    EURUSD  NONE   43  WATCHING  Moving quickly but too choppy

The score is built from **categories**, not from 30 indicators each
getting a vote. Each kind of evidence counts in exactly one category.
Measures that say the same thing are grouped, so they are never counted
twice. Every distance is measured in "ATRs": the market's own typical
one-minute range. That way a quiet pair and a wild pair are judged on the
same scale.

| Category (points) | What it measures |
|---|---|
| Volatility expansion (8) | Is movement picking up? Fast against slow ATR, the last minute's range against normal, tick volatility, Bollinger inside Keltner (a squeeze), and a squeeze releasing. |
| Directional efficiency (12) | **TREND_EFFICIENCY** = how far price got, divided by how far it travelled. A clean run scores near 1, back-and-forth near 0. |
| Velocity (12) | How fast price is moving over 1, 3, 5, 10, 15 and 30 seconds and 1 and 3 minutes, in ATRs. |
| Acceleration (8) | Is it speeding up (and is the speeding-up itself growing)? |
| Price structure (7) | Higher highs and higher lows (or the reverse) in the tick-level swings, strong candles closing near their extremes, and a stop-hunt that snapped back. |
| Breakout quality (10) | Breaking the 20-minute range, the first clean retest of a broken level, the London or New York opening range, or the Asian, London or previous day's high or low. It looks at how far through, for how long, how strongly, how busy the market is, and whether it is late. |
| Pullback quality (5) | A shallow, slow, quiet pullback in a strong move that is now turning back the right way. |
| Participation (6) | How many price updates arrive against normal. This is the broker's tick count. It shows activity, not traded volume. |
| Multi-timeframe (8) | One-minute momentum measurements, the shape of the moving averages, ADX and how it is changing, VWAP, and the five-minute slope. |
| Session (4) | The London and New York opens and their overlap, or Asia for yen and Antipodean pairs. |
| News (3) | MetaTrader calendar events for the pair's currencies (see below). |
| Currency strength (5) | How strong each currency is across all the pairs. Example: EUR weakest and USD strongest puts EURUSD SHORT first. |
| Cross-market (4) | A broad currency move ("the dollar is rising against everything") counts for more than a move in one pair only. |
| Headroom (3) | Open space to the next obstacle: a swing, a finished session's high or low, yesterday's high or low, or a round number. |
| Execution cost (3) | **COST_RATIO** = (spread + commission + slippage) / the move it can expect. |
| Historical match (2) | How similar set-ups have done before (see "What it learns"). It stays neutral until there is enough history. |

**Anti-chop is a gate, not points.** Chop means price going back and forth.
Signs of it are: low two-minute efficiency, a big range with little
progress, four or more big reversals inside two minutes, and failed
breakouts. Flat averages, weak ADX and repeated VWAP crossings add weight
only when the ticks are choppy too. Chop means the direction is NONE:
wait. A market that triggered one way and then the other inside 90 seconds
is called a whipsaw, and it waits too.

**What does not exist, said plainly.** Retail MetaTrader FX has no real
order flow and no depth of market. The bot has no order-flow category, and
it never fakes one from candles or tick counts.

## How it enters

Each market moves through these states:
**WATCHING -> BUILDING -> READY -> TRIGGER -> ENTERED**

- **BUILDING:** the score reaches 40 in a direction.
- **READY:** the score reaches 55, and every gate is passed. The gates
  are: not choppy; the costs are no more than 15% of the expected
  move (a quarter until 9 Oct, when the first live morning showed
  commission eating the wins); at least 1.2 ATR of open space ahead; no high-impact news for
  either currency due in the next two minutes; the spread not blown out;
  and enough price updates.
- **TRIGGER:** the score reaches 60, and price itself confirms the
  direction. Confirmation means the live price has just broken the high
  (or low) of the last 20 seconds, and the last 5 seconds moved the same
  way.

A fast move can go straight from WATCHING to TRIGGER, because getting on
early is the point. No condition makes it wait for the next one-minute bar
to close.

On a TRIGGER the bot:

1. **Sizes the trade** from Aaron's GBP per pip (see below).
2. **Places the stop.** It goes just beyond the last small swing against
   the trade (the point where the move would be proved wrong), plus a
   small buffer and half the spread. It is never closer than 0.6 ATR, 3
   spreads or the broker's minimum. If the swing is further than 3 ATR
   (or 30 pips), the stop is put at that limit instead (a volatility stop).
3. **Sends the order with the stop on it**, under magic 990811. If the
   broker does not hold the stop, the position is closed again at once.
   There is no take-profit.

Rules on top: one position per market. At most 4 open at once; that is a
sanity cap, not a target. No quotas and no daily limits: the market
decides. After a trade ends, the same market can be traded again only on
a **fresh** trigger, meaning a new arrival in TRIGGER, not the same one.
There is also a 20-second cooldown so the same signal is never taken
twice. Momentum comes in waves, and a new wave may be ridden again. The
two guards added on 9 Oct (next section) can also refuse an entry.

**News.** News alone is never a reason to trade. Before a high-impact
release for one of the pair's currencies, new entries wait. After it, the
first 20 seconds are never chased. After that the bot measures from price
what happened:

- the first jump;
- whether a second wave carried on beyond it;
- whether the move was rejected;
- whether the spread has come back to normal.

## Guards added on 9 Oct

**What happened.** The Rider's first LIVE morning was Friday 9 October.
The broker's own figures: 23 trades between 08:54 and 11:05 UTC; 9 won
and 14 lost. Before costs they made +4.7 pips. After GBP 22.92 of costs
(commission is about GBP 1 a trade at GBP 1 a pip) the result was
-GBP 18.27. By exit:

- THESIS_DEAD: 11 trades, -GBP 32.62.
- STOP: 8 trades, -GBP 8.19.
- PROTECT_LEVEL: 4 trades, +GBP 22.54.

Eleven of the trades came in five minutes (08:54-08:59), most of them JPY
crosses on the same yen move: EURJPY four times (-4.97, -5.07, -2.56,
-4.17), GBPJPY twice, CADJPY and AUDJPY. Later CHFJPY was traded four
times (-3.76, -3.16, -3.47, +0.24). One idea was traded many times over,
and every time it paid the costs again.

**Two guards now stop that.** Both only ever refuse a new entry. They
never change a size, a score, a threshold, a gate, or how FlowLock X
manages a trade. Both are on by default, and each can be switched off in
`data/rider.json`.

1. **The currency cluster** (`currency_cluster_guard`). Every pair is two
   bets: short EURJPY is short EUR and long JPY. A new entry is refused
   when it would add to an open position's bet on the same currency in
   the same direction, beyond `max_same_currency_positions` (1). Short
   EURJPY and short GBPJPY are both long JPY, so the second one is refused,
   and the scanner says "already long JPY through EURJPY". Long EURUSD and
   short USDCHF are both short USD. Long GBPJPY (short JPY) is a different
   idea, and it is allowed.
2. **The loss cooldown** (`loss_cooldown_guard`). A loss means a loss
   after every cost.
   - After a losing exit on a market, no new entry there for 15 minutes
     (`loss_cooldown_minutes`).
   - After two losing exits in a row on a market, the second within 2 hours
     of the first, none there for 2 hours (`loss_pause_minutes`). A win in
     between breaks the run. Two losses further apart (yesterday's last
     trade and today's first, say) get only the 15-minute rest, so the
     research replay, which starts each UTC day afresh, measures the same
     rule that trades live.
   - A losing exit also rests its two currency bets for 15 minutes
     (`loss_cooldown_same_currency`). After a losing short EURJPY, there is
     no new long JPY and no new short EUR anywhere for 15 minutes. Without
     this, the same yen idea came straight back through the next JPY cross
     as soon as the first trade had closed.
   - The brake: after 4 losing exits within 10 minutes, no new entry on any
     market for 15 minutes (`loss_brake_losses`,
     `loss_brake_window_minutes`, `loss_brake_minutes`).

**What you see.** A market that is held back says so at the start of its
reason on the scanner, for example: "Held back: CADJPY lost 3 min ago: no
new entry on it for 15 min - 12 min left." The status file has a `guards`
block that lists what is held right now. The "why no trade" record counts
each guard by name.

**After a restart** the waits carry on. The bot reads its recent closed
trades back from `data/rider.sqlite`. A trade it did not decide itself (an
orphan it closed, or a trade closed by a mode switch) does not count.

**Research.** The replay applies exactly the same rules, because they live
in RiderCore. The registry gets a pair: RMR-GUARDS-ON and RMR-GUARDS-OFF.
RMR-BASE keeps its definition (the shipped settings, which now include the
guards).

**Tested on made-up prices only.** A SYNTHETIC replay of the 08:54-08:59
shape uses four quick yen waves through five JPY crosses. Without the
guards it entered 12 times, every one of them long JPY. With the guards it
entered once. This shows that the rules work as designed. It says nothing
about profit; the research pair on real prices has to show that.

## How FlowLock X manages the trade

**Rule 1: the stop never widens.** On a buy, the stop only ever moves up.
On a sell, it only ever moves down. This rule lives in one function, and
every candidate stop passes through it. Automated tests run it on 5,000
random price paths, longs and shorts, with random spreads, price gaps and
refused stop moves. They check that the stop never widens and is always
on the right side of the price. A stop that would sit at or through the
price is not sent: the trade is closed instead, because the level it was
protecting has already gone.

The states:

| State | In plain words |
|---|---|
| INITIAL | The starting stop. If the idea fails, it is out at once (see THESIS DEAD). |
| PROVING | Some progress. The possible loss is cut to 0.6 of the starting risk, but the stop never goes closer than 1 ATR to the price. |
| COSTS COVERED | The stop sits at true net breakeven: entry plus commission plus a slippage allowance. The spread is already covered, because a buy is bought at the ask and its stop fills at the bid. |
| RUNNER | Fast, clean, making new extremes, shallow pullbacks, other pairs agreeing. It is given more room (2.8 ATR), because a stronger move deserves more room. |
| PROFIT RATCHET | A floor that keeps part of the best profit so far. A young winner may give back more of it; a mature winner less; a slowing winner very little. This "giveback curve" is a setting, so research can compare several. |
| SWING TRAIL | The stop follows the last confirmed small swing. |
| ATR / CHANDELIER | The stop follows the best price, 2 ATR behind. |
| MOMENTUM DECAY | A MOMENTUM_DECAY_SCORE from 0 to 1 is built from: velocity falling, slowing down, time since the last new extreme, candles and wicks against the trade, efficiency and ADX getting worse, the averages flattening, other pairs no longer agreeing, fewer price updates, and a failed breakout. As it rises, the trail tightens. At 0.85 the move is called dead and the profit is taken. |
| EXHAUSTION | Very stretched from the averages and slowing: it keeps at least 75% of the best profit. |
| THESIS DEAD | Out now. Any of these: price fell back through the level it had just broken; it turned hard against the trade; it went 0.7 of the risk against and kept going; or it never got going in two and a half minutes. |

The stop candidates are: structure (the swing), ATR/chandelier, the profit
floor, the fast moving average, the decay-tightened trail, and
(switched off by default, kept for research) a Parabolic SAR. Each state
uses the right one, and all of them pass Rule 1.

**How often the stop is moved at the broker.** FlowLock X may want a new
stop on every tick, but the bot does not send every small change. A move
goes to the broker only when it gains at least 0.3 pip and at least one
spread (`stop_min_step_pips`, `stop_min_step_spreads`), and at least 5
seconds have passed since the last move was sent (`stop_min_modify_seconds`),
whether the broker took that one or refused it. There is one exception to
the wait, never to the size: the first move to net breakeven is sent at
once, unless the last move sent was refused. Holding a move back never
widens anything: the old stop stays at the broker, and if the price comes
back to the level FlowLock X wanted to protect, the trade is closed. The
live bot and the replay use the same rule (`RiderCore.stop_move_due`).

**If the broker is not holding the stop (LIVE).** Every pass the bot
checks that the broker holds at least the stop FlowLock X has set. A
tighter stop at the broker is followed; a looser one is never accepted. If
the broker shows no stop, or a looser one, the bot sends its stop again at
once. If the broker refuses it, or still does not show it on the next
check, or the price is already through the stop, the trade is closed at
market straight away. That close is recorded with the reason
NO_BROKER_STOP. If the broker refuses the close too, the bot tries again
on every pass until the trade is shut.

## How GBP 1 per pip is set

`user_pip_value_gbp` (default 1.0) is **Aaron's** setting. The bot reads
it and never changes it, in memory or on disk.

- **The size, per market:** lots = GBP per pip / (tick value x pip /
  tick size). MetaTrader's tick value is already in the account currency
  for one lot, so nothing is guessed.
- **The pip:** 0.0001 on 5- and 4-digit pairs, 0.01 on 3- and 2-digit
  yen pairs. It is checked against the quote currency.
- **Rounding:** to the nearest step the broker allows, never below its
  smallest size, never above its largest. The actual GBP per pip is
  recorded with every trade.
- **Examples with typical tick values for a GBP account** (the figures the tests use, not readings from the live broker):

  | Pair | Lots | GBP per pip |
  |---|---|---|
  | EURUSD | 0.14 | 1.04 |
  | USDJPY | 0.20 | 0.99 |
  | GBPJPY | 0.20 | 0.99 |
  | EURGBP | 0.10 | 1.00 |
  | AUDNZD | 0.23 | 0.99 |

- **Non-GBP accounts:** they would need an exchange rate to convert. With
  no rate there is no trade.

The size is never raised after a loss. There is no martingale, no grid and
no averaging down.

## The settings file: `data/rider.json`

The settings file is created with the mode only. A missing or broken file
gives the defaults. A broken entry keeps its own default, and everything
else still loads. The installer sets `"mode": "LIVE"`.

```json
{"mode": "LIVE", "user_pip_value_gbp": 1.0}
```

Useful keys:

- **Mode:** `mode` is OFF (trades nothing; see "Switching away from
  LIVE" below), PAPER (simulates) or LIVE.
- **Markets:** `symbols` (empty means every liquid FX pair).
- **Costs:** `commission_per_lot_side` (3.5) with `commission_currency`
  (USD), replaced by the figure learned from the broker's own deals once
  one comes in. Also `slippage_pips`.
- **Entry states:** `building_score`, `ready_score`, `trigger_score`,
  `max_cost_ratio`, `max_chop`, `min_headroom_atr`.
- **Re-entry and limits:** `cooldown_seconds`,
  `max_concurrent_positions`.
- **The 9 Oct guards:** `currency_cluster_guard`,
  `max_same_currency_positions`, `loss_cooldown_guard`,
  `loss_cooldown_minutes`, `loss_pause_minutes`,
  `loss_cooldown_same_currency`, `loss_brake_losses`,
  `loss_brake_window_minutes`, `loss_brake_minutes`. For example,
  `{"loss_cooldown_guard": false}` switches the loss cooldown off.
- **Starting stop:** `stop_min_atr`, `stop_max_atr`, `stop_max_pips`.
- **FlowLock X:** `flowlock`, a dictionary of settings, for example
  `{"curve": "tight"}`.
- **Score weights:** `weights`, a dictionary of category weights.
- **Research only:** `latency_ms` (the order delay used in replay).

## How it runs

```
python -m mintel.rider.run --config data/config.json
```

- **Process:** it runs as its own process. Heartbeat "rider", pid file
  `data/rider.pid`, log `logs/rider.log`.
- **Prices:** it reads every market about once a second. With
  `tick_history` it takes every tick since the last look, the same ticks
  a replay of history would use. Each tick is used once, even when several
  share the same millisecond. The newest price is used for the trade at
  once, but it is not added to the tick record until the broker's history
  holds it too, so no tick in between is skipped. If the history is still
  behind after 5 seconds, the newest price is added anyway, as if there
  were no history. Because the history can arrive a moment late, in PAPER
  each tick is checked against the stop that was in place at that tick's
  own time, as in the replay: a tick from before the price the trade was
  opened on cannot close it, and a tick from before a stop move is checked
  against the stop before that move. When such a tick closes the trade,
  the record shows the stop that tick met and the best and worst prices up
  to that tick. The later stop move is still listed with the stop moves,
  and the trade's note says it came after the closing price and does not
  count.
- **Restarts:** positions in its records are picked up again and managed.
  A position at the broker with the rider's magic but no record here is
  closed as an orphan. It is never adopted.
- **Switching away from LIVE:** a switch to PAPER or OFF never leaves a
  real trade running with nothing managing it.
  - **PAPER:** the first pass closes every real position under 990811 at
    the broker and records it as SWITCHED TO PAPER. After that it looks at
    the broker's list again once a minute, and closes anything under 990811
    it finds there.
  - **OFF:** the Rider trades nothing, but when it starts it first looks
    in its records for an open LIVE trade and at the broker for a position
    under 990811. If it finds one, it closes it at the broker, records it
    as SWITCHED OFF with the broker's figure, and then exits. A refused
    close is tried again once a minute (only while the FX market is open)
    for up to 10 minutes. Anything still open after that keeps its stop at
    the broker, and the next start tries again. With nothing left from
    LIVE, it exits at once. An empty list from the broker is read a second
    time a few seconds later before it decides that nothing is left.
  - **Who starts the OFF pass:** the watchdog. It does not start the Rider
    while its file says OFF, except when the Rider's records still hold an
    open LIVE trade (for example, the LIVE Rider was stopped for the switch
    with a trade open). Then it starts the Rider, which closes that trade
    and exits, and the watchdog leaves it alone after that. If the trade
    could not be closed, the watchdog starts it again, within its usual
    limit of restarts an hour.
  - **One empty answer is not proof.** MetaTrader can answer "no positions"
    when it has no answer at all. So a LIVE trade that is missing from the
    broker's list, with no closing record, is kept open and looked for
    again. It is booked as closed (an estimate, marked `?`) only once it
    has been missing for a minute over several looks, the same rule the
    LIVE bot uses.
  - **Refused closes are not hammered.** A position with no record here
    (an orphan, or one left from LIVE) whose close the broker refuses is
    tried again a minute later, not on every pass, and is not tried again
    while the FX market is closed (the weekend, and a few minutes around
    the 5 pm New York rollover). Each refusal is noted, so at most once a
    minute. It keeps its broker stop meanwhile.
  - Another bot's position is never touched.
- **Health checks:** MetaTrader alive, broker connected, algo trading
  switched on, fresh prices, markets available, spreads valid, orders
  going through, positions matching the broker, FlowLock X alive, records
  writable, disk and memory. If an important check fails, **new entries
  stop. Open positions are always managed.** That holds even when the
  records cannot be written: the record-keeping that failed is noted and
  tried again a minute later, and the stops are still moved. A trade whose
  record could not be written when it opened is still managed, and its
  record is written on the first later pass the database takes it (or,
  once it has closed, by the next minute's record-keeping, with its
  close). It reconnects automatically.
- **Status:** written to `data/rider-status.json` every 2 seconds. It
  holds the mode, the health checks, the scanner rows (MARKET, DIRECTION,
  SCORE, STATE, REASON), open positions (pips, money, stop, FlowLock state)
  and a TODAY block (trades, won, lost, win rate, net pips, net money, best
  trade). It also holds the latest trades, each explained as to a child,
  and the `guards` block (see "Guards added on 9 Oct").
- **Is the tick history really working?** `tick_feed` in the status is
  only the setting. `tick_history` says where the prices of the last 10
  minutes came from: "working" when the broker's tick history brought
  them, "NOT delivering" when only the latest price was fed, one a pass.
  On 9 Oct MetaTrader under Wine read the times it was given as UK local
  time, so every history answer was for the hour before the one asked and
  the live Rider ran on one price a pass while the status still said
  "every tick".

## The records: `data/rider.sqlite`

- `trades`: one row per trade. Each row holds:
  - the full market picture just before entry: every category and family,
    with its measurements;
  - the entry family and score;
  - MFE (best profit reached) and MAE (worst loss reached), in pips and
    money. The price the trade closed on counts too, as in the replay, so
    a price that jumps through the stop shows in the MAE;
  - realised pips;
  - **MFE_CAPTURE**: realised pips divided by the best pips reached;
  - every FlowLock X state the trade went through, and the exit reason;
  - latency: signal, order sent, broker acknowledgement, fill price and
    time;
  - the mode;
  - where the money figure came from: `broker` (LIVE: the broker's own
    closing deals plus entry commission and swap), `paper` (a simulation
    that includes commission; a stop fills as in the replay, at the stop
    or at the price that jumped through it, less slippage), or `estimate`
    (marked `?` until the broker's figure arrives). A trade that left the
    broker with no closing record yet is estimated at the worse of its stop
    and the price at that moment, never better than the stop. The same
    holds when it is found after a switch to PAPER or OFF. A real position
    that the bot closes after such a switch is booked at the price the
    broker closed it at, until the closing record can be read. When the
    broker's figure arrives, it replaces the estimate and the story is
    written again from it. That happens within a minute in PAPER or LIVE,
    and in OFF while the OFF start is still running (for up to about two
    minutes);
  - a story written for a child. For example (an illustration of the
    wording, not a real trade): *"GBP/USD suddenly started
    falling much faster than normal, so the bot sold it. It kept going
    fast, so the bot let it run and kept moving the safety stop behind the
    price (it never moves it back). When the price came back to the stop,
    the trade closed at +24.0 pips (+£24.10)."*
- `stop_changes`: every stop move the broker accepted.
- `scans`: a scanner snapshot every minute.
- `events`: starts, refusals, orphans and switches.

## What it learns

After each closed trade, the bot keeps statistics in groups: by entry
family, session, pair group, volatility regime, efficiency band and cost
band. A group counts only with at least 30 trades spread over at least 3
days. Its result is pulled toward the overall average, and only the most
recent 600 trades are used. Learning only moves the 2-point "historical
match" category. It never bans a market and never changes a rule, a stop
or a size. Nothing is rewritten from one day.

## Honest limits

- **Order flow:** there is none on retail MetaTrader FX, so there is
  none here.
- **Tick volume:** it is the broker's count of price updates, not traded
  volume.
- **Spread in the costs:** the spread used for COST_RATIO is the live
  one. Commission is configured until the broker's deals show the real
  figure.
- **Tests:** they use synthetic paths. They show the logic: a clean trend
  triggers its way, violent chop waits once it is recognisable (its first
  leg looks like a breakout, and FlowLock X's thesis checks are there to
  cut it), and a squeeze release triggers the release way. They are not
  evidence of profit.

## Research: does it work on real prices?

The research suite is `mintel/rider/research/`. It answers two questions
from the brief. First: what happens just before a large, clean, short-term
run? Second: does the bot, exactly as built, make money after every cost
when it is replayed tick by tick over real history?

**No real result exists yet.** Real tick history is only on the machine
that runs MetaTrader (the Mac, through the bridge). Until the command below
has been run there, the only output is from made-up prices. Every such
report is marked **SYNTHETIC - NOT PERFORMANCE**.

### One command

```
python -m mintel.rider.research --config data/config.json --days 10 --upload
```

- **Connects** the same way the trader does. It reads every tick (bid and
  ask) for the last 10 complete weekdays, plus a seed day before them. The
  ticks come from the broker's own history (`ticks_range`), an hour at a
  time.
- **Stores** them in `data/rider-research/ticks/<SYMBOL>/<YYYY-MM-DD>.csv.gz`.
  If the fetch is stopped, the next run carries on where it left off. A
  size budget (`--max-gb`, default 3) stops it before the disk fills. It
  also saves the contract specs and the MetaTrader calendar, then
  disconnects. The trader and the bridge keep running.
- **Runs the research offline**, in parallel (`--workers`; default: all
  CPUs but one). It keeps a cache, so a stopped run picks up where it was.
- **Writes** `data/rider-research/<date>/report.md`, `summary.json` and
  `trades.csv`. With `--upload` it also publishes them to the reports
  branch under `reports/rider-research/<date>/`. The GitHub token comes
  from `secrets.json`. If another upload gets in the way, it reads the
  file again and retries, up to five times.

Options:

- `--symbols`: `auto` (8 liquid pairs: EURUSD, GBPUSD, USDJPY, AUDUSD,
  USDCAD, USDCHF, EURJPY, GBPJPY), `all`, or a list.
- `--variants`: `default`, `all`, `base`, or a list of IDs.
- `--no-fetch`: re-analyse the ticks already stored.
- `--chunk-minutes`: how many minutes of ticks to ask for at once
  (default 60). An hour that times out is tried again in 15-minute pieces.
- `--synthetic`: run everything on made-up prices.
- `--probe`: only check that past ticks can be read (below).

**Past ticks that do not come.** On 9 Oct the research on the Mac
(`reports/rider-research/2026-10-09`) asked for 3,456 hour chunks of 10
past weekdays for 8 pairs, and got ONE tick in total, with no error. Two
things are now handled:

- **An answer for the wrong hour.** MetaTrader under Wine read the times it
  was given as UK local time, so each answer was for the hour before the
  one asked, and those ticks were dropped as outside the hour. The broker
  adapter now gives MetaTrader the broker's clock as whole seconds. The
  tick store also checks every answer against the hour it asked for. If
  the answer is for another time, it measures how far off it is, asks
  every later hour that much later, and says so in a note.
- **An empty answer.** MetaTrader often answers nothing for a past period
  it has not loaded from the server yet. An empty hour in the busy hours
  (01:00-20:00 UTC on a weekday) is never stored as final. MetaTrader is
  asked to load it (the market selected, its one-minute bars read), and
  the hour is asked again after 1, 2, 4 and 8 seconds. If it is still
  empty, nothing is stored for it, so the next run asks again; the rest of
  that day gets one quick retry per hour. If three market-days in a row
  give no tick at all, the fetch stops and says to run the probe. An empty
  hour outside the busy hours, or on a weekend, is stored as it came.

**The probe:**

```
python -m mintel.rider.research --config data/config.json --probe
```

It fetches 10:00-11:00 UTC of EURUSD on the last complete weekday (and the
last complete hour, to compare) exactly as the research would. It prints
how many ticks MetaTrader returned and how many were kept, with the first
and last times, then a plain verdict. It stores nothing. The exit code is
0 when past ticks can be read and 5 when they cannot.

**How long it takes:** the replay cost is the bot's own scan, once a second
for every market. With the defaults (10 days, 8 markets, 6 variants), the
estimate was roughly one to two hours on the Mac, plus the first fetch.
The guard pair adds one more replay (RMR-GUARDS-ON has the same settings as
RMR-BASE with the shipped defaults, so it is replayed once and reported
under both), so expect a little longer. This is an estimate scaled from
synthetic runs, not a measurement on real ticks.

### What it does

1. **Coverage.** It counts ticks per market per day and finds the holes. A
   market-day counts if it has at least 5,000 ticks and no hole longer than
   30 minutes between 07:00 and 20:00 UTC. With fewer than 5 usable days,
   the verdict is **INSUFFICIENT DATA**, and no result is claimed.
2. **Run labels.** A moment is the start of a SUCCESSFUL MOMENTUM RUN if
   buying at the ask (or selling at the bid) reaches +X pips before -Y
   pips within H. Five settings are used: 5/2.5 in 2 minutes, 10/5 in 5,
   20/8 in 10, 30/12 in 15, and 50/20 in 30. The spread is paid inside the
   label. When both levels are hit in the same second, it counts as a
   failure.
3. **Event study.** The evidence families are measured 30 seconds before
   each run, 10 seconds before, and at its start. The code is the same as
   the bot's, fed only the ticks up to that moment. These runs are compared
   with random ordinary moments, using effect sizes (Cohen's d and AUC).
   Outcomes are measured 10 s, 30 s, 1 min and 5 min later. The start of a
   run is, in hindsight, the bottom of a small dip, so the study also asks
   the bot's real question: of the fast bursts it could see, what did the
   ones that became runs have? It also measures **latency**: does a run
   still work if entered 250 ms, 500 ms, 1 s or 2 s late?
4. **Replay.** RiderCore is driven tick by tick, in the live order: feed
   ticks, manage with FlowLock X, scan once a second, then enter. The
   rules:
   - An entry fills at the first tick after the order delay, at the ask or
     bid, plus slippage.
   - Every tick is checked against the stop. A gap fills at the gapped
     price, never at the stop.
   - A stop move counts only once the broker would have accepted it.
   - Commission is charged per lot per side.
   - Nothing is seen before it happens. The test suite proves this by
     changing the future and checking the past does not change.
   - The calendar never shows a figure before it was released.
5. **Cost stress.** The same entry signals are replayed with 1.5x and 2x
   spread and slippage, and with a 1 s and 2 s delay. If 1.5x turns a
   profit into a loss, the result is flagged **FRAGILE**.
6. **Walk-forward.** It chooses only from a small grid: trigger score 55,
   60 or 65, and giveback curve loose, balanced or tight. The choice uses
   in-sample days only. A setting's result is averaged with its
   neighbours', so a lone lucky setting is not picked. One day is purged
   between in-sample and out-of-sample. Only the out-of-sample days are
   reported as the result.
7. **Research registry.** Every variant run is stored in
   `data/rider-research/registry.sqlite`, with its data source (REAL or
   SYNTHETIC). Each row holds: METHOD_ID, DESCRIPTION, HYPOTHESIS,
   MARKETS, PERIOD, TRADES, WIN RATE, EXPECTANCY, PROFIT FACTOR, DRAWDOWN,
   MFE CAPTURE, COST SENSITIVITY and OUT-OF-SAMPLE RESULT. The variants
   tested are:
   - RMR-BASE, the headline: the core as built, fixed before any research.
   - Trigger score 55 and trigger score 65.
   - The loose and tight giveback curves.
   - The anti-chop gate switched off.
   - The 9 Oct guards forced on (RMR-GUARDS-ON) and off (RMR-GUARDS-OFF),
     everything else as the baseline.
   - Optionally (`--variants all`), the steep and flat curves.
8. **Report.** In plain English, it covers:
   - trades tested and trades per day;
   - win rate, net pips after costs and net money at GBP 1/pip;
   - profit factor, average win, average loss and maximum drawdown;
   - MFE capture;
   - the cost stress and out-of-sample results;
   - the data coverage, the event study and latency;
   - a verdict: INSUFFICIENT DATA, NO EDGE, FRAGILE, NOT CONFIRMED
     OUT-OF-SAMPLE, UNPROVEN, or PASSED THIS TEST. Even PASSED is not
     proof; the demo account is the next test.

### Research limits, said plainly

- **Days are separate:** each day is replayed with a fresh bot, seeded
  with the previous days' bars. A trade open at midnight is followed to
  its end, but the next day does not know about it. The loss guards also
  start each replayed day empty (live, a loss just before midnight UTC
  still counts after it).
- **Learning:** the bot's own learning (2 of 100 points) stays neutral.
- **Weekends:** a trade still open on Friday night is closed at Friday's
  last price. It is not held through the weekend gap.
- **Commission:** cost stress multiplies spread and slippage, not
  commission.
- **Fills:** real fills, especially around news, can be worse than
  modelled.
