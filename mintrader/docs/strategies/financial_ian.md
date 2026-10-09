# Financial Ian - trading from the futures order book

**Financial Ian reads what buyers and sellers are actually doing at each
price in a real central order book - the CME FX futures book - and trades
the matching spot pair in MetaTrader 5 early, when several independent
pieces of evidence say one side has a meaningful advantage. Every position
has a broker stop from the moment it opens, and that stop only ever
tightens.**

A completely separate bot: magic number **990911**, order comment **IAN**,
its own process (`python -m mintel.ian.run`), its own records
(`data/ian.sqlite`) and its own page data (`data/ian-status.json`). It never
reads, changes or closes another bot's positions.

Built 8 October 2026. It starts in **PAPER** (it never sends an order).
**Today no institutional feed is connected**, so it runs in a safe
**DATA-DEGRADED** state: it shows a plain status, produces no signals and
places no trades. What Aaron needs to do to connect a feed is in
[docs/ops/financial_ian_data_feed.md](../ops/financial_ian_data_feed.md).

Nothing in this document is a result. No real order-book data has been
seen yet; every figure produced so far comes from synthetic test data and
is labelled **SYNTHETIC - NOT PERFORMANCE**.

## Two kinds of data, never mixed

| | Where it comes from | What it is used for |
|---|---|---|
| **INSTITUTIONAL** | The CME Globex futures order book (6E, 6B, 6A, 6N, 6C, 6J, 6S), through a data vendor such as Databento or a futures broker's API | All the order-flow intelligence: the book, the executions, absorption, sweeps, walls, the score |
| **RETAIL** | MetaTrader 5 at IC Markets: spot quotes, 1-minute / 5-minute / 1-hour bars, the economic calendar | The chart context ("where"), the spot confirmation and the execution checks only |

Spot FX has no single order book, so the retail broker's depth is not the
market's book. Financial Ian never treats MT5 depth as the institutional
book and never manufactures depth. If someone sets the feed to "mt5" it
refuses and says why.

## Futures and spot: the direction can flip

CME FX futures are priced in **US dollars per unit of the other currency**.
For the euro, pound, Australian and New Zealand dollars that is the same
way round as the spot pair, so the direction is the same. For the yen, the
Swiss franc and the Canadian dollar the spot pair is quoted the other way
round, so the direction **inverts**:

| Future | Spot pair | Future UP means |
|---|---|---|
| 6E Euro FX | EURUSD | EURUSD **up** |
| 6B British Pound | GBPUSD | GBPUSD **up** |
| 6A Australian Dollar | AUDUSD | AUDUSD **up** |
| 6N New Zealand Dollar | NZDUSD | NZDUSD **up** |
| 6C Canadian Dollar | USDCAD | USDCAD **down** |
| 6J Japanese Yen | USDJPY | USDJPY **down** |
| 6S Swiss Franc | USDCHF | USDCHF **down** |

Getting this wrong would trade exactly backwards, so every row, both ways,
has its own test (`tests/test_ian_mapping.py`). Prices convert as 1/x for
the inverted pairs (6J at 0.0066667 is USDJPY 150.0). The bot follows the
quarterly contract that carries the liquidity and rolls to the next one
eight days before the last trading day.

E-mini S&P (ES), the 10-year note (ZN) and gold (GC) are watched as
context when the feed carries them; they are never traded by this bot.

## What it looks at (all in `mintel/ian/features.py`)

Every measure has its formula written next to its code, and the same data
always gives the same numbers. Everything is compared with what is normal
for that market over the last few minutes.

- **The book, several levels deep**: size at each price, weighted by
  distance from the middle, how long the lean has lasted, and the
  microprice (where the touch sizes say the price is being pushed).
- **Who is crossing the spread**: buying that lifts offers versus selling
  that hits bids, how hard, how fast it is accelerating, and large prints.
  Aggression that moves the price matters more than resting orders.
- **Order-flow imbalance** at the top of the book (Cont, Kukanov and
  Stoikov).
- **ABSORPTION_SCORE**: heavy one-sided aggression that fails to move the
  price - the other side is soaking it up. The bot leans with the absorber,
  never with the aggressor that is being absorbed.
- **Sweeps**: one burst of executions taking several price levels, checked
  to be real executions and not cancellations.
- **Liquidity pulling**: offers (or bids) vanishing ahead of the price
  without being traded.
- **Walls**: unusually large resting size - how big, how far, how long it
  has stood, how much has traded into it, whether it refilled, and how it
  ended: consumed (a wall hit repeatedly and finally taken can be bullish),
  pulled, or rejected.
- **POSSIBLE REFRESH / ABSORPTION**: more traded at one price than was ever
  shown there. Always labelled "possible" - an iceberg is never claimed as
  a fact, and no queue positions are invented.
- **Book velocity**: adds, cancels and executions per second, depth change,
  turnover, spread change.
- **Consumption versus replenishment**, and **resilience** after a big
  consumption: does the price hold the new level, or does liquidity come
  back and the price snap back?
- **Cumulative delta** (only when the exchange says which side was the
  aggressor), its slope and divergences.
- **Footprint**: buying versus selling at each price, stacked imbalances,
  delta by level.
- **Volume profile** (value area, high and low volume nodes, acceptance)
  and **session VWAP** (distance, slope, acceptance) as context.

## Regimes and tactics (`mintel/ian/regime.py`)

BALANCED, TRENDING, BREAKOUT, HIGH VOLATILITY, LOW LIQUIDITY, NEWS SHOCK,
ABSORPTION, LIQUIDITY VACUUM and REVERSAL, each with its own tactic:

- **News shock**: never chase the first print. No entry from two minutes
  before a high-impact release until a minute after it, and none while the
  spread and depth are still disturbed.
- **Liquidity vacuum**: no new trades at all.
- **Absorption**: never trade with the side being absorbed.
- **Breakout / trend / reversal**: only with the side doing the taking.
- **Balanced / high volatility / thin book**: a higher bar.

## The FINANCIAL IAN OPPORTUNITY SCORE (`mintel/ian/score.py`)

One score per future, 0 to 100, from independent groups of evidence:
order flow, the book, liquidity consumption, absorption, context (profile
and VWAP), the spot chart (retail), the news (retail calendar) and the
other dollar futures. Execution quality (the spot spread and quote age) and
the bot's own history after costs can only scale it down or up a little;
they never invent a direction.

It enters **early**: the score over the threshold (60, plus the regime's
extra), at least **three** groups agreeing - two of them from the futures
book - and no group strongly against. Not twenty confirmations.

Every opportunity carries a sentence in plain English, for example:

> Large buyers are repeatedly taking the available EUR offers, the order
> flow at the top of the book favours buyers, and a buyer swept 6 price
> levels 3 seconds ago. Euro FX futures (6E) point to the EUR rising, so
> EURUSD BUY.

> ... Japanese Yen futures (6J) point to the JPY rising, so USDJPY SELL
> (the pair is quoted the other way round).

## From signal to order (`mintel/ian/signal.py`, `mintel/ian/bridge.py`)

The signal holds the future, the spot pair, the spot direction (already
translated), the confidence, the reasoning, the intended stop, the strategy
id and the time. Before anything is sent the MT5 bridge checks:

1. the symbol is tradable and the account allows trading;
2. the spot quote is no more than 5 seconds old;
3. the spread is at most 2.5 pips and at most three times its usual size;
4. the spot market confirms - over the last 15 seconds it has not moved
   more than a pip against the signal, nor already run half the stop
   distance with it (no chasing);
5. the stop is on the losing side and outside the broker's minimum
   distance.

The stop is the largest of: just beyond the futures structure of the last
30 seconds (converted to spot, 1/x for the inverted pairs), 1.5 times the
spot 1-minute ATR, 6 pips, and three spreads. More than 30 pips away means
no trade - the stop is never squeezed to make a trade fit.

Size: **10 at the stop** (account currency), never more than 0.5 lots; the
smallest lot only if it risks at most 20. The size never increases after a
loss. No martingale, no grids, no averaging down.

The order goes through the shared executor (`mintel/botexec.py`) with the
stop attached. In LIVE, if the broker does not hold the stop, the position
is closed at once. That close is only believed when the broker's own deal
history shows it: MetaTrader can report the stop as missing because its
positions list came back empty (it does that when the read itself fails),
and then the close can fail too while the position is still open with its
stop. In that case the order is treated as "outcome unknown" (below), so
nothing else is sent on that pair and the position is found and managed.
The broker's positions are read once more straight away: if they show the
position with no stop, its stop is placed at once, or it is closed. And
after any send whose outcome is unknown, the sweep (see "Running it") runs
on the very next pass instead of up to a minute later.

A position closed at once like this did fill, so it is a real trade: it is
booked (exit reason NO_STOP) with the broker's own figure - or an estimate
from the price at the time, marked NO_STOP?, while the broker has no
record of the close yet - so the bot's own daily loss limit, its entries
per day and today's figures count it. When the close went through but the
broker's record of it is not there yet, the order stays "outcome unknown"
with the ticket of its fill, and the sweep books it with the broker's own
figure as soon as that record shows; it is never let go unbooked. The
bot's own daily loss limit is checked again before every order, so a loss
booked this way stops the next order in the same pass.

**Never a duplicate.** Each signal's id comes from the strategy, the
future, the direction and the minute. The id is claimed in the database
before anything is sent; the same id can never send a second order - not a
duplicate, not a retry after a dropped connection, not after a restart. A
send's outcome is unknown when the connection dropped mid-send, and also
when MetaTrader answers "no connection" or "timed out": the order may still
have reached the broker, so it is never treated as refused. The broker's
positions are then searched for the order (its comment carries the id) and
it is adopted if it is there. Until that order is found and adopted, **no
new order is sent on that spot pair**, even for a new signal in a later
minute. It is taken as not there only when three reads of the broker's
positions in a row, over at least a minute, do not show it **and** the
broker's deal history since the send has no open entry of ours on that
pair. One empty read is never enough: MetaTrader returns an empty list when
the read itself fails. A read only counts while MetaTrader says it is
connected to the trade server (otherwise it is showing its own old copy,
and the count starts again), and the deal history is searched from ten
minutes before the send, because the send time comes from this computer's
clock and the deal times from the broker's. While the deal history cannot
be read, the pair stays blocked. The outcome is kept in `data/ian.sqlite`,
so a restart does not forget it.

**An open trade is only booked as closed from the broker's own closing
deal.** If MetaTrader's positions list stops showing an open trade and the
broker has no closing deal for it, the trade is kept as open: its broker
stop stays where it is, nothing moves it, and the page says so (the open
position shows "not shown in the broker's positions since ..." and that
its profit figure is from the last read that showed it, until it is seen
again). It is booked when the broker's closing deal shows how it ended,
with the broker's own figure.

A position of ours that the bot takes over (it was not managing it, for
example after an unknown send) keeps the time the broker opened it, so the
four-hour limit and the trade's length run from the fill, not from the
take-over.

**Latency** is recorded at every hop: data received, signal, order
request, MT5 receipt (when the adapter reports it - otherwise "not
measured", never guessed), broker acceptance and fill.

Every order is a genuine market order with the intent to hold the position.
Nothing is ever placed to be cancelled: no spoofing, layering, wash trading
or quote stuffing.

## Managing the trade (`mintel/ian/trail.py`)

No fixed target. The stop trails the best price by a distance that shrinks
as the order flow behind the trade fades:

| The flow | The stop trails the best price by |
|---|---|
| still with the trade | 1.0 R |
| aggression weakening | 0.7 R |
| the book turned against the trade | 0.45 R |
| the other side absorbing, or liquidity regenerating in the way | 0.3 R |
| no reliable book (data degraded) | 0.8 R, a plain trail |

After 1 R in profit the stop is at least at entry plus 0.1 R. The trade is
closed at once when the thesis dies - the flow has turned hard against it,
its flow is being absorbed, or the book now gives a strong signal the other
way. Also out after four hours, and flat for the weekend (Friday 20:30
UTC). **The stop never widens**: proven over thousands of random price and
flow paths in `tests/test_ian_trail.py`.

## When the data is not good enough

The data-quality monitor (`mintel/ian/dataquality.py`) watches sequence
gaps, a stale book or tape, timestamps out of order or from the future,
latency, missing depth, a crossed book and the feed's own error messages.
Each future is LIVE, DEGRADED or DOWN.

**The rule: a new trade needs the whole feed to be LIVE.** If any future
that has started sending data is DEGRADED or DOWN, no new trade is placed
on **any** future - not even one whose own data is clean - and the page's
headline names the futures at fault ("DATA-DEGRADED on 6N: ..."). This is
deliberate: one bad stream can be the first sign of a wider feed problem.

A future is at fault when it has: a sequence gap (or the vendor reports
one), an event out of order, a timestamp that cannot be right, data more
than 1.5 seconds late, no book update for 10 seconds (DOWN after 60), no
trade for two minutes (once it has traded), fewer than three price levels
on a side, a crossed book, or when the feed itself reports an error or
disconnects. The cost of the rule: in quiet hours a sleepy future (6N is
the usual one) can go two minutes without a trade, and that alone stops new
entries on every future until it trades again.

A future that has sent nothing at all since the start shows as "waiting";
it cannot signal, and it does not hold up the others. The context markets
(ES, ZN, GC) never hold up trading either.

Open positions keep their broker stops and are always still managed (a
plain trail where their own data is not clean). After a fault the future
stays DEGRADED until 200 clean events have arrived over at least five
seconds, while the bot asks the feed to recover (reconnect, resubscribe,
fresh snapshot) every 5, 10, 20 ... up to 120 seconds.

## Records, replay and research

- `data/ian.sqlite`: every signal (score, components, reasoning, what
  happened, latency), the dedupe store, and every trade with the full book
  state at entry, the regime, MFE, MAE, MFE capture and the exit explained
  in words. Every row says PAPER or LIVE and LIVE FEED, REPLAY or
  SYNTHETIC.
- `data/ian/recordings/YYYY-MM-DD/<future>.jsonl.gz`: every book and trade
  event as received. A recording replays exactly, giving the same signals
  and decisions (`--vendor replay`, as fast as possible or in real time).
- `python -m mintel.ian.research.featuretest --replay <recordings>`: the
  predictive value of every feature at 5 s, 30 s and 120 s, after costs,
  by regime. A feature with no edge after costs is reported as NO EVIDENCE
  - and should be removed.
- `python -m mintel.ian.research.benchmark`: price only, chart, order flow
  only, chart + flow, chart + flow + news, and the full engine, on the same
  data with the same exits and costs.

Run on the synthetic scenarios these only prove the pipeline works, and say
so (**SYNTHETIC - NOT PERFORMANCE**). The first real answers come from
recordings of the real feed.

## The page (`data/ian-status.json`)

Running, the institutional feed's state (NOT CONFIGURED / LIVE / DEGRADED /
DOWN, with the reason), MetaTrader connected, the top opportunity and its
reasoning, open positions, today's profit and win rate, the last trade, and
a small price ladder for the busiest future: per price the offers and bids
resting, what was bought and sold there in the last 30 seconds, walls,
possible refresh levels, POC / VWAP / value nodes, the imbalance and where
the pressure is.

## Running it

    python -m mintel.ian.run --config <data>/config.json     the bot (heartbeat "ian", pid data/ian.pid, logs/ian.log)
    python -m mintel.ian --config <data>/config.json --status      the last status in words
    python -m mintel.ian --config <data>/config.json --check-feed  is a feed configured, does it connect?
    python -m mintel.ian --config <data>/config.json --set-key     save the Databento key
    python -m mintel.ian --config <data>/config.json --demo sweep_up   a synthetic scenario end to end

Settings live in `data/ian.json`. The first start writes
`{"mode": "PAPER", "feed": {"vendor": "none"}}`. Modes: OFF (nothing runs),
PAPER (simulated fills on real spot quotes, never an order), LIVE (real
orders on the demo account, a deliberate edit). A synthetic or replayed
feed is never LIVE.

MetaTrader: when it is installed but cannot be reached (the terminal is
not running, or not logged in), Financial Ian waits for it, trying again
every 15 seconds. Nothing is read or placed meanwhile, and the page says
"WAITING FOR METATRADER". Started with `--no-broker`, or where the
MetaTrader5 package is not installed, it runs without MetaTrader: in LIVE
it places nothing and the page says so ("LIVE needs MetaTrader: nothing
can be placed"), still showing today's LIVE figures.

The magic number is fixed at 990911: a different `magic` in
`ian.json` is ignored with a warning in the log, because the LIVE sweep
closes any position with that magic that it has no record of.

The sweep runs once a minute in LIVE (and on the next pass after a send
whose outcome is unknown). A position with magic 990911 whose order
comment carries the id of one of this bot's own orders, and which holds
its broker stop, is taken over and managed - with one exception: if the
bot already has an open trade on that spot pair, the position is closed,
because a pair never holds two trades at once (this can happen when an
order taken as not at the broker turns up later). Any other position with
that magic (including one of ours with no stop) is closed at once. Every
position the sweep closes is booked in `data/ian.sqlite` with the broker's
own figure, so the bot's own daily loss limit counts it, and the note says
which case it was.

## Honest limits

- No real order-book data has been seen yet. Every threshold is a
  reasoned starting point, not a fitted one, and must be checked with the
  feature test on real recordings before trusting it.
- The futures book is the best single view of FX liquidity, but it is not
  the whole market; spot liquidity is fragmented across many venues.
- Retail execution is slower than the futures market. The bot looks for
  things that persist long enough for our path, and records the latency of
  every hop so it can learn which opportunities survive it.
- Exchange holidays are not modelled in the roll calendar (the roll happens
  a week before expiry, so this never changes which contract is the front).
- The installer step (INSTALL-FINANCIAL-IAN), the start script and the page
  itself are wired separately; this package provides `python -m mintel.ian
  --set-key` for the installer to call and the status file for the page.
