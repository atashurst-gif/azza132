# FINANCIAL IAN - the brief (from Aaron, 8 Oct 2026, condensed faithfully)

A completely separate trading system. Do not alter, merge with or interfere
with the other bots. Its intelligence comes from the PRICE LADDER, DEPTH OF
MARKET, EXECUTED ORDER FLOW and LIQUIDITY - institutional-quality data - to
understand what happens INSIDE the market before a candle moves.

## Market reality
Spot FX has no single global order book; liquidity is fragmented. The first
institutional source is a genuine central limit order book: CME FX FUTURES
(Euro FX -> EURUSD, British Pound -> GBPUSD, Australian Dollar -> AUDUSD,
Canadian Dollar -> USDCAD INVERTED, Japanese Yen -> USDJPY INVERTED, Swiss Franc
-> USDCHF INVERTED, ...). Futures quote USD per unit of foreign currency, so
JPY/CAD/CHF directions invert against the spot pairs. Normalise correctly; an
inversion error trades exactly backwards - unit tests compulsory.
DO NOT FAKE DEPTH. If MT5/the retail broker has no meaningful depth, do not
manufacture it or treat five retail levels as the institutional book. Keep
RETAIL MT5 DATA and CENTRALISED FUTURES BOOK DATA clearly separate. The
institutional feed drives the order-flow intelligence.

## Architecture
Separate DATA, INTELLIGENCE and EXECUTION layers.
MODE A: futures order flow -> signal -> translated to spot direction -> MT5
order (only if the spot market confirms). MODE B (later): direct futures
execution through a futures broker - keep it possible (no tight MT5 coupling).
Feed adapter interface; never hard-code one vendor (CME-compatible vendors,
futures brokers, professional APIs, historical tick/book providers); the
strategy consumes one normalised internal order-book format.

## The core question: what are buyers and sellers actually doing at each price?
Price ladder: per level bid/ask price and size, size changes, orders appearing
and disappearing, executions, trade volume, imbalance, distance from mid, time
at level, price reaction. Several levels, not just top of book.
Book imbalance: weighted by distance, persistence, change, executions, price
reaction, historical usefulness - never "bids > offers = buy".
Order-flow imbalance: aggressive buys lifting offers vs sells hitting bids,
executed volumes, intensity, frequency, acceleration, large prints, clusters,
whether aggression moves price (more important than resting orders).
ABSORPTION_SCORE: heavy aggression that fails to move price (passive side
absorbing); test continuation vs reversal afterwards.
Liquidity sweeps: executions consuming several levels fast (real sweep vs
cancellations). Liquidity pulling: walls vanishing ahead of price. Walls: size
vs normal, distance, persistence, refresh, executions, rejection or consumption
(a wall repeatedly hit and finally consumed can be bullish). Refresh /
iceberg-like behaviour: label POSSIBLE REFRESH / ABSORPTION, never claim
certainty. Order-book velocity: adds, cancels, executions per second, depth
change, imbalance change, level turnover, spread change. Microprice / book
pressure (test its predictive value). Queue dynamics only as far as the feed
allows (no invented queue positions). Volume profile (high/low volume nodes,
value area, acceptance). Session VWAP (distance, slope, acceptance) as context.
Cumulative delta where aggressor classification is reliable (direction,
acceleration, divergence, extremes, failure to move price). Footprint-style
machine features (bid/ask imbalance at price, stacked imbalance, delta by
level, absorption, clusters, initiative flow) - every signal deterministic and
mathematically defined. Rate of liquidity consumption vs replenishment. Book
resilience after consumption (regenerates/snaps back vs genuine repricing).
Regimes: BALANCED, TRENDING, BREAKOUT, HIGH VOLATILITY, LOW LIQUIDITY, NEWS
SHOCK, ABSORPTION, LIQUIDITY VACUUM, REVERSAL - different tactics per regime.
Charts still matter (tick, 1m, 5m, 15m, 30m, 1h, structure): the book says
WHAT is happening inside the move, the chart says WHERE. News matters,
especially what the book does AFTER the release (news shock mode: spreads,
depth, cancellations, intensity, recovery; never chase the first print).
Cross-market institutional confirmation (other CME FX futures, Treasury/
equity-index/gold futures where data exists) - evidence, not a requirement.

## Score, entry, exit
FINANCIAL IAN OPPORTUNITY SCORE per instrument from: order flow, book
imbalance, liquidity consumption, absorption, sweeps, book velocity, delta,
microprice, volume profile, VWAP, structure, momentum, volatility, news,
cross-market, execution quality, historical performance. Watch all supported
markets (no tiny whitelist). Enter when several independent pieces of evidence
say one side has a meaningful advantage - EARLY, not after twenty
confirmations. Exit with an intelligent trailing engine: no tiny fixed target;
ride strong flow; protect more as aggression weakens, opposite absorption
appears, liquidity regenerates against the trade or imbalance reverses; exit
when the thesis dies. The stop NEVER widens. A real broker-side stop on every
position.

## MT5 bridge and safety
Signal object: instrument, direction, confidence, entry reasoning, intended
stop, strategy ID, timestamp. MT5 side validates tradability, quote freshness,
spread, places the order with its protective stop, reports execution back.
Idempotent trade IDs: a communication failure must never duplicate a trade.
Explicit futures<->spot symbol mapping with tested direction inversions.
Latency recorded at every hop (data received, signal, order request, MT5
receipt, broker acceptance, fill); learn which opportunities survive retail
execution. Not HFT: look for phenomena that persist long enough for our path.
NEVER spoofing, layering, wash trading, quote stuffing or fake liquidity: every
order is genuine trading intent. Observing others' orders is fine.

## Data quality, recording, learning, testing
Monitor feed connection, sequence gaps, stale book/trades, timestamp problems,
out-of-order events, missing depth, latency, disconnects; if unreliable: a safe
DATA-DEGRADED state (no signals) while recovering automatically. A recorder of
book and trade events and a deterministic replay of a real session as if live.
Learning: store the full book state before every trade (levels, imbalances,
delta, intensity, liquidity changes, sweeps, absorption, VWAP, profile, news,
chart context, entry, MFE, MAE, exit, result); learn which patterns were
profitable AFTER costs. Test every feature for predictive value (horizon,
after costs, time, market, regime) and remove what has none. Benchmark:
price-only, chart/indicator, order-flow-only, chart+flow, chart+flow+news, full.
Report trades, per day, win rate, net after costs, expectancy, PF, avg win/loss,
drawdown, MFE, MAE, MFE capture, by market/signal/regime. Never invent results.

## Operations and pages
Health supervisor (feed, MT5, broker, book freshness, trade-event freshness,
heartbeats, database, CPU, memory, disk, internet, reconciliation; restart what
can be restarted). Windows VPS (the Mac is only a screen). ONE installer
INSTALL-FINANCIAL-IAN (dependencies, folders, config, services, adapters, MT5
bridge, database, dashboard, watchdog, startup tasks, health validation; asks
for any data subscription credential inside that one process) and ONE start
START-FINANCIAL-IAN (no duplicate processes; "FINANCIAL IAN - HEALTHY").
Dedicated page: running, institutional feed, MT5, top opportunity, open
positions, today's profit, win rate, last trade, the reasoning in simple
English, and a readable visual ladder (asks, price, bids, executed volume,
important liquidity, imbalance, where pressure is). Every trade explained in
ordinary English. If a market-data subscription genuinely needs Aaron: finish
everything else first, then give exact instructions.
