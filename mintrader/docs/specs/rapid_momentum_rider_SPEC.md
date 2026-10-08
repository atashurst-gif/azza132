# RAPID MOMENTUM RIDER - the brief (from Aaron, 8 Oct 2026, condensed faithfully)

A brand-new STANDALONE bot that REPLACES the Rapid Scalper. Reuse only proven
infrastructure (MT5 connectivity, market data, order execution, health checks,
logging, results pages, VPS deployment). Do not inherit restrictive logic from
the other bots. New strategy from first principles.

## The idea
Find a market that has started moving hard, get on early, ride it while it is
strong, get out very quickly when the move dies. Not a prediction bot. It asks
"which market is running right now?" and "is this a real directional move or
noise?". Wrong: cut fast. Right: no tiny fixed take-profit - ride it.

Desired distribution: many small controlled failed attempts + normal profitable
scalps + occasional very large rides. Never many tiny winners then one huge
loss. Optimise: small losses, large winners, high MFE capture, fast re-entry.

## Exposure (the user's setting, never changed by the bot)
USER_PIP_VALUE_GBP = 1.00 (about GBP 1 per pip). Convert correctly to lots per
FX instrument using contract size, tick size, tick value, account currency,
quote currency, pip definition, lot step, min and max lot. Do not guess. Test
5-digit FX, 3-digit JPY FX and crosses. The bot chooses market, direction,
entry, stop and exit; Aaron chooses exposure.

## Market intelligence: evidence FAMILIES, not 30 voting indicators
Group evidence by what it represents; never double-count.
1 Volatility regime (ATR fast/slow/ratio, realised and tick volatility, range
  expansion/percentile, Bollinger and Keltner width, squeeze, Donchian range,
  candle and session range vs normal, velocity of volatility change). Volatility
  alone is not enough: want rising volatility + high directional efficiency.
2 Directional efficiency: TREND_EFFICIENCY = |net move| / total path travelled.
  A major filter.
3 Price velocity over 1s, 3s, 5s, 10s, 15s, 30s, 1m, 3m; normalised across
  instruments; velocity, acceleration, jerk.
4 Momentum measures (ROC, EMA slope and separation, MACD histogram acceleration,
  ADX/DMI, RSI behaviour, regression slope, displacement, consecutive closes) as
  measurements, never "RSI crossed 50".
5 Breakouts (recent range, Donchian, session high/low, opening range, previous
  day high/low, swing high/low, consolidation, volatility breakout) measured by
  distance through, close beyond, time beyond, follow-through, body strength,
  tick activity, retest behaviour.
6 Structure break -> FIRST clean retest -> continuation (not the 5th retest).
7 Momentum pullback: shallow, low opposing velocity/volume, fast continuation vs
  deep aggressive reversal.
8 Opening ranges 1/3/5/15/30 min around London, New York (by instrument/regime).
9 Session breakout: Asian/London/New York/previous session high-low.
10 Squeeze release: quiet -> volatility release + direction (follow, never guess).
11 Volume/participation: tick volume, relative to baseline, acceleration, trade
   frequency (spot FX tick volume is not exchange volume - use it for what it is).
12 VWAP as context (rising VWAP momentum vs price spiralling through flat VWAP).
13 EMA geometry (slope, separation, expansion, compression, distance, pullbacks),
   never a crossover bot.
14 ADX/DMI: trend strength vs directionless activity; watch CHANGE in ADX.
15 Candle quality: body/range, wicks, close location, consecutive candles, range
   expansion, relative size.
16 Microstructure from ticks: micro highs/lows, HH/HL, LH/LL, swing structure,
   tick arrival rate, bid/ask changes, spread behaviour. No fake DOM.
17 Order flow only where genuine data exists (MT5 retail FX has none: say so).
18 Currency strength for USD EUR GBP JPY CHF CAD AUD NZD from multiple pairs.
19 Cross-market confirmation: broad currency move vs pair-specific move.
20 News catalyst from the MT5 calendar (event, currency, importance, previous,
   forecast, actual, revision). News is not a trade by itself.
21 Post-news momentum: first impulse, second wave, continuation, rejection; did
   price jump, did spread recover, is momentum continuing, are other pairs
   confirming, is structure breaking. Never chase the first tick blindly.
22 Failed breakout: mainly to EXIT bad momentum trades fast.
23 Liquidity sweep / reclaim, defined mathematically.
24 Structural headroom: open space to the next swing/session level/round number.
25 ATR-normalise every distance (breakout/ATR, spread/ATR, stop/ATR, move/ATR,
   headroom/ATR, pullback/ATR).
26 Cost-to-opportunity: COST_RATIO = (spread + commission + slippage) / expected
   available move. Never trade moves whose edge the costs eat.
27 Anti-chop (low efficiency, direction flips, flat VWAP/EMAs, weak ADX, wicks,
   repeated mean crossing, breakout failures, poor follow-through) -> WAIT.

MOMENTUM_OPPORTUNITY_SCORE 0-100 per market, built from categories: volatility
expansion, directional efficiency, velocity, acceleration, price structure,
breakout quality, pullback quality, participation, multi-timeframe, session,
news, currency strength, cross-market, headroom, execution cost, historical
match. No double counting.

Watch ALL liquid FX instruments in MT5; no permanent bans (history changes
confidence only). Rank them (e.g. GBPUSD 94 SHORT, EURJPY 90 LONG). Many trades
a day are allowed; no quotas, no arbitrary per-day caps; the market decides.
FAST ENTRY: find (by replay) the earliest point where continuation is worth it.
Entry states: WATCHING -> BUILDING -> READY -> TRIGGER -> ENTERED.

## Risk and exits
Every trade gets a genuine broker-side stop immediately (structure, swing,
volatility, spread, invalidation). Failed thesis = fast exit. No martingale,
no averaging down, no grids, no size increase after a loss.

FLOWLOCK X (the key component):
- RULE 1 NEVER WIDEN (long stop only rises, short only falls). Automated tests
  with thousands of random simulated price paths prove it.
- States: INITIAL (structural stop; exit at once if the thesis fails), PROVING
  (reduce downside without strangling), COSTS COVERED (true NET breakeven incl.
  spread, commission, slippage buffer), RUNNER (strong velocity/acceleration,
  new extremes, high efficiency, shallow pullbacks, confirmation intact: give it
  room - a stronger move may deserve MORE room), PROFIT RATCHET (MFE-based
  locked floor; test several giveback curves: early winner more room, mature
  winner protect more, decelerating winner protect aggressively), SWING TRAIL
  (last confirmed micro swing), ATR/CHANDELIER, MOMENTUM DECAY (a
  MOMENTUM_DECAY_SCORE from falling velocity/acceleration, time since new
  extreme, opposing candles/wicks, efficiency/ADX deteriorating, EMA flattening,
  cross-market confirmation vanishing, tick activity slowing, failed breakout:
  as decay rises the trail tightens), EXHAUSTION (protect aggressively), THESIS
  DEAD (exit now).
- HYBRID STOP: candidates (structure, ATR, MFE floor, PSAR where useful, EMA,
  decay) - FlowLock X picks the right valid one for the state; all subject to
  NEVER WIDEN. No fixed take profit (optional emergency TP only if required).
Re-entry: momentum comes in waves; after a profitable stop, keep watching; a
fresh independent trigger may re-enter (short cooldown only against duplicates).

## Measurement and learning
Record MFE, MAE, realised pips, MFE_CAPTURE (a key metric), latency (signal ->
send -> broker ack -> fill). Every trade stores the full pre-entry market state.
Learn by market, session, volatility regime, efficiency, score, entry family,
breakout/pullback type, currency strength, news, spread, cost ratio, FlowLock
behaviour - with minimum samples, rolling performance, out-of-sample and
walk-forward validation; prefer stable parameter ranges; never present the
single luckiest of thousands of combinations.
RESEARCH REGISTRY: every tested variation recorded (METHOD_ID, DESCRIPTION,
HYPOTHESIS, MARKETS, PERIOD, TRADES, WIN RATE, EXPECTANCY, PROFIT FACTOR,
DRAWDOWN, MFE CAPTURE, COST SENSITIVITY, OUT-OF-SAMPLE RESULT). Do not deploy
them all.
Dashboard shows win rate prominently but the bot optimises NET EXPECTANCY after
costs (trades, wins, losses, win rate, net pips, money, PF, avg win/loss, max
drawdown, MFE, MAE, MFE capture).

## Pages
Results page, extremely simple: TODAY - trades, won, lost, win rate, net pips,
net money, best trade; then each trade explained like to a child. Scanner page:
MARKET, DIRECTION, SCORE, REASON (e.g. "EURUSD NONE 43 - moving quickly but too
choppy").

## Research (the most important question)
"WHAT CONDITIONS EXIST IMMEDIATELY BEFORE A LARGE CLEAN SHORT-TERM PRICE RUN?"
Use REAL TICKS (every tick, bid/ask, spread, commission, slippage, execution
delay). If tick history is insufficient, say so; never fake precision.
EVENT STUDY: automatically find strong directional moves (5/10/20/30/50+ pips
without excessive adverse excursion); for each examine 30s before, 10s before,
the entry moment, 10s/30s/1m/5m after; what changed before the run? TARGET
LABEL: SUCCESSFUL MOMENTUM RUN if price reaches a configurable favourable
distance before a configurable adverse distance; several horizons.
COST STRESS: normal, 1.5x and 2x spread/slippage; flag fragile results.
LATENCY: determine whether opportunities survive retail execution speed.

## Operations
Health checks: MT5 alive, broker connected, algo trading enabled, fresh ticks,
symbols available, spread data valid, execution working, positions reconciled,
FlowLock alive, database writable, disk/memory - recover automatically.
Broker-side catastrophic SL on every trade; FlowLock moves it; a crash never
leaves a naked position. Runs on a Windows VPS (the Mac is only a screen).
SETUP-RAPID-RIDER (first install) and START-RAPID-RIDER (normal use): one action
verifies/starts MT5, bot, watchdog, database, dashboard.

## Final philosophy
A short-term momentum detector and rider: genuine volatile directional move
beginning? No: keep scanning. Yes: get aboard. Wrong: out fast. Right: let it
run, raise an irreversible profit floor, tighten as it weakens, bank it when
momentum dies, hunt the next one. Never invent performance; if the backtest
fails, say so, analyse why, improve via the registry, retest.
