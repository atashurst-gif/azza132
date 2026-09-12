# Market Intelligence Trader

An autonomous, opportunity-seeking trading system for MetaTrader 5.

It is not a strategy bot. It is a market-intelligence engine: it reads price,
charts, news, economic data, market structure, momentum, volatility, session
behaviour, currency strength and cross-market confirmation across every
instrument the broker offers, decides **what kind of market each one is**, picks
the tactic that fits that market, ranks every opportunity on one scale, and
trades the strongest one available — sized between the two risk numbers you
control.

```
ALWAYS WATCHING.  ALWAYS THINKING.  ALWAYS READY.
TRADE HARD WHEN THE EVIDENCE IS STRONG.
CONTROL THE DAMAGE WHEN WRONG.  PRESS THE ADVANTAGE WHEN RIGHT.
```

---

## Install and run

### On a MacBook — one icon

Double-click **`deploy/mac/Start Trading Bot.command`** in the folder you
downloaded. It installs everything, asks for your account once, starts
trading, and puts a permanent **Start Trading Bot** icon on your Desktop —
after which you can delete the download. Every double-click after that starts
it if it is stopped and reports healthy if it is not. It restarts itself if it
ever crashes and starts again when you log in.

MetaTrader 5's Python interface is Windows-only, so on a Mac MT5 and one small
Windows Python run inside a self-contained Wine folder while the entire
intelligence layer runs as native macOS Python and talks to them over a
localhost socket. Full detail, including exactly what happens when you close
the lid, is in **[deploy/mac/README.md](deploy/mac/README.md)**.

Want to see it work before involving a broker? Double-click
**`Self Test.command`** — it runs the real bridge, the real trader and the real
dashboard against a live-tracking simulation, with no account and nothing at
risk.

### On a Windows VPS — one command

For trading that continues with your laptop shut and in your bag. In
PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\SETUP-AND-START.ps1
```

That single command installs Python, creates the environment, installs the
packages, asks you once for your account details and risk settings, finds or
installs MetaTrader 5, copies the MQL5 safety net, registers the tasks that
restart everything after a reboot, starts the watchdog, the trader and the
dashboard, verifies every dependency, runs a safe smoke test, and prints a
status block.

Afterwards, normal use is also one command:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\START-BOT.ps1
```

It is safe to run as often as you like. If everything is already running it
reports that it is healthy and starts nothing — it will never start a second
trader, because two traders on one account would double every trade.

Either way, open **http://127.0.0.1:8787** for status and
**http://127.0.0.1:8787/results** for what happened and why.

---

## How it decides

### 1. What kind of market is this?

Every instrument is classified before any tactic is allowed to compete:

| Regime | What it means |
|---|---|
| `TREND` | directional structure with efficient movement |
| `RANGE` | two-sided, contained, mean-reverting |
| `SQUEEZE` | volatility compressed below its own history, coiled |
| `HIGH_VOL` | volatility in the top decile, expansion under way |
| `LOW_VOL` | volatility in the bottom decile without compression |
| `NEWS` | inside a live reaction window to a scheduled release |
| `UNCLEAR` | the honest answer when the data does not support a label |

### 2. Which tactic fits it?

Tactics only compete in the regimes where they belong, so the bot cannot
mean-revert into a trend day.

| Tactic | Regimes |
|---|---|
| Momentum continuation | trend, high vol, news |
| Trend pullback | trend, low vol |
| Breakout acceptance / retest | trend, squeeze, high vol, unclear |
| Session (opening-range) expansion | trend, high vol, squeeze, unclear |
| Squeeze release | squeeze, low vol |
| Range rejection | range, low vol |
| Failed breakout / reclaim | range, high vol, trend, unclear |
| Liquidity sweep reversal | range, trend, high vol, unclear |
| VWAP reversion | range, low vol |
| News continuation | news, high vol, trend |

### 3. Evidence, not an indicator vote

Measurements are grouped into **families**, and a family contributes exactly
one score no matter how many indicators feed it. EMA slope, EMA separation and
rate of change all live inside `MOMENTUM` and cannot vote three times. Weights
belong to families, and they change with the regime.

```
STRUCTURE  MOMENTUM  VOLATILITY  PARTICIPATION  SESSION  MULTI_TF
CROSS_MARKET  NEWS  EXECUTION  HISTORICAL  REGIME  HEADROOM
```

Each family also carries a confidence in 0–1. Low confidence shrinks that
family toward neutral rather than dropping it, so thin evidence dilutes instead
of distorting.

### 4. Reality checks before the score is trusted

The blended evidence is then multiplied by things that can only make a trade
worse (except generous headroom, which earns a small bonus):

- **Structural headroom** — what will price collide with? A textbook long with
  daily resistance three pips overhead is a bad trade.
- **Exhaustion** — how much of a typical day's range is already spent. This is
  a discount, never a veto: a genuine trend day routinely prints 2–3× ATR and
  refusing to trade it would be the expensive mistake.
- **Execution quality** — spread now versus its own history, tick freshness,
  expected slippage, the broker's minimum stop distance. If costs destroy the
  edge, there is no trade.
- **Session liquidity** — a catalyst in thin Asian hours is still tradable,
  just with a wider execution allowance.

### 5. Every market competes

```
1  GBPUSD  SHORT  93   TREND / HIGH VOL   momentum continuation
2  XAUUSD  SHORT  89   TREND / NORMAL     breakout retest
3  USDJPY  LONG   84   SQUEEZE            squeeze release
```

The ranking changes with every scan. There is no whitelist, no approved-market
list, no requirement that an instrument "earn the right" to be traded, no
one-trade-per-day rule and no minimum holding period. Instruments are
discovered from the broker's own symbol list; the only reason one is excluded
is that its contract specification is incomplete, in which case the reason is
recorded and shown.

---

## How news is used

**News provides information. Price provides the verdict.**

1. The MT5 economic calendar is read for every currency involved, converted
   from trade-server time to UTC, and archived to SQLite.
2. A surprise is measured against forecast, against previous, against revisions
   — and normalised by **how much that particular series normally surprises**.
   Without enough history the magnitude is capped, so a poorly-understood
   release cannot look like a five-sigma shock.
3. Direction is looked up per event family. A CPI beat is currency-positive; an
   unemployment-rate beat is currency-negative; crude inventories are
   ambiguous and produce no hypothesis at all. "Better number = buy" is
   refused.
4. Then the engine **watches what the market actually did**: the size of the
   jump, spread behaviour, tick participation, persistence, whether it was
   given back, and whether other markets agree.
5. If dollar-bullish data lands and the dollar is sold across the board, the
   bot follows the selling and records the textbook reading as contradicted.

Wave model: `PENDING` (no new entries — execution is unpriceable) →
`FIRST_SPIKE` (chaos, wide spreads) → `FIRST_WAVE` (held, spreads normalised) →
`SECOND_WAVE` (slower real participation, often the better entry) — or
`REJECTED` / `WHIPSAW`, where a fully rejected impulse becomes tradable in the
*other* direction.

External news sources are modular, opt-in, tiered (official releases beat
wires beat aggregators; social sources are off unless explicitly enabled and
can never fire a trade alone), cached, and non-fatal. If a provider freezes,
the bot keeps trading on charts plus the stored calendar and says on the
dashboard that it is degraded.

**Backtests read only the archive.** A run whose window is not covered by
stored calendar data reports `news_aware: false` and says why. It will never
present a "news-aware" backtest with no news in it.

---

## How charts are used

Price action first, indicators second.

- Higher highs / higher lows / lower highs / lower lows, break of structure
  confirmed by a **close** beyond a swing that was already confirmed at that
  time, reclaim, sweep-then-reversal, compression, expansion.
- A wick outside a level is explicitly **not** a breakout. Confirmation needs a
  body close beyond the level by a meaningful fraction of ATR, with
  participation. A poke-and-reclaim is reported as a *failed* breakout, which
  is what feeds the reversal tactics.
- Session ranges (Asia / London / New York / US cash) and opening ranges at 1,
  3, 5, 15 and 30 minutes, with break, acceptance and reclaim tracked
  separately. All windows come from real exchange timezones, so daylight-saving
  transitions are handled by the tz database rather than guessed — including
  the three weeks each spring when London and New York are four hours apart
  instead of five.
- Timeframes have roles rather than being required to agree: D1/H4/H1 for
  context, M30/M15 for intraday structure, M5/M1 for immediate behaviour, and
  sub-minute frames (synthesised from ticks where available) for entry timing
  only.
- Momentum is measured as *displacement versus path*: 20 pips in a straight
  line scores very differently from 80 pips of thrashing that ends 20 pips up.
  That efficiency measure is the most load-bearing number in the engine.

---

## How aggressive sizing works

You own two numbers:

```
base_risk_pct        risk at NORMAL confidence
max_risk_pct         hard ceiling, never exceeded by anything
```

Risk scales between them according to **calibrated** confidence — what that
score band has *actually delivered* in the trade journal, not what the raw
score claims. A 97/100 in a band with no history sizes near base risk; the same
score in a band with sixty observed trades and positive expectancy is allowed
to reach the maximum.

The aggression setting moves you along that curve. It never raises the ceiling
and it never disables a circuit breaker.

| Setting | Effect |
|---|---|
| `CONSERVATIVE` | pulls base risk back toward your minimum, climbs slowly |
| `NORMAL` | base risk at normal confidence |
| `AGGRESSIVE` | reaches the ceiling on genuinely strong evidence |
| `MAXIMUM` | reaches the ceiling on merely strong evidence |

Sizing accounts for entry, stop distance, tick value, tick size, contract size,
the broker's volume grid (floored, never rounded up), free margin, the
**projected margin level after the fill**, existing positions and correlated
exposure — long EURUSD and short USDJPY are the same dollar bet and share one
exposure bucket.

**Never martingale.** No doubling after losses, no averaging into a loser, no
recovery grids, no revenge sizing. The sizing function cannot even see past
outcomes: its only inputs are the current opportunity, calibrated confidence,
and a large-sample fractional-Kelly cap that can only ever *reduce* risk.

---

## How FlowLock works

A stateful trailing engine whose job is to cut failures fast, let exceptional
trades run, and lock profit as a move dies.

| State | Behaviour |
|---|---|
| `INITIAL` | the trade has not proven itself; the structural stop stands |
| `PROVING` | evidence improving; the stop begins to follow |
| `STRONG_FLOW` | momentum exceptional — the stop is deliberately kept **wide** |
| `NORMAL_FLOW` | working but ordinary; protect progressively |
| `DECAY` | no new extremes; tighten hard |
| `REVERSAL` | the thesis has broken; close now, do not wait for the stop |

It reads MFE, MAE, ATR, realised volatility, how often new extremes print,
momentum, acceleration, pullback depth, structure, swing points, EMA slope,
wick behaviour, participation, cross-market confirmation, currency strength,
news changes, time since the last extension, and thesis strength.

**The stop never widens.** For a long it may only stay or rise; for a short only
stay or fall. That invariant is enforced in FlowLock and re-checked
independently in the executor, so even a bug upstream cannot loosen a stop.

Partial profits are configurable and tested rather than assumed: a portion is
banked once the trade has materially paid for its risk and the remainder runs —
but only if the evidence says that improves expectancy. Break-even protection
waits until the trade has earned it, because moving to break-even early turns
winners into scratches.

Open positions are managed on a fast clock (~1s) while the full universe scan
runs on a slower one (~5s), so a stop is never blind between scans.

### Continuous position thesis

Every open position carries `THESIS_STRENGTH`, recomputed from live structure,
momentum, multi-timeframe agreement, currency strength, cross-market
confirmation, news and execution conditions, and compared against what was true
at entry. Improving → the winner gets room. Fading → the stop tightens. Broken →
it closes.

---

## How health checks work

The system is built around one distinction: **market risk versus software
stupidity.** Losing money on a judged trade is acceptable. Losing money because
MT5 quietly died, the feed went stale, a stop vanished or a process hung is an
engineering failure.

Checked continuously: MT5 process, MT5 connection, broker connection, the
**correct account and server**, demo/live agreement with the configuration,
trade permission, tick freshness, Market Watch symbols, chart history, news
feed, economic calendar, database, disk space, memory, **machine clock versus
broker clock**, heartbeats, open-position reconciliation, unprotected
positions, stuck requests, broker return codes, spread anomalies, margin level
and daily P&L.

**Heartbeats, and who judges whom.** Every component writes an atomic heartbeat
file. A process is never allowed to certify its own health — a deadlocked
process still holds its PID and still looks alive from the inside. So the
independent **watchdog process** judges the trader, and the trader judges the
watchdog. Neither vouches for itself.

**Self-healing.** Recoverable faults are fixed automatically and rate-limited so
a genuinely broken environment produces a clear alert instead of an infinite
restart loop: MT5 disconnects → reconnect with backoff, then **reconcile**
because local state can no longer be trusted; the database drops → reconnect;
the news worker fails → restart it; a process hangs → kill it *before* starting
a replacement, so two traders never fight over one account.

**SAFE MODE.** When something critical is wrong the bot does not trade blind. It
keeps recovering, keeps existing positions protected at the broker, keeps
reconciling, keeps looking — and resumes automatically once healthy. "Always
trading" means always operating and looking; it never means sending market
orders while the data is stale.

**Circuit breakers** (all configurable): risk per trade, total open risk,
correlated exposure, daily loss, drawdown, rejected-order storms, abnormal
spread, order rate, minimum margin level. A breaker stops new entries and never
stops managing existing risk — the management path does not even consult the
entry gate.

**Order safety.** Every intent gets a deterministic idempotency key, recorded
*before* the order is sent and written into the order comment so the broker's
own records can be searched during recovery. Return codes are checked by name;
API success is never assumed to be execution success. After any restart the
**broker is the source of truth**: live positions are read, adopted, and any
position found without a stop gets one immediately.

---

## How it keeps running

### On the MacBook

A LaunchAgent starts the watchdog when you log in and restarts it if it ever
stops. The watchdog then owns everything else: it starts the trader, starts the
bridge, restarts MetaTrader 5 under Wine if the terminal dies, and kills a hung
process *before* starting a replacement so two traders can never fight over one
account. `caffeinate` stops the Mac dozing off while it trades.

The honest limit: **closing the lid sleeps the Mac and pauses the bot.** No
software can prevent that on battery. Your open positions keep their stop
losses, because those are held by the broker rather than by this program, and
on waking the bot detects the suspension, discards its stale view, re-reads the
calendar and reconciles against the broker before trading again. For lid-closed
trading you need clamshell mode — mains power plus an external monitor and
keyboard — or the VPS below.

### On the Windows VPS, with your Mac closed

Your Mac is then only a viewing device. Nothing about trading depends on it.

Everything runs on the Windows VPS: MetaTrader 5, the trader, the news engine,
the SQLite state, the watchdog, the logs and the dashboard. Closing your laptop
changes nothing.

Two scheduled tasks are registered at setup, both running as `SYSTEM` at
highest privilege:

- **`MintelTrader`** — fires at boot and restarts on failure, so a Windows
  update or a power event brings everything back without you.
- **`MintelKeepAlive`** — re-checks every five minutes, which covers the case
  where the boot task ran before MT5 was ready.

After a reboot the order is: watchdog starts → MT5 starts → the trader starts →
health validation runs → **positions are reconciled against the broker** → and
only then does trading resume.

Inside the terminal, `MintelGuardian.mq5` is a deliberately tiny MQL5 safety
net that makes no trading decisions. If a position somehow has no stop, it
places an emergency ATR stop from inside MT5 within seconds; it also enforces a
hard daily loss cut-out and writes its own heartbeat. Being dependency-free, it
also works under MetaTrader Virtual Hosting — but the full intelligence layer
needs external Python processes, so a proper Windows VPS is the recommended
production architecture rather than Virtual Hosting.

To view from your Mac, either use Remote Desktop, or tunnel the dashboard:

```bash
ssh -L 8787:127.0.0.1:8787 you@your-vps      # then open http://127.0.0.1:8787
```

The dashboard binds to localhost only. It is deliberately not exposed to the
internet.

---

## Layout

```
mintel/
  clock.py            timezone, DST, trade-server offset
  config.py           settings, risk bounds, the demo/live gate
  contracts.py        symbol specs, pip/tick/money math
  broker/             base protocol, MT5 adapter, deterministic simulator,
                      and the bridge (wire format, server, client) that lets
                      the engine run natively on macOS
  data/               structure, momentum, sessions, headroom,
                      currency strength, cross-market, regime
  news/               calendar store, adapters, surprise, the news engine
  engine/             evidence, tactics, scanner, opportunity, risk,
                      execution, FlowLock, thesis, journal, learning, trader
  ops/                health, watchdog, dashboard
  research/           replay broker, backtest, validation, trial ledger
  run.py              the trading process
  verify.py           prove every dependency works
  status.py           command-line status
deploy/               Windows: SETUP-AND-START.ps1, START-BOT.ps1, STOP-BOT.ps1
  mac/                macOS: the desktop icons, the installer, the self test
  mql5/               broker-side guardian EA
tests/                unit, integration and chaos tests
tools/validate.py     performance validation and aggression analysis
```

---

## Development and testing

Everything is testable on any operating system with no MetaTrader installation,
because the engine talks to a `Broker` protocol with two implementations: the
real MT5 adapter, and a deterministic simulator with a fault-injection API.

```bash
pip install -r requirements-dev.txt
python -m pytest                    # the whole suite
python -m pytest -m "not slow"      # quick
python tools/validate.py            # mechanics check on generated data
python tools/validate.py --csv EURUSD=eurusd_m1.csv --calendar-csv cal.csv
```

The chaos tests deliberately break things: kill the trader, kill MT5, sever the
connection, serve stale ticks, blow the spread out, corrupt the cache, corrupt
the config, force rejections, restart mid-position, hang a process, and
destroy the database.

### On backtest honesty

- Every reported figure is **after** spread, slippage and commission.
- Generated data is labelled `SYNTHETIC` and the report states plainly that it
  verifies mechanics only. A price process we invented cannot tell us whether
  an edge exists.
- Every configuration tried is written to a **trial ledger**, so the deflated
  Sharpe ratio and the probability of backtest overfitting are computed against
  the real number of attempts rather than against one.
- Validation offers walk-forward folds, purged-and-embargoed k-fold, CPCV,
  PBO, deflated and probabilistic Sharpe, Monte Carlo (permutation for path
  risk, bootstrap for outcome risk), cost stress and parameter-stability
  checks that distinguish a plateau from a fitted spike.
- The backtester runs the **live Trader** over a replay broker. There is no
  separate backtest strategy in this codebase, and a test asserts there never
  will be.

---

## Demo and live

Default is DEMO. Going live needs **two** independent things: `mode` set to
`LIVE` *and* the exact marker string `I-UNDERSTAND-THIS-TRADES-REAL-MONEY`
stored in the configuration. A corrupt config, a missing marker or a typo can
only ever fall back to DEMO, never forward to LIVE. On top of that, the health
supervisor refuses to trade when the configured mode and the connected
account disagree, and the dashboard shows LIVE in red.

---

## What this does not do

- No spoofing, layering, wash trading, quote stuffing or any other manipulative
  practice. Paul Rotter's *market-reading* lessons inform legitimate concepts —
  adaptation, fast reaction, short-term volatility, opening activity,
  discipline, strict stopping limits — not tactics that would be prohibited
  today.
- No fabricated order-book depth. Retail MT5 CFD feeds do not provide genuine
  market depth, so `depth_available` is reported as `False` and legitimate
  tick-volume and spread proxies are used instead.
- No promises. Nobody can tell you this will make money, and this document will
  not pretend otherwise. What it can tell you is that the machinery has been
  built to take market risk deliberately and to refuse to lose money to
  software failure.
