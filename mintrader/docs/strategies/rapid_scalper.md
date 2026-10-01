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
