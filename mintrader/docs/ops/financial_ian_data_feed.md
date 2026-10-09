# Financial Ian - connecting the institutional order-book feed

Financial Ian is built and tested, but it needs **one thing only Aaron can
do**: a subscription to a CME futures order-book feed. Until then it runs in
a safe **DATA-DEGRADED** state - its page says
"DATA-DEGRADED: no institutional feed is configured - no signals, no
trades" - and nothing else is affected.

MetaTrader's own depth is retail broker data, not the centralised futures
book, and is never used in its place.

## What is needed

| | |
|---|---|
| Exchange | CME Globex (MDP 3.0) |
| Products | 6E Euro FX, 6B British Pound, 6A Australian Dollar, 6N New Zealand Dollar, 6C Canadian Dollar, 6J Japanese Yen, 6S Swiss Franc. Optional context: ES (E-mini S&P 500), ZN (10-Year Note), GC (Gold) |
| Contracts | The front quarterly contract of each (for example 6EZ6 in October 2026). The bot works out the contract itself and rolls eight days before the last trading day. |
| Depth | At least 10 price levels each side (market-by-price, "MBP-10"), or full order-by-order ("MBO") |
| Trades | Every execution with the aggressor side |
| Use | Live streaming; history for research is a later step |

## Option 1 (built in): Databento

The adapter for Databento is already written and tested
(`mintel/ian/feeds/databento.py`). The steps:

1. **Open an account** at databento.com (the portal). Check the current
   prices for live CME data on Databento's own pricing page before
   subscribing - this document does not quote prices because they change.
   CME also charges its own exchange fees for live data and asks every
   user to accept its market-data licence and to say whether they are a
   professional or non-professional user; the Databento portal walks
   through this. Answer it truthfully.
2. **Subscribe to live data for the dataset `GLBX.MDP3`** (CME Globex
   MDP 3.0). The schemas Financial Ian uses:
   - `mbp-10` - the top ten price levels each side (the default), and
   - `trades` - every execution with its aggressor side.
   `mbo` (every individual order) also works and gives the most detail,
   at a higher data volume; set `"schema": "mbo"` to use it.
3. **Create an API key** in the portal (API keys start with `db-`).
4. **Paste the key** - one of these, never anywhere else, never in source
   code and never in `ian.json`:
   - the **INSTALL-FINANCIAL-IAN** step asks for it and saves it; or
   - on the VPS run
     `python -m mintel.ian --config <data folder>\config.json --set-key`
     and paste it when asked (it is not shown on screen); or
   - add it by hand to `data\secrets.json`:
     `"databento_api_key": "db-..."` (keep the other keys in that file).
5. **Install the Databento package** in the Python the bot uses:
   `pip install databento`.
   The VPS must be able to reach Databento's live gateway
   (`glbx-mdp3.lsg.databento.com`, TCP port 13000 - the defaults in the
   `databento` package, version 0.87.0) through any firewall.
6. **Turn the feed on** in `data\ian.json`:

       {
         "mode": "PAPER",
         "feed": {"vendor": "databento", "schema": "mbp-10", "trades": true}
       }

7. **Check it**:
   `python -m mintel.ian --config <data folder>\config.json --check-feed`
   should print the contracts it subscribed to and how many events
   arrived in ten seconds. If it says NOT CONFIGURED it also says what is
   missing (the package or the key); DOWN comes with the vendor's reason
   (for example a refused key or a missing subscription).
8. **Restart Financial Ian** (START-FINANCIAL-IAN, or
   `python -m mintel.ian.run --config <data folder>\config.json`). The page
   should show the institutional feed **LIVE** and, after a few minutes of
   warm-up, the price ladder and the top opportunity.

### After it is connected

- Leave it in **PAPER** for at least several sessions. Every event is
  recorded in `data\ian\recordings\YYYY-MM-DD\<contract>.jsonl.gz`.
- Run the feature test on the recordings:
  `python -m mintel.ian.research.featuretest --replay data\ian\recordings`
  and the benchmark. These are the first real measurements; anything the
  bot showed before was synthetic test data.
- Only then consider `"mode": "LIVE"` (a deliberate edit of
  `data\ian.json`).
- Recordings take disk space: check the size of the first day's folder
  and prune old days if the disk needs it.

## Option 2: a futures broker's API

Financial Ian does not depend on one vendor. A futures broker's market-data
API works the same way once an adapter is written for it - one small
module in `mintel/ian/feeds/` with four methods (connect, subscribe,
events, health) that turns the broker's messages into the internal format
of `mintel/ian/book.py`. Candidates:

- **Rithmic** (R | Protocol API) - CME depth and trades, used by many
  futures brokers.
- **CQG** (CQG API / WebAPI) - CME depth and trades.
- A futures broker that offers CME depth through its own API (for
  example Interactive Brokers' TWS API with a CME futures market-data
  subscription).

Each needs its own account and its own exchange-fee declarations; check
the current costs with the provider. A futures broker would also make
MODE B possible later (trading the future itself instead of the spot pair):
the execution side is a separate module (`mintel/ian/bridge.py`), so that
needs one more executor, not a rewrite.

## What the bot does without a feed

- The process runs, keeps its heartbeat ("ian") and writes its page file.
- The page shows the feed as NOT CONFIGURED with the reason.
- No signals are produced and no orders are ever sent.
- MetaTrader and every other bot are untouched.

## Settings reference (`data\ian.json`)

| Key | Default | Meaning |
|---|---|---|
| `mode` | `PAPER` | OFF / PAPER / LIVE |
| `feed.vendor` | `none` | `none`, `databento`, `replay` (recordings), `synthetic` (tests only - never trades) |
| `feed.schema` | `mbp-10` | `mbp-10` or `mbo` (Databento) |
| `feed.trades` | `true` | also subscribe to the trades schema |
| `feed.path` / `feed.speed` | recordings folder / `asap` | for `replay`: a folder or file; `asap`, `realtime` or a speed factor |
| `instruments` | 6E 6B 6A 6N 6C 6J 6S | the futures traded through their spot pairs |
| `context` | ES GC ZN | watched as evidence only |
| `risk_money` | 10 | money at the stop per trade |
| `max_lots` | 0.5 | the most per trade |
| `max_open` | 3 | positions at once |
| `max_daily_loss_money` | 50 | this bot only: no new entries for the rest of the day after this loss |
| `record` | true | record every event for replay |
