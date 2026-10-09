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
| Use | First a few days of **history** to test on (next section), then live streaming |

## Test on past data first

Aaron's decision: lower risk first. Before paying for the live CME feed,
Financial Ian is tested on **real past CME order-book data** bought from
Databento's historical service, which is priced per request (usage-based).
The tool **always shows Databento's exact price first and buys nothing
without a "y" typed at the keyboard.**

### On the Mac: one icon

Double-click **TEST-FINANCIAL-IAN** - inside the downloaded folder
(`deploy/mac`), or on the Desktop. It needs the bot installed (Start
Trading Bot once) and then:

1. installs the `databento` package into the bot's own Python if missing;
2. asks once for the Databento API key if none is saved (hidden input,
   kept in `data/secrets.json`, never printed). Saving the key does **not**
   switch Ian's live feed on - `ian.json` is not touched;
3. prints what Databento has, the test period and contracts, and the price:

       Databento has CME Globex (GLBX.MDP3) data from <first day> to <last day and time> UTC.
       Test period: 5 weekday(s), Fri 02 Oct to Thu 08 Oct 2026, London and New York hours only (07:00-21:00 UTC).
       Contracts: 6AZ6 6BZ6 6CZ6 6EZ6 6JZ6 6NZ6 6SZ6 (the front month each day by Ian's own roll rule: ...)
       Asking Databento what this costs (asking is free: nothing is downloaded or charged yet) ...
         Fri 02 Oct 2026   mbp-10   ...  $...   trades   ...  $...
         ...
       This test will download about <size> of CME order-book data for 5 days and Databento will charge about $<quote>.
         (That is Databento's own quote for exactly this request ... Its API does not report account credit,
         so none is assumed ...)
         A cheaper first look: --days 2 (the last 2 of these days) would cost about $<quote>.
       Go ahead? (y/N)

   (`<...>` is filled in with Databento's own figures at the time; with
   `--start`/`--end` the cheaper look is offered as the last two of those
   days, `--start ... --end ...`);
4. on "y" downloads day by day with progress, replays the data through
   Ian's own engine, writes the report, uploads it and prints the verdict:

       ======================================================================
         VERDICT: <one of the four below>
           - <why, in plain words>
         <n> trades, net <pips> pips / GBP <money> after costs (REAL PAST CME DATA)
         Ian's parameters were never tuned on this data, so the whole period is out-of-sample; ...
       ======================================================================
       Report: ~/MarketBot/data/ian-research/<today>/report.md

It never restarts, stops or changes the running bots and runs at the
lowest priority (`nice -n 19`). The replay is slow - Ian decides every
half second for 7 futures - roughly 30 to 60 minutes per day of data; it
shows progress and the time left. On the Windows VPS the same is
**TEST-FINANCIAL-IAN.cmd** (Idle priority).

When it stops, the last line says what happened to the money. "Nothing
was bought." appears only when nothing was bought in that run (exit code
10, which nothing else gives). Anything else - stopped with Ctrl+C after
saying yes, a download that failed, a crash during the replay - says
"Anything already downloaded was paid for and stays on disk"; running it
again carries on without buying that again.

### From a terminal

    cd ~/MarketBot/app
    ~/MarketBot/venv/bin/python -m mintel.ian.research.historical --config ~/MarketBot/data/config.json

| Option | Meaning |
|---|---|
| `--days N` | the last N complete weekdays (default 5). `--days 2` is the cheap first look |
| `--start YYYY-MM-DD --end YYYY-MM-DD` | exact days (weekdays only); dates outside Databento's range are refused |
| `--hours london_ny` / `all` | 07:00-21:00 UTC (default) or the whole day |
| `--schema mbp-10` | the order book, ten levels each side (the only one the test uses) |
| `--no-trades` | do not buy the trades schema; the trades inside mbp-10 are used instead (a little cheaper) |
| `--max-cost USD` | the most Databento may charge; over it, it stops and says so (it must be a plain number: `nan` or `inf` is refused) |
| `--yes` | buy without asking - **only with `--max-cost`** at or above the quote |
| `--max-gb GB` | size budget (default 25 GB, Databento's billable size; a plain number above 0); free disk is checked too |
| `--offline` | never contact Databento: test what is already on disk |
| `--download-only` | buy and download (after the price question), do not test |
| `--upload` | publish the report to the reports branch (`reports/ian-research/<date>/`) |
| `--set-key` | enter the Databento key (hidden) and save it |
| `--latency-ms`, `--slippage-pips`, `--commission-usd`, `--spread PAIR=PIPS` | the cost model (defaults 250 ms, 0.1 pip, USD 3.50 a lot a side, typical IC Markets Raw spreads) |

### What is bought, and kept

- Dataset `GLBX.MDP3`, schemas `mbp-10` and `trades`, requested per day
  with `stype_in=raw_symbol` for the contract Ian's live code would have
  traded that day (its own front-month rule, rolling 8 days before the last
  trading day - not Databento's continuous symbols, so the replay sees
  exactly the contracts live Ian subscribes to).
- Saved compressed as `data/ian-research/history/<YYYY-MM-DD>/<schema>.dbn.zst`
  with a `manifest.json` (each file's hours and contracts, the quote, the
  download time; never the key). A file gets its final name only once
  complete, so **what is on disk is never downloaded or paid for again**:
  asking for more days buys only the new ones, and asking for more of a day
  already there - `--hours all` after London and New York, or futures added
  to `ian.json` - quotes and buys only the missing hours or contracts, as
  extra files beside the first (`mbp-10.2.dbn.zst` ...). The price question
  says which part of a day is already on disk. A test only uses a day that
  has every contract for the whole window (`--offline` says what a day
  lacks), and only the contracts that day's plan names.
- The mbp-10 schema already carries every trade; the trades file is used
  for them and the copy inside mbp-10 is not counted a second time - the
  same rule as the live feed with both subscriptions (below), so the test
  replays exactly the stream live Ian will get.

### How it tests

- The records go through the **live adapter's own** `RecordMapper`, so a
  past record becomes exactly the event a live one would, then into the
  **real `IanEngine`**: book, features, regime, score, the MT5 bridge's
  checks, the order-flow trail and the data-quality gate, a decision every
  `decision_interval_s` of event time, Ian's own settings from `ian.json`
  (forced to PAPER). Nothing is tuned: the whole period is out-of-sample.
- A simulated executor stands in for MetaTrader: spot prices are the futures
  prices mapped with Ian's own mapping (1/x for 6J, 6C and 6S), plus the
  pair's typical spread (wider when the futures spread widens); a market
  order fills 250 ms after the decision plus slippage; a stop fills at the
  stop **or worse** the moment any quote touches it; commission USD 3.50 a
  lot a side; size from Ian's own `risk_money`, money in GBP.
- No look-ahead: a decision sees only data stamped before it. The same files
  always give the same trades.
- Ian decides only inside the test hours. When a test window ends, a trade
  still open is closed at the last price of its own contract (never a later
  quote, never the next contract's after a roll) and nothing new is opened
  until the next window starts.

### Reading the verdict

| Verdict | When |
|---|---|
| INSUFFICIENT DATA | fewer than 30 trades - run more days |
| NO EDGE | not positive after costs |
| FRAGILE | positive, but costs 1.5 times higher turn it into a loss, or one instrument made all the profit |
| PROMISING - WORTH A LIVE FEED TRIAL | positive after costs, not fragile, at least 30 trades, more than one instrument contributing |

The report (`report.md`, `summary.json`, `trades.csv` in
`data/ian-research/<date>/`) also gives the trades per day, win rate, profit
factor, average win and loss, drawdown, MFE capture and the expectancy in R
(both after every cost, like the pounds), results by instrument,
regime and hour, the cost stress at 1.5x and 2x, the data-quality time
(LIVE / DEGRADED / DOWN), and a list of what the test cannot show (the
forward points, typical not measured spreads, the chart rebuilt from futures
prices, the calendar only where the bot's own record covers the days,
trades closed when a test window ends). Data from anything but a recorded
Databento download is labelled **SYNTHETIC - NOT PERFORMANCE**.

### Before saying yes

- The Databento account needs historical access to `GLBX.MDP3` and a
  payment method (or credit). Databento's API does not report account
  credit, so the tool never assumes any.
- A bad key, a refused payment or a dataset the account may not use are said
  in plain words; nothing is charged for a refused request.
- This is the same key the live feed would use later; it is kept in
  `data/secrets.json` only.

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
   mbp-10 carries every trade as well; with both subscribed each trade is
   taken from `trades` only and counted once (the past-data test does the
   same, so its result applies to this stream).
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
