# Crowd Fader (the fifth bot) - positioning from Coinversa

**When the losing crowd is piled up one way on a market and the proven
winners are not with them, lean the other way - once the price has started
to turn - with the stop beyond the last hour and the nearest liquidation
cluster as the target.**

Started 8 October 2026 in PAPER, where it never places an order. Switched
to LIVE on the demo the same day (see "Going LIVE" below): it now places its
own orders under its own magic number, with the stop and the target at the
broker. Its results stay on its own tab and are never added to the account
total. It needs a Coinversa API key saved on the Mac (see "Setting it up");
without one it shows NO KEY and does nothing.

## Where the data comes from

Coinversa Pulse indexes every wallet on Hyperliquid, a crypto exchange
whose "xyz" market also lists perpetual contracts on gold, silver, the
S&P 500, the Nikkei, EUR, GBP, WTI and Brent. Every wallet is classed by
its lifetime profitability: Apex and Sharps (the proven winners), Grinders
and Scrapers (in between), The Crowd, Bleeders, Trapped and Blown Out (the
losing crowd). For each market the bot reads, every ten minutes:

- who is long and short in each class, by wallet count and by money;
- the liquidation map: how much long money is forced out at each price
  below, and how much short money at each price above, within 3%.

The same backend the Coinversa connector uses: `https://api.coinversa.ai`,
the key in an `X-API-Key` header, on the Pro plan 600 calls a minute and
20,000 a day. Eight markets, two calls each, every ten minutes is about
2,300 calls a day.

## Why it might work, honestly stated

Retail positioning as a contrarian signal is the best-documented sentiment
edge there is: IG's client-sentiment series and the old DailyFX "SSI"
showed the retail crowd net wrong at extremes for years, and the
Commitments of Traders index is the same idea on the futures market (fade
the stretched speculators, side with the hedgers). Liquidation clusters are
the crypto-native stop run: a dense pool of forced closes just beyond the
price is fuel for a move into it.

The caveat: Hyperliquid's gold or S&P book is a small sample of the real
market (hundreds of millions, not hundreds of billions), so this is a
sentiment read, not order flow. IG's series is a small book too and still
worked as a contrarian read. None of it is proof on this account, which is
why the bot is in PAPER and records every read it makes.

## The rules, as built (`mintel/crowd/`)

| | |
|---|---|
| Markets | XAUUSD = xyz:GOLD, XAGUSD = xyz:SILVER, US500 = xyz:SP500, JP225 = xyz:JP225, EURUSD = xyz:EUR, GBPUSD = xyz:GBP, XTIUSD = xyz:CL, XBRUSD = xyz:BRENTOIL (any the broker lacks is skipped) |
| The read | every 10 minutes, on its own thread: crowd net bias by wallet count (The Crowd + Bleeders + Trapped + Blown Out); winners' net bias by money (Apex + Sharps); liquidation fuel within 3% each side. Needs at least 200 crowd wallets |
| Lean | crowd net bias at least 0.5 (three in four one way) AND the winners not with them (their money bias with the crowd at most +0.1) -> lean the other way |
| Score | 0-100: 50 for how stretched the crowd is, 30 for how far the winners lean against them, 20 for how much liquidation fuel sits on the fade side. Needs 60 |
| Trigger | the price must turn first: a closed 15-minute bar beyond the previous eight closes in the lean's direction. One entry per read |
| Stop | beyond the last four 15-minute bars (an hour) plus the spread; at least 0.15% away; over 1.5% away is skipped |
| Target | the heaviest liquidation cluster on the fade side, carried over as a percentage move, if it sits 1-4 R away; else 2 R |
| Out | the stop (PAPER: counted AT the stop; LIVE: the broker's stop), the target (PAPER: AT the target; LIVE: the broker's take-profit), 24 hours, or Friday 20:30 UTC (both a market close in LIVE) |
| Size | 10 at the stop; when the smallest size the broker allows risks more than 10 it is taken anyway up to 30, else skipped (gold with a 20-point stop risks 20 on 0.01 lots). The risk taken is recorded on every trade |
| Hours | entries 07:00-20:00 UTC, Monday to Friday; positions managed at all hours |
| Limits | at most 2 entries a day per market |
| Costs | PAPER: fills at the touch plus one point; a buy marked on the bid, a sell on the ask; no commission counted (indices and metals carry none here; FX would). LIVE: whatever the broker charged, inside its own figure |
| Records | `data/crowd.sqlite` (`trades`: one row per trade, `ticket` is the broker's ticket in LIVE, `mode` says which; and `checks`: every read), `data/crowd-status.json`; its own tab and panel on the page; `reports/<date>/crowd.json` nightly |
| Settings | `"crowd": {...}` in `config.json` (all optional: `enabled`, `mode` (`PAPER`, `LIVE` or `OFF`), `magic` (990711), `markets`, `poll_minutes`, `stretch`, `agree_cap`, `min_score`, `risk_money`, `max_risk_money`, `hold_hours`, ...) |
| Code | `mintel/crowd/`, hooked in from `Trader.manage` and never able to raise into it; orders go through the shared executor `mintel/botexec.py` |

## Going LIVE (8 October 2026, demo account)

Aaron asked for it to trade for real on the demo on the day it was built,
ahead of the two weeks of paper the plan below asked for. What changes, and
what does not:

- **Its own orders, magic 990711, comment `CROWD`.** Every order carries the
  Crowd Fader's own magic number, and the bot only ever reads positions with
  that number, so it can never see or touch Trend & Breakout's (990311), the
  scalper's, the Runner's or the Band Breaker's positions, and they can never
  touch its.
- **The stop and the target both sit at the broker, or there is no trade.**
  The order is sent with the stop and the target on it. An order whose stop
  the broker did not take is closed again at once and counted as refused.
  The broker's stop and take-profit do the closing; the bot itself closes
  at the market after 24 hours, on Friday at 20:30 UTC, or when the price
  is through the target and the broker's take-profit has not fired (a
  backstop). A close the broker refuses is kept and tried again on the
  next pass.
- **The broker's figure is the record.** Every LIVE row's `net_pnl` is the
  broker's own profit + commission + swap for that position, read from its
  history once the position is gone. The exit reason is the broker's own
  word for the close: its stop (`STOP`), its take-profit (`TARGET`), one of
  the bot's own closes finalised late after a restart (`TIME`, `WEEKEND`,
  `TARGET`), a close by hand (`MANUAL` or `BROKER_CLOSED`, never a
  profitable "stop"); only when the broker gives no word at all is it the
  level the exit price is nearer to.
- **One empty read is not a close.** The MT5 adapter answers a terminal
  hiccup with an empty list, so a position missing from one read is kept
  and checked again ("not in the broker's list and no closing deal yet;
  kept and checked again"); nothing is closed on the market while it is
  unseen, and no second position can open there. Only after three reads in
  a row over at least a minute with no closing deal is the row booked at
  the last price with the reason marked `?` (for example `STOP?`) so the
  review can see it was estimated. If the position then turns out to be at
  the broker after all, the sweep below closes it and its row takes the
  broker's figure.
- **A `?` is corrected.** Every LIVE row booked with a `?` (the history had
  not caught up when the position closed) is re-read from the broker at
  start and once a minute for a day; as soon as its deal is there the row
  takes the broker's exit price, figure and time and loses the `?`, and the
  log says so ("now holds the broker's record"). The status counts the rows
  still waiting (`estimated_rows`).
- **The shared account gate.** No new entry while Trend & Breakout's daily
  loss stop, a drawdown or exposure breaker, or safe mode holds; the page
  says "entries paused by the account gate". Managing an open position is
  never gated.
- **A refused order is a clean no.** Nothing is recorded, the note says
  "order refused: ..." and that read is marked as acted on so the broker is
  not hammered; the next read ten minutes later gets a fresh go. The day's
  refusals are counted on the status. An order whose outcome is unknown (it
  may have filled, then the read-back of the broker's list failed) is
  treated the same way - "order sent, state unknown" - and the broker's
  list is swept at once, so a position that did fill is closed and booked
  rather than left running with no row of its own. A fill whose ticket
  number is already on record (a PAPER row from before) is closed again at
  once and counted as refused; the old row is never touched.
- **Restarts fail closed, and the sweep runs on.** On start the bot checks
  the broker: a row the broker closed while the bot was down is finalised
  from the broker's record; a position carrying the Crowd Fader's magic
  that is not on record is closed at once ("orphan closed"), never adopted,
  and if a row already carries that ticket (an earlier `?` estimate) the
  row takes the broker's figure. The same sweep runs once a minute while
  LIVE, so a stray position is never left until the next restart; a read
  that fails sweeps nothing and is tried again. A second open row on a
  market that already has one is settled the same way. A row opened in the
  other mode is left alone and listed on the page as unmanaged.
- **PAPER is unchanged.** In PAPER nothing is sent: fills at the touch plus
  one point, the stop closes AT the stop and the target AT the target, a
  buy marked on the bid and a sell on the ask. `OFF` has no executor at all
  and does not even poll Coinversa.
- **Switching back:** `python -m mintel.ops.modes --config ~/MarketBot/data/config.json --paper crowd`
  (or set `"mode": "PAPER"` under `"crowd"` in `config.json` and restart).
  A LIVE position open at the moment of the switch keeps its stop and
  target at the broker and is left alone by the paper run, which says so
  on the page.

## Setting it up

1. Get an API key at coinversa.ai/developers (it starts with `cvsa_`).
2. On the Mac, in Terminal:

       cd ~/MarketBot/app && ~/MarketBot/venv/bin/python -m mintel.crowd --config ~/MarketBot/data/config.json --set-key

   It asks for the key without showing it and saves it in `secrets.json`
   next to the password and the GitHub token. The installer keeps it across
   reinstalls. Never put the key in `config.json` or in the code.
3. Check it reads:

       cd ~/MarketBot/app && ~/MarketBot/venv/bin/python -m mintel.crowd --config ~/MarketBot/data/config.json --probe

   prints gold's read in plain English and how many calls are left.
4. Restart the bot (the Start icon or a reinstall). The panel on the page
   shows each market's read; `--show` prints the latest reads from the records.

## Across the other bots

Nothing in the other bots changes on this. The reads are kept for every
market every ten minutes, so the nightly review can look up what the crowd
and the winners were doing when Trend & Breakout, the Runner or the Band
Breaker took a trade (`mintel.crowd.engine.context_from_db`) and tally
trades taken with the winners against those taken against them. If that
tally is one-sided after enough days, it becomes a rule by the standing
process; it is not one now.

## What would earn it real money

The plan was two weeks of PAPER with a positive result, losses no worse
than the planned risk, the reads and the trades matching, and no day lost
to a bug. Aaron chose to run it on the demo at once; the same two-week
test now runs on the broker's own figures.

## Daily record

| Day | Trades | Wins | Net (the broker's figure from the switch to LIVE) | By market | Notes |
|---|---|---|---|---|---|
| 8 Oct | - | - | - | - | built; waiting for the key on the Mac. The first live read (07:08 UTC, from the connector): gold crowd 82% long over 2,324 wallets, Apex and Sharps 22% short by money, 4.3m of longs liquidate within 3% below against 1.2m of shorts above - a lean SHORT at 71/100, waiting for the price to turn |
