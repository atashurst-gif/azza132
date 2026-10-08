# Crowd Fader (the fifth bot) - positioning from Coinversa, PAPER

**When the losing crowd is piled up one way on a market and the proven
winners are not with them, lean the other way - once the price has started
to turn - with the stop beyond the last hour and the nearest liquidation
cluster as the target.**

Started 8 October 2026 in PAPER. It never places an order and its results
are never added to the account. It needs a Coinversa API key saved on the
Mac (see "Setting it up"); without one it shows NO KEY and does nothing.

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
| Out | the stop (counted AT the stop), the target (AT the target), 24 hours, or Friday 20:30 UTC |
| Size | 10 at the stop; when the smallest size the broker allows risks more than 10 it is taken anyway up to 30, else skipped (gold with a 20-point stop risks 20 on 0.01 lots). The risk taken is recorded on every trade |
| Hours | entries 07:00-20:00 UTC, Monday to Friday; positions managed at all hours |
| Limits | at most 2 entries a day per market |
| Costs | fills at the touch plus one point; a buy marked on the bid, a sell on the ask; no commission counted (indices and metals carry none here; FX would) |
| Records | `data/crowd.sqlite` (`trades`, and `checks`: every read), `data/crowd-status.json`; its own tab and panel on the page; `reports/<date>/crowd.json` nightly |
| Settings | `"crowd": {...}` in `config.json` (all optional: `enabled`, `markets`, `poll_minutes`, `stretch`, `agree_cap`, `min_score`, `risk_money`, `max_risk_money`, `hold_hours`, ...) |

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

Two weeks of PAPER with a positive result, losses no worse than the planned
risk, the reads and the trades matching, and no day lost to a bug. Then it
is Aaron's call.

## Daily record

| Day | Trades | Wins | Net (paper) | By market | Notes |
|---|---|---|---|---|---|
| 8 Oct | - | - | - | - | built; waiting for the key on the Mac. The first live read (07:08 UTC, from the connector): gold crowd 82% long over 2,324 wallets, Apex and Sharps 22% short by money, 4.3m of longs liquidate within 3% below against 1.2m of shorts above - a lean SHORT at 71/100, waiting for the price to turn |
