# Always watching: what happens when MetaTrader goes quiet

Written after 24 September 2026, when the bot stopped at 10:32 UK and
nobody knew until the evening.

## What went wrong that day
1. MetaTrader's pipe stopped answering (`mt5.initialize failed: -10003,
   IPC initialize failed`). The terminal was still running, so nothing
   restarted it.
2. The bridge dialled MetaTrader *before* opening its port, so while it
   waited a minute per attempt the port was dead.
3. The trader treated "cannot reach the bridge" as fatal and exited.
   The watchdog restarted it, it exited again, and after twelve rounds the
   watchdog gave up: "NEEDS A HUMAN".
4. The watchdog's own probe of the bridge carried no token, so every check
   left "rejected a request with a bad token" in the bridge log, which
   hid the real fault.

## What is different now
- **The trader waits.** If MetaTrader or the bridge is not answering it
  tries again every 15 seconds for as long as it takes, heartbeating so the
  watchdog leaves it alone. The page shows "WAITING FOR METATRADER" with
  the reason and the attempt count.
- **The bridge opens its port first** and dials MetaTrader in the
  background, again every 30 seconds until it answers. Its ping now says
  whether MetaTrader is connected and what the last error was.
- **A deaf terminal is restarted.** If the bridge reports MetaTrader not
  connected for ten minutes while the terminal is running, the watchdog
  closes the terminal and starts it fresh (at most 12 times an hour).
- **The probe carries the token**, so the bridge log only says "bad token"
  when something really is wrong.
- **A pulse every ten minutes**, independent of the bot (`com.mintel.pulse`
  launchd job): one line in `data/uptime.log`, UP with the account's
  equity, every real open trade on the account and the account's result
  for the UK day (from 00:00 UK, the page's Today; not the broker's day,
  which starts at 22:00 UK) as `today`, the page's Account card figure.
  While Trend & Breakout is on PAPER its practice result for the same UK
  day follows, labelled `tnb_practice`, never added in. DOWN comes with
  the reason it could work out (trader heartbeat age, watchdog state,
  bridge answering or not, MetaTrader connected or not, terminal running
  or not). Published to the `reports` branch as
  `reports/live/pulse.json`, `reports/live/status.json`,
  `reports/live/uptime.log` and `reports/live/bots.json` (see below).
- **The nightly report counts the gaps.** `reports/<date>/uptime.log` and a
  BOT UPTIME section in the day review: percentage up, each outage with
  start, end, length and reason. The nightly review message reports it.

## Why has the Rider not traded? (9 Oct)
The Rapid Momentum Rider keeps a "why no trade" record in its status file
(`data/rider-status.json`, the `why_no_trade` block), covering the last hour
and brought up to date after every scan. For each market it counts how many
scans it spent in each state (WATCHING, BUILDING, READY, TRIGGER, or ENTERED
while it holds a trade there), the best score it reached, which way and
when, how old its latest price is and how many prices a minute are coming
in. Whenever a market scored at or over the trigger score, or was READY or
TRIGGER, and no trade was opened, the record names the first rule that held
it back - too choppy, costs too big for the expected move (with the cost
ratio and the expected move in pips), too little room to the next level,
news, spread, too few price updates, the price not confirming the move, the
cooldown or the fresh-trigger rule, one position per market or the cap on
open positions, a failed health check, the mode, or the order step itself
(no contract details or price, the trade could not be sized, the price
already through the planned stop, the broker refusing the order, or an
order sent with no answer back, which the next pass checks and which may
yet show as open) - and counts it, per rule and per market. The last ten near misses are kept in full with their
numbers (a run of back-to-back scans held back the same way counts as one,
with how many scans it lasted), and two lines say when the last trade was
opened and when the last trigger came and whether it was taken. The
`summary` line at the top says it all in one sentence. The record only reads
what the scan and the entry check already decided and never changes a
decision: a test runs the same synthetic prices with and without it and gets
the same orders, stop moves and exits. The same status file's TODAY block
(the Rider's own page) now counts from 00:00 UK, like the status page, so a
trade from last night after 22:00 UK is no longer in today's figures.

## Every bot, every ten minutes: reports/live/bots.json
Each ten-minute pulse also publishes `reports/live/bots.json` on the reports
branch, next to `pulse.json`, so the bots can be read from GitHub without
asking Aaron. It has one entry per bot: its mode (as it runs, and as its
settings say), its health line, today's trades and result copied from the
status page's standing block (MetaTrader's own figures, real money, for
the page's Today: the UK day from 00:00 UK; a paper bot's practice figure
is shown apart and never added in), and its open trades. The Rapid Momentum Rider's
entry carries its "why no trade" block and the top ten rows of its scanner
(market, direction, score, state, reason, cost ratio, headroom); Financial
Ian's says what its data feed is doing and why; Trend & Breakout's lists its
open paper trades (from its paper record, read-only) and its last few
"thinking" rows; the Momentum Runner's lists its open rides. Every status
file is only read: a missing, broken or stale one gives a plain "not running
/ not readable" entry instead of stopping the pulse, and if the trader's page
did not answer, the money figures say "not known" rather than guess. The
file is kept under 60 KB (the least needed detail is cut first), never holds
the account login, a key or a token, and goes up with the same upload as the
rest of the pulse, which retries when GitHub reports a clash with another
writer.

## If it is down anyway
Read `reports/live/pulse.json` on the reports branch: the `reason` says
what it could see. The one-shot recovery is Stop, quit MetaTrader, Start:

    bash ~/MarketBot/app/deploy/mac/"Start Trading Bot.command" stop; pkill -f terminal64.exe; sleep 5; bash ~/MarketBot/app/deploy/mac/"Start Trading Bot.command"

## Weekend clock alarm (26 Sep)
"This machine's clock is 180s out of step with the broker" appeared on a
Saturday with new trades paused. The offset between the broker's clock and
UTC was measured from the last price tick, and on a weekend that tick is
Friday's. Now: the offset snaps to the half-hour grid brokers use; a stale
tick is never allowed to move a trusted offset; on a weekend start the
offset is read from the Friday close and confirmed on Monday's first live
tick; and the clock check judges only on a live tick. With the market
closed or quiet it reports "not comparable right now" and blocks nothing.
A tick from the future, or a tick minutes old while the market is open,
is still a real clock fault and still pauses entries.

## Sunday 27 Sep: the bridge restart loop
MetaTrader's pipe went deaf again (IPC initialize failed from 13:29 UK).
Each dial into it froze the bridge for the whole pipe timeout, the
bridge's five-deep connection queue filled, its port then refused, and the
watchdog restarted it every 90 seconds - straight into another frozen
dial. The deaf-terminal restart never fired because it needed a ping
answer the frozen bridge could not give. Now: the connection queue is
128 deep; a bridge whose port accepts but whose ping goes unanswered for
ten minutes counts as a deaf terminal, and both the terminal and the
bridge are restarted; the bridge backs off 30 s to 5 min between dials;
and a stale tick can no longer pose as live (offset must be within 14 h
and the FX market open).


## Checking a day against the broker, per bot

    cd ~/MarketBot/app && ~/MarketBot/venv/bin/python -m mintel.ops.reconcile --config ~/MarketBot/data/config.json --day 2026-10-02

Prints MetaTrader's own record for that day split by magic number, one
line per bot: Trend & Breakout (990311), the Rapid Momentum Rider (990811),
the Momentum Runner (990511), the Band Breaker (990611), the Crowd Fader
(990711), Financial Ian (990911) and the retired Rapid Scalper (990411),
then anything that carried none of them (a hand trade), each with
positions, price result, commission and net, then the account total. The
account line must match the bottom of MetaTrader's History tab for the same
day. Only real trades are in it: Trend & Breakout's PAPER trades never
reach MetaTrader, so its line is only a real trade left open from LIVE.
Add `--positions` to list every position. Read-only.
