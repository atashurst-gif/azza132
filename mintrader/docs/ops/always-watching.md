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
  for the broker's day (`today`, the page's Account card figure; while
  Trend & Breakout is on PAPER its practice result follows, labelled
  `tnb_practice`, never added in), or DOWN with the reason it could work
  out (trader heartbeat age, watchdog state, bridge answering or not,
  MetaTrader connected or not, terminal running or not). Published to the
  `reports` branch as
  `reports/live/pulse.json`, `reports/live/status.json` and
  `reports/live/uptime.log`.
- **The nightly report counts the gaps.** `reports/<date>/uptime.log` and a
  BOT UPTIME section in the day review: percentage up, each outage with
  start, end, length and reason. The nightly review message reports it.

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
