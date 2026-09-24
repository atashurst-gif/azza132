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
  launchd job): one line in `data/uptime.log`, UP with equity and open
  trades, or DOWN with the reason it could work out (trader heartbeat age,
  watchdog state, bridge answering or not, MetaTrader connected or not,
  terminal running or not). Published to the `reports` branch as
  `reports/live/pulse.json`, `reports/live/status.json` and
  `reports/live/uptime.log`.
- **The nightly report counts the gaps.** `reports/<date>/uptime.log` and a
  BOT UPTIME section in the day review: percentage up, each outage with
  start, end, length and reason. The nightly review message reports it.

## If it is down anyway
Read `reports/live/pulse.json` on the reports branch: the `reason` says
what it could see. The one-shot recovery is Stop, quit MetaTrader, Start:

    bash ~/MarketBot/app/deploy/mac/"Start Trading Bot.command" stop; pkill -f terminal64.exe; sleep 5; bash ~/MarketBot/app/deploy/mac/"Start Trading Bot.command"
