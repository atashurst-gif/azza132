# Running on a MacBook

## The one icon

**First time only:** open the folder you downloaded, go into `deploy/mac`, and
double-click **`Start Trading Bot.command`**.

macOS will refuse the first time because the file came from the internet:
right-click it → **Open** → **Open**. You only do that once.

It then installs everything, asks for your account details once, starts
trading, and **puts a permanent icon on your Desktop.** After that you can
delete the download.

**Every time after:** double-click **Start Trading Bot** on your Desktop.

- If the bot is stopped, it starts it.
- If it is already running, it says so, checks it over, and opens the status
  page.
- It is always safe to double-click. Nothing here can start a second trader.

Two more icons land on your Desktop alongside it:

- **Self Test** — proves the Mac side works without any broker, account or
  risk. Worth running first if you want to see it working before committing an
  account.
- **Stop Trading Bot** — stops it. Open trades stay open and keep their stop
  loss at the broker.

## What actually gets installed

Everything lives in one folder, `~/MarketBot`, and nothing is installed
system-wide except Homebrew (for Python):

```
~/MarketBot/
  app/               the program
  data/              your settings, the trade journal, the news archive
  logs/              what it has been doing
  venv/              its own Python, separate from any Python you use
  Wine Devel.app/    WineHQ's official macOS build of Wine (checksum-verified)
  wine/              a self-contained Windows environment for MetaTrader 5
```

To remove it completely: stop it, delete `~/MarketBot`, and delete
`~/Library/LaunchAgents/com.mintel.trader.plist`.

## How it works on a Mac

MetaTrader 5's Python interface is a Windows library. It has no macOS build and
will not get one.

So the system is split in two, which the architecture already allowed for
because the engine talks to a `Broker` interface rather than to MT5 directly:

```
  ┌─────────────────── your Mac ───────────────────┐
  │                                                │
  │   Wine                     native macOS        │
  │   ┌──────────────┐         ┌────────────────┐  │
  │   │ MetaTrader 5 │◄───────►│ bridge server  │  │
  │   └──────────────┘         │ (Windows       │  │
  │                            │  Python)       │  │
  │                            └───────┬────────┘  │
  │                        localhost socket        │
  │                                    │           │
  │            ┌───────────────────────▼────────┐  │
  │            │ the trader (native Python)     │  │
  │            │ scanning, news, risk, FlowLock │  │
  │            └───────────────┬────────────────┘  │
  │                            │                   │
  │            ┌───────────────▼────────────────┐  │
  │            │ dashboard  127.0.0.1:8787      │  │
  │            └────────────────────────────────┘  │
  └────────────────────────────────────────────────┘
```

Only the small bridge process runs inside Wine, and it needs nothing but the
standard library and the `MetaTrader5` package. All the heavy work — analysis,
the optional model, the database, the dashboard — runs as ordinary native
macOS Python at native speed.

The socket is bound to `127.0.0.1` and protected by a random token stored with
owner-only permissions, so nothing else on the Mac can drive your trading
connection.

**A dead bridge looks exactly like a dead broker**, which means the existing
health handling applies unchanged: SAFE MODE, keep positions protected,
reconnect, reconcile, resume. There is a test that kills the bridge mid-run and
asserts precisely that.

## Staying alive

| What | How |
|---|---|
| Starts when you log in | a LaunchAgent with `RunAtLoad` |
| Restarts if it crashes | the same agent with `KeepAlive`, plus the watchdog |
| Restarts if it hangs | the watchdog kills it first, then replaces it |
| Keeps MT5 alive | the watchdog restarts the terminal under Wine |
| Keeps the bridge alive | the watchdog probes the port, not just the process |
| Stops the Mac dozing off | `caffeinate` while the bot runs |
| Survives a reboot | the LaunchAgent starts the watchdog, which starts the rest |

Nothing certifies its own health. The watchdog judges the trader; the trader
judges the watchdog.

## The one thing a laptop cannot do

**If you close the lid, the Mac sleeps and the bot pauses with it.** Nothing in
software can prevent that on battery — it is enforced below the level any app
can reach.

What happens is handled properly rather than ignored:

- On waking, the bot notices the gap, logs it as a suspension, throws away its
  stale view of the market, re-reads the calendar, and reconciles against the
  broker before considering any trade.
- Your open positions were never unprotected: their stop losses are held by the
  broker, not by this software. They keep working while the Mac is asleep.
- Trading resumes only once prices are demonstrably fresh again.

To trade with the lid closed you need **clamshell mode**: mains power plus an
external monitor and keyboard. With those connected, closing the lid does not
sleep the Mac and the bot keeps running.

If you want it trading 24/5 with the Mac shut and in your bag, that is what the
Windows VPS setup in `deploy/` is for. Both use the same engine; only the
deployment differs.

## Checking on it

- Status: <http://127.0.0.1:8787>
- Results, with every trade explained: <http://127.0.0.1:8787/results>
- From Terminal: `~/MarketBot/venv/bin/python -m mintel.status --config ~/MarketBot/data/config.json`
- Full dependency check: `~/MarketBot/venv/bin/python -m mintel.verify --config ~/MarketBot/data/config.json`

## Watching it work without a broker

```bash
cd ~/MarketBot/app
./deploy/mac/'Self Test.command'
```

Or run a longer demo against a live-tracking simulation, at thirty times speed:

```bash
~/MarketBot/venv/bin/python -m mintel.broker.bridge_server \
    --backend sim --sim-live --sim-speed 30 --port 8790 &
~/MarketBot/venv/bin/python -m mintel.run --config ~/MarketBot/data/config.json
```

No order can reach a broker in either case.

## If something goes wrong

| Symptom | What to do |
|---|---|
| Icon will not open | right-click → Open → Open |
| "Wine could not be installed" | The output above that line says why (usually the download). Double-click the icon again; it resumes. Wine is WineHQ's official macOS package, downloaded from `github.com/Gcenx/macOS_Wine_builds` and checksum-verified, unpacked to `~/MarketBot/Wine Devel.app`. (The development build, not 11.0 stable: MetaTrader refuses Wine 10.3–11.0 with "A debugger has been found".) Homebrew's `wine-stable` cask is disabled (since 2026-09-01) and is only tried as a fallback. |
| MT5 window never appears | `WINEPREFIX=~/MarketBot/wine wine ~/MarketBot/wine/drive_c/Program\ Files/MetaTrader\ 5/terminal64.exe` |
| Status page says SAFE MODE | it tells you why in plain English; usually MT5 is closed or logged out |
| Nothing trades | normal. Check the "what it is looking at" list — it shows why each idea is being passed over |
| Start again from scratch | stop it, delete `~/MarketBot`, double-click the icon |

Logs are in `~/MarketBot/logs/`. The most useful one is `trader.err.log`.
