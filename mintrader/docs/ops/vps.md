# Moving the bot to a Windows VPS

A laptop sleeps when its lid closes, and the bot sleeps with it. A Windows
VPS (a Windows computer you rent in a data centre) never closes its lid: once
it is set up, every bot keeps running after a reboot, with nobody logged in,
and closing or shutting down the Mac stops nothing.

This is the same program as on the Mac. On Windows it talks to MetaTrader 5
directly (no Wine, no bridge). The supervisor (the watchdog) starts and keeps
alive the trader, the **Rapid Momentum Rider** and **Financial Ian**; the
status page is the same page.

**One rule above all: only one copy may ever trade the account.** Two copies
would place every trade twice. Step 1 below stops the Mac for good before the
VPS starts.

---

## What you need

- **A Windows VPS**: Windows Server 2019 or 2022 (or Windows 10/11), 2 CPU
  cores, 4 GB of memory, 60 GB of disk. Choose a data centre close to your
  broker's trade servers (ask IC Markets which city serves your MT5 server;
  London and New York are typical). Any reputable provider will do; prices
  vary, so compare a few.
- **Remote Desktop on the Mac**: Microsoft's free "Windows App" (formerly
  "Microsoft Remote Desktop") from the Mac App Store.
- Your MetaTrader 5 account number, password and server name.
- Optionally, the GitHub token (for the nightly report) and the Databento key
  (for Financial Ian). If you copy `secrets.json` from the Mac (step 3), both
  come with it.

---

## Step 1 - stop the Mac for good

On the Mac:

1. Double-click **Stop Trading Bot**. Open trades stay open with their stop at
   the broker.
2. Stop the Mac from starting it again at the next login. In Terminal:

   ```bash
   for a in trader report pulse; do
     launchctl unload ~/Library/LaunchAgents/com.mintel.$a.plist 2>/dev/null
     rm -f ~/Library/LaunchAgents/com.mintel.$a.plist
   done
   ```

   From now on, **do not double-click Start Trading Bot on the Mac** (it would
   put the agents back and start a second copy).

## Step 2 - connect to the VPS

Open the Windows App on the Mac, add the VPS with the address, user name and
password your provider gave you, and connect. You now see the VPS's Windows
desktop in a window.

## Step 3 - copy the program (and, if you like, the records)

1. On the Mac, zip the program folder you downloaded (the one with `mintel`,
   `deploy`, `tests` inside). Copy the zip to the VPS: drag it into the Remote
   Desktop window, or download it on the VPS from GitHub. Unzip it anywhere,
   for example `C:\Users\Administrator\Downloads\mintrader`.
2. Optional - to keep every bot's own records and saved keys, copy these files
   from `~/MarketBot/data` on the Mac into `C:\mintel\data` on the VPS (make
   the folder first):

   `journal.sqlite`, `runner.sqlite`, `bandbreaker.sqlite`, `crowd.sqlite`,
   `rider.sqlite`, `ian.sqlite`, `scalper.sqlite`, `tnb_paper.sqlite` (Trend &
   Breakout's paper trades), `rider.json`, `ian.json`, `scalper.json`,
   `secrets.json` and `config.json`.

   If you copy `config.json`, answer **y** when the setup asks "Change your
   settings?" in step 5: that rewrites the Mac's folders and Wine settings for
   Windows, while the measuring start, the account reset and every bot's
   block are carried over. `secrets.json` is kept whole: the password you type
   replaces only the password.

## Step 4 - MetaTrader 5

Install MetaTrader 5 on the VPS (IC Markets' download, or let the setup
download the standard one), log in with your account, and switch **Algo
Trading** on (the button in the toolbar). Leave it running.

## Step 5 - run the setup ONCE

In the unzipped folder, open `deploy` and double-click **SETUP-AND-START.cmd**.
It asks for administrator rights (needed for the tasks that restart
everything after a reboot) - say yes. Then it:

- installs Python if it is missing, and the bot's own Python environment;
- asks for your settings (once);
- the first time, sets the line-up (once; it prints which bots are LIVE and
  which are PAPER, and a later choice made with the modes command is never
  undone by running the setup again):
  - **LIVE**: the Momentum Runner, the Rapid Momentum Rider (**GBP 1 a
    pip**, in place of the retired Rapid Scalper) and Financial Ian (it can
    only trade once a CME data feed is configured);
  - **PAPER**: Trend & Breakout (real prices, simulated orders; the Momentum
    Runner still rides its index entries with its own real orders), the
    Band Breaker and the Crowd Fader;
- registers the scheduled tasks:
  - **MintelTrader** - at every boot, starts everything;
  - **MintelKeepAlive** - every 5 minutes, starts whatever has stopped (it
    never starts a second copy);
  - **MintelReport** - every evening at 22:10, the day's record to GitHub;
  - **MintelPulse** - every 10 minutes, UP or DOWN, to GitHub;
- starts the watchdog, which starts the trader, the Rider and Ian;
- checks everything and prints a summary.

For Financial Ian's data feed, double-click **INSTALL-FINANCIAL-IAN.cmd**: it
asks once for the Databento key (nothing is shown as you type; press Enter to
skip). Without a key Ian runs safely with no signals and no trades.

## Step 6 - from then on

Nothing. The scheduled tasks keep it running. To check on it, double-click:

| File | What it does |
|---|---|
| `START-BOT.cmd` | starts whatever is not running, prints the health |
| `START-RAPID-RIDER.cmd` | the same, then one line: `RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 28 markets` |
| `START-FINANCIAL-IAN.cmd` | the same, then one line: `FINANCIAL IAN - HEALTHY, PAPER, feed NOT CONFIGURED` |
| `SETUP-RAPID-RIDER.cmd` | puts the Rider back on LIVE (GBP 1 a pip unless a figure is set) and checks it |
| `STOP-BOT.cmd` | stops everything; open trades keep their broker stops |

Once a day, `START-BOT` (and the scheduled tasks that run it) also starts the
Rapid Momentum Rider's research on the last ten days of real ticks, in the
background, on one worker process at Windows' lowest priority (Idle), so it
never slows the bots. Its log is `C:\mintel\logs\rider-research.log`.

To see or change the line-up later (Trend & Breakout is `tnb`, LIVE or
PAPER only):

```
C:\mintel\venv\Scripts\python.exe -m mintel.ops.modes --config C:\mintel\data\config.json --show
C:\mintel\venv\Scripts\python.exe -m mintel.ops.modes --config C:\mintel\data\config.json --check tnb
```

When you leave, just **close the Remote Desktop window** (disconnect). Do not
choose "Sign out" in Windows: MetaTrader is safest left running in your
session.

To install a newer version: copy the new folder over (step 3) and run
`SETUP-AND-START.cmd` again, answering **N** to "Change your settings?". It
copies the new program and restarts everything on it.

## Step 7 - watching from the Mac

The status page listens only on the VPS itself (`127.0.0.1:8787`), on
purpose: it has no password, so it must never be open to the internet. Two
safe ways to see it:

- **Inside Remote Desktop**: open a browser on the VPS at
  <http://127.0.0.1:8787> (and `/rider`, `/ian`, `/results`).
- **On the Mac, through an SSH tunnel**: on the VPS, turn on Windows' own
  OpenSSH Server (Settings - Apps - Optional features - OpenSSH Server, then
  start the "OpenSSH SSH Server" service). On the Mac, in Terminal:

  ```bash
  ssh -N -L 8787:127.0.0.1:8787 Administrator@YOUR-VPS-ADDRESS
  ```

  Leave that running and open <http://127.0.0.1:8787> on the Mac. Closing the
  Terminal window closes the tunnel, not the bot.

The nightly report and the pulse keep arriving on GitHub as before, as long
as the GitHub token is in `C:\mintel\data\secrets.json` (copied in step 3, or
saved with `C:\mintel\venv\Scripts\python.exe -m mintel.ops.report_upload
--config C:\mintel\data\config.json --set-token`).

---

## Where things are on the VPS

| Path | What |
|---|---|
| `C:\mintel\app` | the program |
| `C:\mintel\data` | settings, keys (`secrets.json`), every bot's records and status |
| `C:\mintel\logs` | what each part has been doing: `trader.out.log`, `rider.out.log`, `ian.out.log`, `watchdog.log` |

## Switching a bot by hand

```
C:\mintel\venv\Scripts\python.exe -m mintel.ops.modes --config C:\mintel\data\config.json --show
C:\mintel\venv\Scripts\python.exe -m mintel.ops.modes --config C:\mintel\data\config.json --paper rider
```

then `STOP-BOT.cmd` and `START-BOT.cmd`. The Rapid Scalper is retired: it can
only be switched OFF.
