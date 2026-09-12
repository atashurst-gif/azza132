# MQL5 component

`MintelGuardian.mq5` is a **safety net, not a strategy.** It makes no trading
decisions. Its only jobs are:

1. Give any position that somehow has no stop loss an emergency ATR-based stop.
2. Close everything if the account breaches a hard daily loss limit.
3. Write a heartbeat the Python watchdog can read.

It exists because the most dangerous state this system can be in is "a position
is open and nothing is protecting it". The Python engine places a broker-side
stop on every entry and FlowLock improves it continuously — but if the Python
process dies at the exact moment between fill and stop placement, this EA
notices within seconds and fixes it from inside the terminal.

It is intentionally dependency-free so it also works under MetaTrader Virtual
Hosting, where external processes are unavailable.

## Installing it

The setup script copies it automatically. To do it by hand:

1. In MetaTrader 5, choose **File → Open Data Folder**.
2. Go into `MQL5\Experts` and create a folder called `Mintel`.
3. Copy `MintelGuardian.mq5` into it.
4. Back in MT5, press **F4** to open MetaEditor, open the file, press **F7**
   to compile. It should report `0 errors, 0 warnings`.
5. Drag `MintelGuardian` from the Navigator onto any chart.
6. Tick **Allow Algo Trading** in the dialog and press OK.
7. Leave the `Magic` input matching the engine's magic number (990311 by
   default) so it only touches the bot's own positions.

You should see a smiley face in the top-right of the chart, and a line in the
Experts tab reading `MintelGuardian active`.
