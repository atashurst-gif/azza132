#!/bin/bash
#
#  FINANCIAL IAN - TEST ON PAST DATA. Double-click it.
#
#  Lower risk first: before any live CME feed is paid for, this tests
#  Financial Ian on REAL PAST CME futures order-book data bought from
#  Databento's historical service (priced per request).
#
#   1. Installs the databento package into the bot's own Python if missing.
#   2. Asks ONCE for the Databento API key if none is saved (nothing is shown
#      as you type; it is kept in data/secrets.json and never printed).
#      Saving it does NOT switch Financial Ian's live feed on.
#   3. Asks Databento what the test costs and shows it first, for example:
#        This test will download about 2.1 GB of CME order-book data for 5
#        days and Databento will charge about $37.40.
#        Go ahead? (y/N)
#      Nothing is bought unless you type y. Days already downloaded are never
#      bought again. A cheaper first look: drag this file into Terminal and
#      add  --days 2  before pressing Enter.
#   4. Replays the data through Ian's own engine (no MetaTrader, no orders),
#      writes the report, uploads it to GitHub (reports/ian-research/<date>/)
#      and prints the verdict and where the report is.
#
#  It never restarts, stops or changes the running bots, and it runs at the
#  lowest priority (nice -n 19) so the bots always come first.
#
#  Works double-clicked inside the downloaded folder (deploy/mac) or copied
#  anywhere else, such as the Desktop.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
pause_close() {
  [[ -n "${MINTEL_NO_PAUSE:-}" ]] && return 0
  read -r -p 'Press Enter to close. ' _ </dev/tty || true
}

MB="${MINTEL_HOME:-$HOME/MarketBot}"
PY="$MB/venv/bin/python"
DATA="$MB/data"
CFG="$DATA/config.json"
LOW=(nice -n 19)

echo ""
echo "=============================================================="
echo "   FINANCIAL IAN - TEST ON PAST CME DATA"
echo "=============================================================="

if [[ ! -x "$PY" || ! -f "$CFG" ]]; then
  echo ""
  echo "The bot is not installed on this Mac yet. Double-click Start Trading Bot"
  echo "first (in the folder you downloaded), then this again."
  echo ""
  pause_close
  exit 1
fi

# The program to test: the one next to this file when it was double-clicked
# inside a downloaded folder (deploy/mac), otherwise the installed one.
APP=""
for cand in "$SCRIPT_DIR/../.." "$MB/app"; do
  if [[ -f "$cand/mintel/ian/research/historical.py" ]]; then
    APP="$(cd "$cand" && pwd -P)"
    break
  fi
done
if [[ -z "$APP" ]]; then
  echo ""
  echo "This copy of the bot does not have the past-data test yet. Double-click"
  echo "Start Trading Bot inside the newest download (it updates the program),"
  echo "then this again."
  echo ""
  pause_close
  exit 1
fi
cd "$APP" || { pause_close; exit 1; }
echo "Testing the Financial Ian code in: $APP"

if ! "${LOW[@]}" "$PY" -c 'import databento' >/dev/null 2>&1; then
  echo ""
  echo "Installing the databento package into the bot's own Python (one time) ..."
  if ! "${LOW[@]}" "$PY" -m pip install --quiet --disable-pip-version-check databento; then
    echo ""
    echo "The databento package did not install (see the lines above). Nothing was bought."
    echo ""
    pause_close
    exit 1
  fi
fi

if ! "$PY" - "$DATA/secrets.json" >/dev/null 2>&1 <<'PYCHK'
import json, sys
try:
    ok = bool(json.load(open(sys.argv[1])).get("databento_api_key"))
except Exception:
    ok = False
sys.exit(0 if ok else 1)
PYCHK
then
  echo ""
  echo "No Databento API key is saved yet. Paste it below - it starts with db-"
  echo "(Databento portal -> API keys). Nothing is shown as you type."
  if ! "${LOW[@]}" "$PY" -m mintel.ian.research.historical --config "$CFG" --set-key; then
    echo ""
    echo "No key saved: nothing was bought. Double-click this again when you have it."
    echo ""
    pause_close
    exit 1
  fi
fi

"${LOW[@]}" "$PY" -m mintel.ian.research.historical --config "$CFG" --upload ${1+"$@"}
code=$?
echo ""
# 10 is the test's own "nothing was bought in this run" and nothing else returns it (Python exits 1 on a
# crash, 2 on a bad option, 4 when Ctrl+C stopped it after buying had started).
case "$code" in
  0) echo "Finished. The report is in $DATA/ian-research/ (and on GitHub when the upload worked)." ;;
  10) echo "Nothing was bought." ;;
  *) echo "It stopped - see the lines above. Anything already downloaded was paid for and stays on disk"
     echo "(it is never bought again: run this again to carry on)." ;;
esac
echo ""
pause_close
exit "$code"
