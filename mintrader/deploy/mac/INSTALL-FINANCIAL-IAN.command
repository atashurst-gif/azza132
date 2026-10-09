#!/bin/bash
#
#  FINANCIAL IAN - INSTALL. Double-click it.
#
#  Asks ONCE for an optional Databento API key (nothing is shown as you type;
#  press Enter to skip), saves it in data/secrets.json - never in the code -
#  installs the databento package only when a key is saved, switches
#  Financial Ian on in PAPER (a LIVE choice already made is kept), makes sure
#  the whole bot is running through the usual Start machinery, and ends with
#  Ian's health in one line, for example:
#
#      FINANCIAL IAN - HEALTHY, PAPER, feed NOT CONFIGURED
#
#  Without a key Ian runs safely DATA-DEGRADED: no signals, no trades.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
pause_close() { read -r -p 'Press Enter to close. ' _ </dev/tty || true; }

LIB="$SCRIPT_DIR/mintel_mac.sh"
[[ -f "$LIB" ]] || LIB="$HOME/MarketBot/app/deploy/mac/mintel_mac.sh"
START="$(dirname "$LIB")/Start Trading Bot.command"
if [[ ! -f "$LIB" || ! -f "$START" ]]; then
  printf '\nI cannot find the program files. The very first time, double-click\n'
  printf 'Start Trading Bot inside the folder you downloaded.\n\n'
  pause_close
  exit 1
fi
# shellcheck source=/dev/null
source "$LIB"

say ""
say "=============================================================="
say "   ${BOLD}FINANCIAL IAN - INSTALL${RESET}"
say "=============================================================="

# Not installed yet: the usual installer first (it switches Ian on in PAPER).
if [[ ! -x "$VENV_DIR/bin/python" || ! -f "$CONFIG" ]]; then
  if ! MINTEL_NO_PAUSE=1 bash "$START"; then
    say ""
    say "  ${RED}The bot could not be installed: see the lines above.${RESET}"
    pause_close
    exit 1
  fi
  # shellcheck source=/dev/null
  source "$LIB"
fi

setup_ian || { pause_close; exit 1; }

# Make sure everything runs (a quick check when it already does).
if ! MINTEL_NO_PAUSE=1 bash "$START"; then
  say ""
  say "  ${RED}The bot did not start: see the lines above.${RESET}"
  pause_close
  exit 1
fi

# Ian reads its feed and its key when it starts: restart only Ian (the
# supervisor starts it again within a check) so a new key is used now.
if is_running ian; then
  kill "$(tr -d '[:space:]' < "$DATA_DIR/ian.pid")" 2>/dev/null || true
  sleep 3
fi

say ""
step "Financial Ian"
LINE="$(wait_bot_line ian 120)"
say "  ${BOLD}${LINE}${RESET}"
say "  Its page: http://127.0.0.1:$DASH_PORT/ian"
say ""
pause_close
exit 0
