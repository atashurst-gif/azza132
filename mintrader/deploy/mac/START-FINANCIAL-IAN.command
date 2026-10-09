#!/bin/bash
#
#  FINANCIAL IAN - START OR CHECK. Double-click it.
#
#  Starts the bot if it is stopped (the supervisor starts Financial Ian with
#  everything else) and ends with Ian's health in one line. It never changes
#  a setting and never starts a second copy of anything.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
pause_close() { read -r -p 'Press Enter to close. ' _ </dev/tty || true; }

LIB="$SCRIPT_DIR/mintel_mac.sh"
[[ -f "$LIB" ]] || LIB="$HOME/MarketBot/app/deploy/mac/mintel_mac.sh"
START="$(dirname "$LIB")/Start Trading Bot.command"
if [[ ! -f "$LIB" || ! -f "$START" ]]; then
  printf '\nI cannot find the program files. The very first time, double-click\n'
  printf 'INSTALL-FINANCIAL-IAN (or Start Trading Bot) inside the folder you downloaded.\n\n'
  pause_close
  exit 1
fi
# shellcheck source=/dev/null
source "$LIB"

if ! MINTEL_NO_PAUSE=1 bash "$START"; then
  say ""
  say "  ${RED}The bot did not start: see the lines above.${RESET}"
  pause_close
  exit 1
fi

say ""
step "Financial Ian"
LINE="$(wait_bot_line ian 120)"
say "  ${BOLD}${LINE}${RESET}"
if [[ "$(bot_file_mode ian)" == "OFF" ]]; then
  say "  It is switched OFF. Double-click INSTALL-FINANCIAL-IAN to switch it on."
fi
say "  Its page: http://127.0.0.1:$DASH_PORT/ian"
say ""
pause_close
exit 0
