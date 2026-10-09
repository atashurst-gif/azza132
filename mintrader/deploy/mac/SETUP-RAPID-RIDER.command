#!/bin/bash
#
#  RAPID MOMENTUM RIDER - SETUP. Double-click it.
#
#  Puts the Rapid Momentum Rider on LIVE in place of the retired Rapid
#  Scalper (GBP 1 a pip unless a figure is already set), makes sure the whole
#  bot is installed and running through the usual Start machinery, and ends
#  with the Rider's health in one line, for example:
#
#      RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 28 markets
#
#  Safe to double-click any time: nothing here can start a second copy of
#  anything (the supervisor starts the Rider, and the Rider refuses to run
#  twice).

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
say "   ${BOLD}RAPID MOMENTUM RIDER - SETUP${RESET}"
say "=============================================================="

# Installed already? Then switch it on now; on a first install the
# installer's own one-time step does exactly this.
if [[ -x "$VENV_DIR/bin/python" && -f "$CONFIG" ]]; then
  setup_rider || { pause_close; exit 1; }
fi

# Install, repair or just check everything - the usual machinery.
if ! MINTEL_NO_PAUSE=1 bash "$START"; then
  say ""
  say "  ${RED}The bot did not start: see the lines above.${RESET}"
  pause_close
  exit 1
fi

# The Rider reads its mode when it starts: restart only the Rider if it is
# still running in its old mode (the supervisor starts it again).
restart_if_mode_changed rider

say ""
step "Rapid Momentum Rider"
LINE="$(wait_bot_line rider 150)"
say "  ${BOLD}${LINE}${RESET}"
say "  Its page: http://127.0.0.1:$DASH_PORT/rider"
say ""
pause_close
exit 0
