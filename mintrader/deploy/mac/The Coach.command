#!/bin/bash
#
#  THE COACH. DOUBLE-CLICK IT.
#
#  The coach decides which of the bots' trades use real money, how much, and
#  in which markets - always inside your own limits, and it only ever loosens
#  at night (announced the evening before, released at 06:30 UK unless you
#  veto it). It cannot place, close or change an order, and it never touches
#  config.json.
#
#  1) open its page   2) coach off: back to my settings   3) HALT all new
#  real-money entries   4) resume   5) approve or veto tonight's changes
#  6) quit

set -uo pipefail

MINTEL_HOME="${MINTEL_HOME:-$HOME/MarketBot}"
APP_DIR="$MINTEL_HOME/app"
DATA_DIR="$MINTEL_HOME/data"
CONFIG="$DATA_DIR/config.json"
PY="$MINTEL_HOME/venv/bin/python"
DASH_PORT="${DASH_PORT:-8787}"

pause_close() {
  read -r -p 'Press Enter to close. ' _ </dev/tty || true
}

back_to_menu() {
  read -r -p '  Press Enter to go back to the menu. ' _ </dev/tty || true
}

if [[ ! -x "$PY" || ! -d "$APP_DIR/mintel/coach" ]]; then
  printf '\nThe coach is not installed yet. Double-click Start Trading Bot first.\n\n'
  pause_close
  exit 1
fi

# coach WORDS... - python -m mintel.coach with this Mac's settings file
coach() {
  (cd "$APP_DIR" && "$PY" -m mintel.coach "$@" --config "$CONFIG")
}

halt_all() {
  printf '\n  HALT stops every NEW real-money entry in every bot at once.\n'
  printf '  Open trades are still managed with their stops; practice carries on.\n\n'
  local answer=""
  read -r -p '  Type YES (in capitals) to HALT: ' answer </dev/tty || answer=""
  coach halt --yes "$answer"
  back_to_menu
}

resume_all() {
  local answer=""
  read -r -p '  Type YES (in capitals) to allow new real-money entries again: ' answer </dev/tty || answer=""
  coach resume --yes "$answer"
  coach on
  back_to_menu
}

approve_or_veto() {
  clear
  printf '\n'
  coach show 2>/dev/null | sed -n '/plan/,$p' | head -40
  printf '\n  Type  v NUMBER  to veto an item of tonight'"'"'s plan (e.g. v 1),\n'
  printf '        a NAME    to approve a "Needs your yes" item (e.g. a f1-jump),\n'
  printf '        or just Enter to go back.\n\n'
  local line=""
  read -r -p '  > ' line </dev/tty || line=""
  case "$line" in
    v\ *) coach veto "${line#v }" ;;
    a\ *) coach approve "${line#a }" ;;
    *) ;;
  esac
  back_to_menu
}

while true; do
  clear
  printf '\n  THE COACH - which trades use real money, within your limits\n\n'
  printf '  %s\n\n' "$(coach show --once 2>/dev/null | tail -1)"
  printf '  1) Open its page\n'
  printf '  2) Coach off: back to my own settings\n'
  printf '  3) HALT all new real-money entries\n'
  printf '  4) Resume (coach on, HALT off)\n'
  printf '  5) Approve or veto tonight'"'"'s announced changes\n'
  printf '  6) Quit\n\n'
  choice=""
  read -r -p '  Choose 1 to 6: ' choice </dev/tty || choice="6"
  case "${choice:-}" in
    1) open "http://127.0.0.1:$DASH_PORT/coach" >/dev/null 2>&1 \
         || printf '  Open http://127.0.0.1:%s/coach in a browser.\n' "$DASH_PORT"
       sleep 1 ;;
    2) coach off; back_to_menu ;;
    3) halt_all ;;
    4) resume_all ;;
    5) approve_or_veto ;;
    6|q|Q) exit 0 ;;
    *) ;;
  esac
done
