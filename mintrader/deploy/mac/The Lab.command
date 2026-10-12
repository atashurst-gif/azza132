#!/bin/bash
#
#  THE LAB. DOUBLE-CLICK IT.
#
#  The strategy lab tries trading ideas every night on old prices, then once
#  on prices it has never seen, and practises the best on live prices with no
#  money. It cannot place an order. Nothing here is about real money: an idea
#  that is ready for a live trial waits for Aaron's word.
#
#  1) open its page   2) run it now   3) pause or resume it   4) quit

set -uo pipefail

MINTEL_HOME="${MINTEL_HOME:-$HOME/MarketBot}"
APP_DIR="$MINTEL_HOME/app"
DATA_DIR="$MINTEL_HOME/data"
LOG_DIR="$MINTEL_HOME/logs"
CONFIG="$DATA_DIR/config.json"
PY="$MINTEL_HOME/venv/bin/python"
DASH_PORT="${DASH_PORT:-8787}"

pause_close() {
  read -r -p 'Press Enter to close. ' _ </dev/tty || true
}

if [[ ! -x "$PY" || ! -d "$APP_DIR/mintel/lab" ]]; then
  printf '\nThe lab is not installed yet. Double-click Start Trading Bot first.\n\n'
  pause_close
  exit 1
fi

# lab WORDS... - python -m mintel.lab with this Mac's settings file
lab() {
  (cd "$APP_DIR" && "$PY" -m mintel.lab "$@" --config "$CONFIG")
}

run_now() {
  local job
  local tailer
  mkdir -p "$LOG_DIR"
  touch "$LOG_DIR/lab-nightly.log"
  printf '\n  Running the lab now, gently (nice -n 19: it only takes spare computer time).\n'
  printf '  Its log follows; it says when it is done. It never places an order.\n'
  printf '  It runs between 23:10 and 06:00 on weekdays (any time at the weekend); at other\n'
  printf '  times it says so and stops, because the practice day is not over yet.\n\n'
  (cd "$APP_DIR" && nice -n 19 "$PY" -m mintel.lab.nightly --config "$CONFIG" --upload --workers 1 \
      >> "$LOG_DIR/lab-nightly.log" 2>&1) &
  job=$!
  tail -n 0 -f "$LOG_DIR/lab-nightly.log" &
  tailer=$!
  wait "$job"
  sleep 1
  kill "$tailer" >/dev/null 2>&1
  printf '\n  Finished.\n\n'
  read -r -p '  Press Enter to go back to the menu. ' _ </dev/tty || true
}

pause_or_resume() {
  local now
  now="$(lab mode show 2>/dev/null | tail -1)"
  if [[ "$now" == *"is OFF"* ]]; then
    lab mode paper
  else
    lab mode off
  fi
  read -r -p '  Press Enter to go back to the menu. ' _ </dev/tty || true
}

while true; do
  clear
  printf '\n  THE LAB - ideas tried on old prices, then once on fresh ones, then practised with no money\n\n'
  printf '  %s\n' "$(lab show --one-line 2>/dev/null | tail -1)"
  printf '  %s\n\n' "$(lab mode show 2>/dev/null | tail -1)"
  printf '  1) Open its page\n'
  printf '  2) Run the lab now (in this window)\n'
  printf '  3) Pause it, or resume it\n'
  printf '  4) Quit\n\n'
  choice=""
  read -r -p '  Choose 1, 2, 3 or 4: ' choice </dev/tty || choice="4"
  case "${choice:-}" in
    1) open "http://127.0.0.1:$DASH_PORT/lab" >/dev/null 2>&1 \
         || printf '  Open http://127.0.0.1:%s/lab in a browser.\n' "$DASH_PORT"
       ;;
    2) run_now ;;
    3) pause_or_resume ;;
    *) break ;;
  esac
done
exit 0
