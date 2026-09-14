#!/bin/bash
#
#  ONE ICON. DOUBLE-CLICK IT.
#
#  First time  : installs everything and starts trading.
#  Any time    : if it is stopped, starts it. If it is running, says so.
#  After a crash, a restart, or the Mac waking up: fixes whatever is missing.
#
#  It is always safe to double-click. Nothing here can start a second trader:
#  the supervisor, the trader and the bridge each refuse to run twice.

set -uo pipefail

# A .command window closes the moment the script exits, so an unexpected error
# would flash past unread. This keeps the window open and shows exactly what
# happened and where, whatever goes wrong.
on_unexpected_exit() {
  local code=$?
  if (( code != 0 )); then
    printf '\n'
    printf 'Something went wrong (exit code %s, line %s).\n' "$code" "${BASH_LINENO[0]:-?}"
    printf 'Copy the last few lines above and send them to me and I will fix it.\n\n'
    read -r -p 'Press Enter to close. ' _ </dev/tty || true
  fi
}
trap on_unexpected_exit EXIT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# The icon can live on the Desktop while the program lives somewhere else, so
# find the real source: either next to this script, or the folder it was
# installed from.
find_source() {
  # 1. Next to this script - the first run, straight from the download.
  local candidate="$SCRIPT_DIR"
  for _ in 1 2 3 4; do
    if [[ -d "$candidate/mintel" && -f "$candidate/pytest.ini" ]]; then
      printf '%s' "$candidate"; return 0
    fi
    candidate="$(dirname "$candidate")"
  done
  # 2. The installed copy - what the Desktop icon uses, and authoritative once
  #    it exists, because the download folder may well have been deleted.
  if [[ -d "$HOME/MarketBot/app/mintel" ]]; then
    printf '%s' "$HOME/MarketBot/app"; return 0
  fi
  # 3. Wherever it was last installed from, if that still exists.
  if [[ -f "$HOME/MarketBot/.source" ]]; then
    local remembered; remembered="$(cat "$HOME/MarketBot/.source")"
    [[ -d "$remembered/mintel" ]] && { printf '%s' "$remembered"; return 0; }
  fi
  return 1
}

SOURCE_DIR="$(find_source)" || {
  printf '\nI cannot find the program files.\n\n'
  printf 'The very first time, run this from inside the folder you downloaded.\n'
  printf 'It will install itself and put a permanent icon on your Desktop, and\n'
  printf 'after that you can delete the download.\n\n'
  read -r -p 'Press Enter to close. ' _
  exit 1
}

export MINTEL_HOME="${MINTEL_HOME:-$HOME/MarketBot}"
mkdir -p "$MINTEL_HOME"
printf '%s' "$SOURCE_DIR" > "$MINTEL_HOME/.source"

LIB="$SOURCE_DIR/deploy/mac/mintel_mac.sh"
[[ -f "$LIB" ]] || LIB="$MINTEL_HOME/app/deploy/mac/mintel_mac.sh"
# shellcheck source=/dev/null
source "$LIB"

export MINTEL_APP="$APP_DIR"

clear
say ""
say "=============================================================="
say "   ${BOLD}MARKET INTELLIGENCE TRADER${RESET}"
say "=============================================================="

MODE="${1:-start}"
if [[ "$MODE" == "stop" ]]; then
  stop_everything
  trap - EXIT
  read -r -p "Press Enter to close. " _ </dev/tty
  exit 0
fi

# ---- Fast path: already healthy? Then say so and stop touching things. -------
if is_running watchdog && is_running trader && ! source_changed "$SOURCE_DIR"; then
  say ""
  say "  ${GREEN}It is already running.${RESET} Checking it over..."
  # Keep these cheap and idempotent, so a routine double-click stays fast.
  find_native_python >/dev/null 2>&1
  prevent_idle_sleep
  show_health
  say ""
  say "  Status page :  http://127.0.0.1:$DASH_PORT"
  say "  Results page:  http://127.0.0.1:$DASH_PORT/results"
  say ""
  open_dashboard
  say "  ${DIM}This window can be closed.${RESET}"
  say ""
  trap - EXIT
  read -r -p "Press Enter to close. " _ </dev/tty
  exit 0
fi

# ---- Otherwise: install what is missing, repair what is broken, start. -------
say ""
say "  Setting things up. Anything already done is skipped."
say "  ${DIM}The first run downloads a lot and can take 20-30 minutes.${RESET}"

check_macos          || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
make_folders
copy_program "$SOURCE_DIR"
find_native_python   || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
make_venv            || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
ensure_wine          || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
ensure_wine_python   || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
ensure_mt5           || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
write_config         || { final_report; trap - EXIT; read -r -p "Press Enter to close. " _ </dev/tty; exit 1; }
install_launch_agent
install_desktop_icon
prevent_idle_sleep
start_mt5
start_everything
show_health

step "Checking every part of it"
(cd "$APP_DIR" && WINEPREFIX="$WINE_PREFIX" WINEDEBUG="-all" \
  "$VENV_DIR/bin/python" -m mintel.verify --config "$CONFIG" 2>&1) | sed 's/^/  /'

final_report
STATUS=$?
open_dashboard
say "  ${DIM}This window can be closed.${RESET}"
say ""
trap - EXIT
read -r -p "Press Enter to close. " _ </dev/tty
exit $STATUS
