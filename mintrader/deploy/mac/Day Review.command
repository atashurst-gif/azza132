#!/bin/bash
#
#  DAY REVIEW - what the bot did today, in plain English, from the broker's
#  own records: price result, commission, real result, by market and by
#  approach, and what should change tomorrow.
#
#  Double-click any evening. Pass a date to review another day:
#      bash "Day Review.command" 2026-09-14

set -uo pipefail
HOME_DIR="$HOME/MarketBot"
if [[ ! -x "$HOME_DIR/venv/bin/python" || ! -d "$HOME_DIR/app/mintel" ]]; then
  echo "The bot is not installed yet. Double-click Start Trading Bot first."
  read -r -p "Press Enter to close. " _
  exit 1
fi
DATE_ARG=()
[[ -n "${1:-}" ]] && DATE_ARG=(--date "$1")
cd "$HOME_DIR/app" || exit 1
echo
"$HOME_DIR/venv/bin/python" -m mintel.ops.day_review --config "$HOME_DIR/data/config.json" "${DATE_ARG[@]}"
echo
read -r -p "Press Enter to close. " _
