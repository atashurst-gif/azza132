#!/bin/bash
cd "$(dirname "$0")" || exit 1
if ! command -v python3 >/dev/null 2>&1; then
  echo "Python 3 is required. Install Python 3 or ask Claude Code to run the project for you."
  read -r -p "Press Return to close..." ignored
  exit 1
fi
python3 -m http.server 8000 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null' EXIT
sleep 1
open http://localhost:8000
wait "$server_pid"
