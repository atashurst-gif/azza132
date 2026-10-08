#!/bin/bash
# Double-click to start Fretwise on a Mac. Uses Node (AI coach capable) if installed, otherwise Python (demo coach only).
cd "$(dirname "$0")" || exit 1
if command -v node >/dev/null 2>&1; then
  node server.mjs &
elif command -v python3 >/dev/null 2>&1; then
  python3 -m http.server 8000 &
else
  echo "Node.js or Python 3 is required. Install one of them, or ask Claude Code to run the project for you."
  read -r -p "Press Return to close..." ignored
  exit 1
fi
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null' EXIT
sleep 1
open http://localhost:8000
wait "$server_pid"
