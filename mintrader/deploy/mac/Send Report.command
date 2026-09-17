#!/bin/bash
#
#  SEND REPORT - publish today's record (review, every trade, status, rules)
#  to the reports folder in GitHub, so it can be read without screenshots.
#  This also runs by itself every evening after the broker's day closes.
#
#  First time: it asks for a GitHub token and keeps it in data/secrets.json.

set -uo pipefail
HOME_DIR="$HOME/MarketBot"
if [[ ! -x "$HOME_DIR/venv/bin/python" || ! -d "$HOME_DIR/app/mintel" ]]; then
  echo "The bot is not installed yet. Double-click Start Trading Bot first."
  read -r -p "Press Enter to close. " _
  exit 1
fi
cd "$HOME_DIR/app" || exit 1
PY="$HOME_DIR/venv/bin/python"
CFG="$HOME_DIR/data/config.json"
if ! "$PY" - "$HOME_DIR/data/secrets.json" <<'PYCHK'
import json, sys
try:
    ok = bool(json.load(open(sys.argv[1])).get("github_token"))
except Exception:
    ok = False
sys.exit(0 if ok else 1)
PYCHK
then
  echo
  echo "No GitHub token stored yet. Create one at:"
  echo "  github.com -> Settings -> Developer settings -> Personal access tokens"
  echo "  -> Fine-grained tokens -> Generate new token"
  echo "  Repository access: only azza132.  Permissions: Contents = Read and write."
  echo
  "$PY" -m mintel.ops.report_upload --config "$CFG" --set-token || { read -r -p "Press Enter to close. " _; exit 1; }
fi
echo
"$PY" -m mintel.ops.report_upload --config "$CFG" "$@"
echo
read -r -p "Press Enter to close. " _
