#!/bin/bash
# Stops the bot. Open trades stay open and keep their broker-side stop.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
exec "$SCRIPT_DIR/Start Trading Bot.command" stop
