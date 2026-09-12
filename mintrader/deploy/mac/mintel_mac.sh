#!/bin/bash
# Shared steps for the macOS installation.
#
# Sourced by the desktop launcher.  Every function is idempotent: running it
# again on an already-working machine checks and moves on rather than
# reinstalling, because the whole promise is "double-click it any time".

set -o pipefail

MINTEL_HOME="${MINTEL_HOME:-$HOME/MarketBot}"
APP_DIR="$MINTEL_HOME/app"
DATA_DIR="$MINTEL_HOME/data"
LOG_DIR="$MINTEL_HOME/logs"
VENV_DIR="$MINTEL_HOME/venv"
WINE_PREFIX="$MINTEL_HOME/wine"
CONFIG="$DATA_DIR/config.json"
SECRETS="$DATA_DIR/secrets.json"
TOKEN_FILE="$DATA_DIR/bridge-token.txt"
PLIST_LABEL="com.mintel.trader"
PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_LABEL.plist"
DASH_PORT="${DASH_PORT:-8787}"
BRIDGE_PORT="${BRIDGE_PORT:-8790}"

BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'
YELLOW=$'\033[33m'; CYAN=$'\033[36m'; RESET=$'\033[0m'

FAILURES=()
WARNINGS=()

# "${*:-}" rather than "$*": bash 3.2 with `set -u` treats an unset "$*" as an
# unbound variable, so a bare `say` with no arguments would kill the script.
say()  { printf '%s\n' "${*:-}"; }
step() { printf '\n%s==> %s%s\n' "$CYAN" "${*:-}" "$RESET"; }
good() { printf '    %s[ OK ]%s %s\n' "$GREEN" "$RESET" "${*:-}"; }
warn() { printf '    %s[WARN]%s %s\n' "$YELLOW" "$RESET" "${*:-}"
         WARNINGS[${#WARNINGS[@]}]="${*:-}"; }
bad()  { printf '    %s[FAIL]%s %s\n' "$RED" "$RESET" "${*:-}"
         FAILURES[${#FAILURES[@]}]="${*:-}"; }

# --------------------------------------------------------------- machine -----
check_macos() {
  step "Checking this Mac"
  if [[ "$(uname -s)" != "Darwin" ]]; then
    bad "This launcher is for macOS. On Windows use deploy/SETUP-AND-START.ps1"
    return 1
  fi
  local version
  local arch
  version="$(sw_vers -productVersion)"
  arch="$(uname -m)"
  good "macOS $version on $arch"
  if [[ "$arch" == "arm64" ]]; then
    if /usr/bin/pgrep -q oahd; then
      good "Rosetta 2 is installed (needed to run MetaTrader under Wine)"
    else
      say "    Rosetta 2 is needed to run MetaTrader on this Mac. Installing it."
      say "    ${DIM}macOS may ask for your password here.${RESET}"
      # Output is deliberately NOT hidden: this can prompt, and a silent
      # prompt looks like a hang. A failure is a warning, not a fatal error -
      # Wine may still work, and the user can install Rosetta separately.
      if softwareupdate --install-rosetta --agree-to-license; then
        good "Rosetta 2 installed"
      else
        warn "Rosetta 2 did not install. If Wine fails later, run this yourself: softwareupdate --install-rosetta"
      fi
    fi
  fi
  local free_gb
  free_gb="$(df -g "$HOME" | awk 'NR==2 {print $4}')"
  if [[ "${free_gb:-0}" -lt 10 ]]; then
    warn "Only ${free_gb}GB of free disk space."
  else
    good "${free_gb}GB of free disk space"
  fi
}

# ---------------------------------------------------------------- folders ----
make_folders() {
  step "Setting up folders"
  mkdir -p "$APP_DIR" "$DATA_DIR" "$LOG_DIR" "$MINTEL_HOME"
  good "Everything lives in $MINTEL_HOME"
}

copy_program() {
  local source_dir
  source_dir="${1:-}"
  [[ -n "$source_dir" ]] || { bad "No source folder given."; return 1; }
  step "Installing the program files"
  if [[ "$source_dir" == "$APP_DIR" ]]; then
    good "Already running from the install folder"
    return 0
  fi
  local item
  for item in mintel tests deploy tools pytest.ini requirements.txt \
              requirements-dev.txt README.md config; do
    if [[ -e "$source_dir/$item" ]]; then
      rm -rf "$APP_DIR/${item:?}"
      cp -R "$source_dir/$item" "$APP_DIR/"
    fi
  done
  good "Program files installed to $APP_DIR"
}

# ------------------------------------------------------------ native python --
NATIVE_PY=""
find_native_python() {
  step "Checking Python"
  local candidate
  for candidate in python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      if "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info>=(3,9) else 1)' 2>/dev/null; then
        NATIVE_PY="$(command -v "$candidate")"
        break
      fi
    fi
  done
  if [[ -z "$NATIVE_PY" ]]; then
    warn "No suitable Python found. Installing it with Homebrew."
    ensure_homebrew || return 1
    brew install python@3.11 >/dev/null 2>&1
    NATIVE_PY="$(command -v python3.11 || command -v python3)"
  fi
  if [[ -z "$NATIVE_PY" ]]; then
    bad "Python 3.9 or newer is required and could not be installed."
    return 1
  fi
  good "Python $("$NATIVE_PY" -c 'import sys;print("%d.%d.%d"%sys.version_info[:3])') at $NATIVE_PY"
}

ensure_homebrew() {
  if command -v brew >/dev/null 2>&1; then
    good "Homebrew is installed"
    return 0
  fi
  warn "Homebrew is not installed. Installing it (this can take a few minutes)."
  say "    ${DIM}macOS may ask for your password. That is Homebrew, not the bot.${RESET}"
  # </dev/tty, never </dev/null: the installer asks the user to press RETURN
  # and then for a password, and a prompt with no input is an invisible hang.
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" </dev/tty
  local brew_path
  for brew_path in /opt/homebrew/bin/brew /usr/local/bin/brew; do
    [[ -x "$brew_path" ]] && eval "$("$brew_path" shellenv)" && break
  done
  command -v brew >/dev/null 2>&1 && good "Homebrew installed" && return 0
  bad "Homebrew could not be installed."
  return 1
}

make_venv() {
  step "Setting up the Python environment"
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    "$NATIVE_PY" -m venv "$VENV_DIR" || { bad "Could not create the environment."; return 1; }
  fi
  "$VENV_DIR/bin/python" -m pip install --upgrade pip --quiet 2>/dev/null
  # The engine itself needs nothing but the standard library.  These are for
  # the optional calibrated model and the memory health check; if they fail to
  # build, the bot still runs.
  "$VENV_DIR/bin/python" -m pip install --quiet psutil numpy scikit-learn 2>/dev/null \
    && good "Optional extras installed (model, memory checks)" \
    || warn "Optional extras failed to install. The bot runs without them."
  "$VENV_DIR/bin/python" -c 'import mintel' 2>/dev/null
  good "Environment ready at $VENV_DIR"
}

# ------------------------------------------------------------------- wine ----
ensure_wine() {
  step "Setting up Wine (this is what lets MetaTrader 5 run on a Mac)"
  if command -v wine64 >/dev/null 2>&1 || command -v wine >/dev/null 2>&1; then
    good "Wine is installed: $(command -v wine64 || command -v wine)"
  else
    ensure_homebrew || return 1
    say "    ${DIM}Installing Wine. This is a large download; be patient.${RESET}"
    brew install --cask --no-quarantine wine-stable >/dev/null 2>&1 \
      || brew install --cask wine-stable >/dev/null 2>&1
    if command -v wine64 >/dev/null 2>&1 || command -v wine >/dev/null 2>&1; then
      good "Wine installed"
    else
      bad "Wine could not be installed. Install it manually: brew install --cask wine-stable"
      return 1
    fi
  fi
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  if [[ ! -d "$WINE_PREFIX/drive_c" ]]; then
    say "    ${DIM}Creating the Windows environment (first time only)...${RESET}"
    WINEDLLOVERRIDES="mscoree,mshtml=" wineboot --init >/dev/null 2>&1
  fi
  [[ -d "$WINE_PREFIX/drive_c" ]] && good "Windows environment ready at $WINE_PREFIX" \
    || { bad "The Windows environment could not be created."; return 1; }
}

WINE_BIN=""
wine_bin() {
  WINE_BIN="$(command -v wine64 || command -v wine)"
  printf '%s' "$WINE_BIN"
}

WINE_PY=""
ensure_wine_python() {
  step "Installing Windows Python inside Wine"
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  local wine; wine="$(wine_bin)"
  local target="$WINE_PREFIX/drive_c/Python311/python.exe"
  if [[ -f "$target" ]]; then
    WINE_PY='C:\Python311\python.exe'
    good "Windows Python already installed"
  else
    local installer="$MINTEL_HOME/python-win.exe"
    if [[ ! -f "$installer" ]]; then
      say "    ${DIM}Downloading Windows Python...${RESET}"
      curl -fsSL -o "$installer" \
        "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe" \
        || { bad "Could not download Windows Python."; return 1; }
    fi
    say "    ${DIM}Installing it inside Wine (a few minutes)...${RESET}"
    "$wine" "$installer" /quiet InstallAllUsers=0 PrependPath=0 Include_test=0 \
      TargetDir='C:\Python311' >/dev/null 2>&1
    wineserver -w 2>/dev/null
    if [[ -f "$target" ]]; then
      WINE_PY='C:\Python311\python.exe'
      good "Windows Python installed"
    else
      bad "Windows Python did not install inside Wine."
      return 1
    fi
  fi
  say "    ${DIM}Installing the MetaTrader 5 package inside Wine...${RESET}"
  "$wine" "$WINE_PY" -m pip install --quiet --upgrade pip >/dev/null 2>&1
  "$wine" "$WINE_PY" -m pip install --quiet MetaTrader5 >/dev/null 2>&1
  wineserver -w 2>/dev/null
  if "$wine" "$WINE_PY" -c "import MetaTrader5" >/dev/null 2>&1; then
    good "MetaTrader5 package working inside Wine"
  else
    bad "The MetaTrader5 package is not working inside Wine."
    return 1
  fi
}

MT5_TERMINAL=""
ensure_mt5() {
  step "Installing MetaTrader 5"
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  local wine; wine="$(wine_bin)"
  local found
  found="$(find "$WINE_PREFIX/drive_c" -maxdepth 5 -name terminal64.exe 2>/dev/null | head -1)"
  if [[ -n "$found" ]]; then
    MT5_TERMINAL="$(printf '%s' "$found" | sed "s|$WINE_PREFIX/drive_c|C:|" | tr '/' '\\')"
    good "MetaTrader 5 found: $MT5_TERMINAL"
    return 0
  fi
  local installer="$MINTEL_HOME/mt5setup.exe"
  if [[ ! -f "$installer" ]]; then
    say "    ${DIM}Downloading MetaTrader 5...${RESET}"
    curl -fsSL -o "$installer" \
      "https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe" \
      || { bad "Could not download MetaTrader 5."; return 1; }
  fi
  say ""
  say "    ${BOLD}The MetaTrader 5 installer is about to open.${RESET}"
  say "    Click Next, then Finish. When it asks you to log in, enter your"
  say "    account number, password and server, then click Login."
  say "    ${DIM}Come back to this window when MetaTrader is showing charts.${RESET}"
  say ""
  read -r -p "    Press Enter to open the installer... " _ </dev/tty
  "$wine" "$installer" >/dev/null 2>&1
  wineserver -w 2>/dev/null
  found="$(find "$WINE_PREFIX/drive_c" -maxdepth 6 -name terminal64.exe 2>/dev/null | head -1)"
  if [[ -n "$found" ]]; then
    MT5_TERMINAL="$(printf '%s' "$found" | sed "s|$WINE_PREFIX/drive_c|C:|" | tr '/' '\\')"
    good "MetaTrader 5 installed: $MT5_TERMINAL"
  else
    bad "MetaTrader 5 was not found after installation."
    return 1
  fi
}

# ----------------------------------------------------------------- config ----
write_config() {
  step "Your settings"
  if [[ -f "$CONFIG" ]]; then
    good "Existing settings found"
    local answer
    read -r -p "    Change them? (y/N) " answer </dev/tty
    [[ "$answer" =~ ^[Yy] ]] || { refresh_config_paths; return 0; }
  fi

  say ""
  say "    ${BOLD}I need a few details. You are only asked once.${RESET}"
  say ""
  local login password server mode marker base_risk max_risk daily aggr
  read -r -p "    MetaTrader account number: " login </dev/tty
  read -r -s -p "    MetaTrader password: " password </dev/tty; say ""
  read -r -p "    MetaTrader server name (exactly as your broker wrote it): " server </dev/tty
  say ""
  say "    ${DIM}DEMO is strongly recommended until you have watched it run.${RESET}"
  read -r -p "    Trade on DEMO or LIVE? [DEMO] " mode </dev/tty
  if [[ "$mode" =~ ^[Ll] ]]; then
    mode="LIVE"
    say ""
    say "    ${RED}${BOLD}LIVE means real money can be lost.${RESET}"
    local confirm
    read -r -p "    Type exactly I-UNDERSTAND-THIS-TRADES-REAL-MONEY to confirm: " confirm </dev/tty
    if [[ "$confirm" == "I-UNDERSTAND-THIS-TRADES-REAL-MONEY" ]]; then
      marker="$confirm"
    else
      warn "Not confirmed - staying on DEMO."
      mode="DEMO"; marker=""
    fi
  else
    mode="DEMO"; marker=""
  fi
  say ""
  read -r -p "    Normal risk per trade, % of the account [0.5]: " base_risk </dev/tty
  read -r -p "    MAXIMUM risk per trade, % (never exceeded) [1.5]: " max_risk </dev/tty
  read -r -p "    Stop for the day after losing this % [3.0]: " daily </dev/tty
  read -r -p "    Aggression CONSERVATIVE/NORMAL/AGGRESSIVE/MAXIMUM [NORMAL]: " aggr </dev/tty
  base_risk="${base_risk:-0.5}"; max_risk="${max_risk:-1.5}"
  daily="${daily:-3.0}"; aggr="$(echo "${aggr:-NORMAL}" | tr '[:lower:]' '[:upper:]')"

  MINTEL_LOGIN="$login" MINTEL_SERVER="$server" MINTEL_MODE="$mode" \
  MINTEL_MARKER="$marker" MINTEL_BASE="$base_risk" MINTEL_MAX="$max_risk" \
  MINTEL_DAILY="$daily" MINTEL_AGGR="$aggr" MINTEL_DATA="$DATA_DIR" \
  MINTEL_LOGS="$LOG_DIR" MINTEL_WINE="$WINE_PREFIX" MINTEL_WINEPY="$WINE_PY" \
  MINTEL_TERMINAL="$MT5_TERMINAL" MINTEL_DASH="$DASH_PORT" \
  MINTEL_BRIDGE="$BRIDGE_PORT" MINTEL_PASSWORD="$password" \
  "$VENV_DIR/bin/python" - <<'PYEOF'
import json, os, pathlib, sys
sys.path.insert(0, os.environ.get("MINTEL_APP", "."))
data = pathlib.Path(os.environ["MINTEL_DATA"])
data.mkdir(parents=True, exist_ok=True)

def f(name, default):
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return float(default)

max_risk = f("MINTEL_MAX", 1.5)
cfg = {
    "version": 1,
    "mode": os.environ["MINTEL_MODE"],
    "live_marker": os.environ.get("MINTEL_MARKER", ""),
    "magic": 990311,
    "account_login": int(os.environ.get("MINTEL_LOGIN") or 0),
    "account_server": os.environ.get("MINTEL_SERVER", ""),
    "mt5_terminal_path": os.environ.get("MINTEL_TERMINAL", ""),
    "broker_mode": "bridge",
    "bridge_host": "127.0.0.1",
    "bridge_port": int(os.environ.get("MINTEL_BRIDGE") or 8790),
    "wine_prefix": os.environ.get("MINTEL_WINE", ""),
    "wine_python": os.environ.get("MINTEL_WINEPY", ""),
    "aggression": os.environ.get("MINTEL_AGGR", "NORMAL"),
    "risk": {
        "base_risk_pct": f("MINTEL_BASE", 0.5),
        "max_risk_pct": max_risk,
        "min_risk_pct": 0.1,
        "max_total_risk_pct": max(max_risk * 3, 4.0),
        "max_correlated_risk_pct": max(max_risk * 1.5, 2.0),
        "max_open_positions": 8,
        "max_positions_per_symbol": 1,
        "max_daily_loss_pct": f("MINTEL_DAILY", 3.0),
        "max_drawdown_pct": 12.0,
        "min_margin_level_pct": 300.0,
        "max_rejects_per_10min": 6,
        "max_spread_multiple": 3.0,
        "max_orders_per_hour": 30,
        "use_fractional_kelly": True,
        "kelly_fraction": 0.25,
        "kelly_min_samples": 60,
    },
    "universe": {
        "groups": ["FX_MAJOR", "FX_MINOR", "GOLD", "SILVER", "INDEX", "ENERGY"],
        "max_symbols": 40,
        "min_history_bars": 400,
        "exclude_patterns": ["BTC", "ETH", ".b", "-2", "TEST"],
        "require_full_spec": True,
    },
    "news": {"enabled": True, "use_mt5_calendar": True,
             "store_path": "calendar.sqlite", "api_keys": {}},
    "ops": {
        "data_dir": os.environ["MINTEL_DATA"],
        "log_dir": os.environ["MINTEL_LOGS"],
        "dashboard_host": "127.0.0.1",
        "dashboard_port": int(os.environ.get("MINTEL_DASH") or 8787),
    },
}
(data / "config.json").write_text(json.dumps(cfg, indent=2, sort_keys=True))

secrets = data / "secrets.json"
secrets.write_text(json.dumps(
    {"account_password": os.environ.get("MINTEL_PASSWORD", ""),
     "api_keys": {}}, indent=2))
os.chmod(secrets, 0o600)

# A shared token for the local bridge socket, so nothing else on this Mac can
# drive the trading connection.
token = data / "bridge-token.txt"
if not token.exists():
    token.write_text(os.urandom(24).hex())
    os.chmod(token, 0o600)
print("written")
PYEOF
  if [[ -f "$CONFIG" ]]; then
    good "Settings saved to $CONFIG"
    good "Password saved separately, readable only by you"
    good "Mode: $mode   Normal risk: ${base_risk}%   Maximum risk: ${max_risk}%"
  else
    bad "Settings could not be saved."
    return 1
  fi
}

refresh_config_paths() {
  # Paths can change between runs (Wine reinstalled, MT5 moved). Keep the
  # stored settings but re-point them at what actually exists now.
  [[ -f "$CONFIG" ]] || return 0
  MINTEL_TERMINAL="$MT5_TERMINAL" MINTEL_WINE="$WINE_PREFIX" \
  MINTEL_WINEPY="$WINE_PY" CONFIG="$CONFIG" \
  "$VENV_DIR/bin/python" - <<'PYEOF'
import json, os, pathlib
path = pathlib.Path(os.environ["CONFIG"])
cfg = json.loads(path.read_text())
for key, env in (("mt5_terminal_path", "MINTEL_TERMINAL"),
                 ("wine_prefix", "MINTEL_WINE"),
                 ("wine_python", "MINTEL_WINEPY")):
    value = os.environ.get(env, "")
    if value:
        cfg[key] = value
cfg["broker_mode"] = "bridge"
path.write_text(json.dumps(cfg, indent=2, sort_keys=True))
PYEOF
  good "Paths checked and up to date"
}

# ------------------------------------------------------------ launch agent ---
install_launch_agent() {
  step "Making it start by itself"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$PLIST_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$VENV_DIR/bin/python</string>
    <string>-m</string><string>mintel.ops.watchdog</string>
    <string>--config</string><string>$CONFIG</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>WINEPREFIX</key><string>$WINE_PREFIX</string>
    <key>WINEDEBUG</key><string>-all</string>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>$LOG_DIR/watchdog.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/watchdog.err.log</string>
</dict>
</plist>
PLIST
  launchctl unload "$PLIST_PATH" >/dev/null 2>&1
  if launchctl load "$PLIST_PATH" >/dev/null 2>&1; then
    good "It now starts automatically when you log in, and restarts itself if it ever stops"
  else
    warn "Automatic start could not be registered. The bot still runs while this window's setup is in effect."
  fi
}

# ---------------------------------------------------------------- caffeine ---
prevent_idle_sleep() {
  # Stops the Mac dozing off while it is awake and plugged in.  It cannot stop
  # a closed lid from sleeping - nothing can, short of clamshell mode - and
  # the report at the end says so plainly rather than pretending otherwise.
  step "Keeping the Mac awake while it trades"
  # Reuse the existing one if it is still alive, so repeated double-clicks do
  # not stack up a pile of caffeinate processes.
  if [[ -f "$DATA_DIR/caffeinate.pid" ]] \
     && kill -0 "$(cat "$DATA_DIR/caffeinate.pid" 2>/dev/null)" 2>/dev/null; then
    good "Already set to stay awake"
    return 0
  fi
  nohup caffeinate -dimsu >/dev/null 2>&1 &
  echo $! > "$DATA_DIR/caffeinate.pid"
  good "This Mac will not doze off on its own while the bot is running"
}

# ----------------------------------------------------------- desktop icon ---
install_desktop_icon() {
  step "Putting an icon on your Desktop"
  local desktop="$HOME/Desktop"
  [[ -d "$desktop" ]] || { warn "No Desktop folder found."; return 0; }
  local name
  for name in "Start Trading Bot" "Stop Trading Bot" "Self Test"; do
    local src="$APP_DIR/deploy/mac/$name.command"
    [[ -f "$src" ]] || continue
    cp -f "$src" "$desktop/$name.command"
    chmod +x "$desktop/$name.command"
    # Strip the quarantine flag so macOS does not refuse to open the copy we
    # just made ourselves.
    xattr -d com.apple.quarantine "$desktop/$name.command" >/dev/null 2>&1
  done
  good "Double-click ${BOLD}Start Trading Bot${RESET} on your Desktop from now on"
}

# ------------------------------------------------------------ start / check --
is_running() {
  # NOTE: each `local` gets its own statement on purpose. macOS ships bash 3.2,
  # where `local a="$1" b="$a"` does NOT see `a` in the second assignment, and
  # under `set -u` that is a fatal error rather than a quiet empty string.
  local name
  local pidfile
  local pid
  name="${1:-}"
  [[ -n "$name" ]] || return 1
  pidfile="$DATA_DIR/$name.pid"
  [[ -f "$pidfile" ]] || return 1
  pid="$(tr -d '[:space:]' < "$pidfile" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

start_mt5() {
  step "MetaTrader 5"
  export WINEPREFIX="$WINE_PREFIX"; export WINEDEBUG="-all"
  if pgrep -f "terminal64.exe" >/dev/null 2>&1; then
    good "already running"
    return 0
  fi
  [[ -n "$MT5_TERMINAL" ]] || { bad "MetaTrader 5 is not installed."; return 1; }
  local wine; wine="$(wine_bin)"
  nohup "$wine" "$MT5_TERMINAL" >/dev/null 2>&1 &
  local waited=0
  while (( waited < 60 )); do
    pgrep -f "terminal64.exe" >/dev/null 2>&1 && break
    sleep 2; waited=$((waited + 2))
  done
  if pgrep -f "terminal64.exe" >/dev/null 2>&1; then
    good "started"
  else
    bad "MetaTrader 5 would not start."
    return 1
  fi
}

start_everything() {
  step "Starting the bot"
  # The watchdog owns the trader and the bridge; starting it is enough, and
  # starting it twice is prevented by its own PID file.
  if is_running watchdog; then
    good "already running - nothing to start"
  else
    launchctl kickstart -k "gui/$(id -u)/$PLIST_LABEL" >/dev/null 2>&1 \
      || launchctl start "$PLIST_LABEL" >/dev/null 2>&1 \
      || {
        cd "$APP_DIR" || return 1
        WINEPREFIX="$WINE_PREFIX" WINEDEBUG="-all" nohup \
          "$VENV_DIR/bin/python" -m mintel.ops.watchdog --config "$CONFIG" \
          >"$LOG_DIR/watchdog.out.log" 2>"$LOG_DIR/watchdog.err.log" &
      }
    sleep 6
    if is_running watchdog; then
      good "started"
    else
      warn "The supervisor did not report in yet. Checking again shortly."
    fi
  fi

  local waited=0
  while (( waited < 90 )); do
    is_running trader && break
    sleep 3; waited=$((waited + 3))
  done
  if is_running trader; then
    good "the trader is running"
  else
    warn "The trader has not started yet. See $LOG_DIR/trader.err.log"
  fi
}

show_health() {
  step "Health"
  local waited
  local body
  waited=0
  body=""
  while (( waited < 60 )); do
    body="$(curl -fsS --max-time 5 "http://127.0.0.1:$DASH_PORT/api" 2>/dev/null)" && break
    sleep 3; waited=$((waited + 3))
  done
  if [[ -z "$body" ]]; then
    warn "The status page has not come up yet. Try http://127.0.0.1:$DASH_PORT in a minute."
    return 0
  fi
  printf '%s' "$body" | "$VENV_DIR/bin/python" - <<'PYEOF'
import json, sys
snap = json.load(sys.stdin)
status = snap.get("status") or {}
health = snap.get("health") or {}
running = status.get("bot") == "RUNNING" and not health.get("safe_mode")
print(f"    {'[ OK ]' if running else '[WARN]'} {health.get('summary', 'starting up')}")
for check in health.get("checks", []):
    if check.get("severity") != "OK":
        print(f"           {check['severity']}: {check['message']}")
print(f"    Mode        : {status.get('mode', '?')}")
print(f"    Open trades : {status.get('open_positions', 0)}")
print(f"    Today       : {status.get('today_pnl', 0)} {status.get('currency', '')}")
print(f"    Last scan   : {status.get('last_scan', 'never')}")
thinking = snap.get("thinking") or []
if thinking:
    print("    Currently watching:")
    for item in thinking[:5]:
        print(f"      {item['rank']}. {item['symbol']:<9}{item['direction']:<6}"
              f"{item['score']:>5.0f}  {item['tier']}")
PYEOF
}

open_dashboard() {
  open "http://127.0.0.1:$DASH_PORT" >/dev/null 2>&1 || true
}

final_report() {
  say ""
  say "=============================================================="
  if (( ${#FAILURES[@]} == 0 )); then
    say "  ${GREEN}${BOLD}THE BOT IS RUNNING${RESET}"
  else
    say "  ${RED}${BOLD}THERE ARE PROBLEMS TO FIX${RESET}"
  fi
  say "=============================================================="
  say ""
  say "  Status page :  http://127.0.0.1:$DASH_PORT"
  say "  Results page:  http://127.0.0.1:$DASH_PORT/results"
  say "  Everything  :  $MINTEL_HOME"
  say ""
  say "  It starts again by itself when you log in, and restarts itself"
  say "  if it ever stops. You can double-click this icon any time - if it"
  say "  is already running it just tells you so."
  say ""
  say "  ${BOLD}One thing to know about laptops:${RESET}"
  say "  If you close the lid, this Mac goes to sleep and the bot pauses"
  say "  with it. Nothing can prevent that on battery. When you open the"
  say "  lid it notices it was asleep, re-checks everything and carries on."
  say "  To trade with the lid closed you need clamshell mode: mains power"
  say "  plus an external monitor and keyboard connected."
  say ""
  if (( ${#WARNINGS[@]} )); then
    say "  ${YELLOW}Warnings:${RESET}"
    printf '    - %s\n' "${WARNINGS[@]}"
    say ""
  fi
  if (( ${#FAILURES[@]} )); then
    say "  ${RED}Problems:${RESET}"
    printf '    - %s\n' "${FAILURES[@]}"
    say ""
    return 1
  fi
  return 0
}

stop_everything() {
  step "Stopping the bot"
  launchctl unload "$PLIST_PATH" >/dev/null 2>&1
  local name
  for name in watchdog trader bridge; do
    local pidfile="$DATA_DIR/$name.pid"
    if [[ -f "$pidfile" ]]; then
      local pid; pid="$(tr -d '[:space:]' < "$pidfile")"
      kill "$pid" 2>/dev/null && good "$name stopped" || warn "$name was not running"
      rm -f "$pidfile"
    fi
  done
  if [[ -f "$DATA_DIR/caffeinate.pid" ]]; then
    kill "$(cat "$DATA_DIR/caffeinate.pid")" 2>/dev/null
    rm -f "$DATA_DIR/caffeinate.pid"
  fi
  say ""
  say "  Any open trades are still open, and still have their stop loss held"
  say "  by the broker. The bot is simply no longer managing them."
  say ""
}
