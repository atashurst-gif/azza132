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
USING_EXISTING_MT5=""     # "yes" once use_existing_mt5 has taken over
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
SETUP_LOG="$LOG_DIR/setup.log"

# run_logged "what it is" command args...
#
# Runs a long or fallible step with its output shown live AND appended to the
# setup log. The first version of this installer silenced these steps, and when
# Wine failed on a real Mac there was nothing to read - not for the user, not
# for anyone helping them. Live output also means a sudo or "press RETURN"
# prompt is visible instead of looking like a hang.
run_logged() {
  local what
  local code
  what="${1:-command}"
  shift
  mkdir -p "$(dirname "$SETUP_LOG")" 2>/dev/null
  printf '\n===== %s =====\n' "$what" >> "$SETUP_LOG"
  say "    ${DIM}--- $what ---${RESET}"
  "$@" 2>&1 | tee -a "$SETUP_LOG" | sed 's/^/        /'
  code=${PIPESTATUS[0]}
  if (( code != 0 )); then
    say "    ${RED}--- $what failed (exit $code). Full log: $SETUP_LOG ---${RESET}"
  fi
  return "$code"
}

# resolve_path PATH - follow symlinks (macOS has no readlink -f).
resolve_path() {
  local p
  local dir
  p="${1:-}"
  while [[ -L "$p" ]]; do
    dir="$(cd "$(dirname "$p")" && pwd -P)"
    p="$(readlink "$p")"
    [[ "$p" = /* ]] || p="$dir/$p"
  done
  printf '%s' "$p"
}

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
  if source_changed "$source_dir"; then
    CODE_CHANGED="yes"
  fi
  for item in mintel tests deploy tools pytest.ini requirements.txt \
              requirements-dev.txt README.md config; do
    if [[ -e "$source_dir/$item" ]]; then
      rm -rf "$APP_DIR/${item:?}"
      cp -R "$source_dir/$item" "$APP_DIR/"
    fi
  done
  mkdir -p "$DATA_DIR"
  code_hash "$source_dir" > "$DATA_DIR/app.version"
  if [[ -n "$CODE_CHANGED" ]]; then
    good "Program files installed to $APP_DIR (new version)"
  else
    good "Program files installed to $APP_DIR"
  fi
  BUILD_STAMP="$(cd "$APP_DIR" && "${NATIVE_PY:-python3}" -c 'from mintel.version import running_stamp; print(running_stamp())' 2>/dev/null || true)"
  if [[ -n "$BUILD_STAMP" ]]; then
    good "Build stamp: $BUILD_STAMP  (the status page must show this same code)"
  fi
}

# Fingerprint of the program's code, so a double-click can tell "same code,
# already running" from "new code copied in, a restart is needed".
code_hash() {
  local dir
  dir="${1:-}"
  ( cd "$dir" 2>/dev/null && find mintel deploy -type f \( -name '*.py' -o -name '*.sh' -o -name '*.command' \) -print0 2>/dev/null \
      | sort -z | xargs -0 cat 2>/dev/null | sha256_of /dev/stdin )
}

source_changed() {
  local dir
  local have
  dir="${1:-}"
  have="$(cat "$DATA_DIR/app.version" 2>/dev/null || true)"
  [[ -z "$have" ]] && return 0
  [[ "$(code_hash "$dir")" != "$have" ]]
}
CODE_CHANGED=""

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
    run_logged "Installing Python with Homebrew" brew install python@3.11
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
WINE_BIN=""
WINE_DIR=""
# The official WineHQ build for macOS, published on GitHub by its maintainer
# (the same packages Homebrew's wine casks installed). Pinned by version and
# checksum so what runs is exactly what was tested.
#
# Why the development build and not "stable": MetaTrader's installer and
# terminal refuse to run on Wine 10.3 through 11.0 ("A debugger has been found
# running in your system"); 11.0 is the current stable. The 11.x development
# releases carry the fix.
# (MINTEL_WINEHQ_URL / MINTEL_WINEHQ_SHA256 let an operator, or a test, pin a
# different build on purpose. Both must be set together.)
WINEHQ_VERSION="11.17"
WINEHQ_CHANNEL="devel"
WINEHQ_APP="Wine Devel.app"
WINEHQ_SHA256="${MINTEL_WINEHQ_SHA256:-c2b3a8274dbc594deaa64e40469b607cbc4aa8ef5656dec4c5f6f3dac0da770c}"
WINEHQ_URL="${MINTEL_WINEHQ_URL:-https://github.com/Gcenx/macOS_Wine_builds/releases/download/${WINEHQ_VERSION}/wine-${WINEHQ_CHANNEL}-${WINEHQ_VERSION}-osx64.tar.xz}"
WINE_APP="$MINTEL_HOME/$WINEHQ_APP"

# find_wine - locate a usable wine binary. Checks PATH first, then the app
# bundles the Homebrew casks install, because a cask can succeed in installing
# the app while its command-line symlink step does not.
find_wine() {
  local candidate
  local real
  for candidate in \
      "$WINE_APP/Contents/Resources/wine/bin/wine" \
      "$(command -v wine64 2>/dev/null || true)" \
      "$(command -v wine 2>/dev/null || true)" \
      "/Applications/Wine Stable.app/Contents/Resources/wine/bin/wine64" \
      "/Applications/Wine Stable.app/Contents/Resources/wine/bin/wine" \
      "/Applications/Wine Crossover.app/Contents/Resources/wine/bin/wine64" \
      "/Applications/Wine Crossover.app/Contents/Resources/wine/bin/wine" \
      "/Applications/Wine Staging.app/Contents/Resources/wine/bin/wine" \
      "/Applications/Wine Devel.app/Contents/Resources/wine/bin/wine" \
      "/opt/homebrew/bin/wine" \
      "/usr/local/bin/wine"; do
    [[ -n "$candidate" && -x "$candidate" ]] || continue
    real="$(resolve_path "$candidate")"
    WINE_BIN="$candidate"
    WINE_DIR="$(cd "$(dirname "$real")" && pwd -P)"
    return 0
  done
  return 1
}

wine_bin() {
  [[ -n "$WINE_BIN" ]] || find_wine || true
  printf '%s' "$WINE_BIN"
}

# wine_tool NAME - path to a wine helper (wineserver, wineboot) next to the
# real wine binary, or empty if the build does not ship it.
wine_tool() {
  local name
  name="${1:-}"
  [[ -n "$WINE_DIR" && -x "$WINE_DIR/$name" ]] && printf '%s' "$WINE_DIR/$name"
  return 0
}

# wine_wait - let Wine finish whatever it is doing (installers run detached).
wine_wait() {
  local ws
  local pid
  local waited
  # A running MetaTrader keeps Wine's server alive on purpose, so waiting for
  # it to go idle would wait forever. Give its files a moment and move on.
  if [[ -n "$USING_EXISTING_MT5" ]] || pgrep -f "terminal64.exe" >/dev/null 2>&1; then
    sleep 1
    return 0
  fi
  ws="$(wine_tool wineserver)"
  if [[ -z "$ws" ]]; then
    sleep 5
    return 0
  fi
  # Bounded: never let a lingering Wine process hang the whole setup.
  # Detached from our output so that nothing it leaves behind can hold the
  # setup window open either.
  "$ws" -w >/dev/null 2>&1 </dev/null &
  pid=$!
  waited=0
  while kill -0 "$pid" 2>/dev/null && (( waited < 30 )); do
    sleep 1
    waited=$((waited + 1))
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 0
}

# SHA-256 of a file, with whichever tool this Mac (or a test box) has.
sha256_of() {
  local f
  f="${1:-}"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$f" | cut -d' ' -f1
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" | cut -d' ' -f1
  else
    openssl dgst -sha256 "$f" | sed 's/^.*= //'
  fi
}

# Install Wine from WineHQ's own macOS package, into the bot's folder.
# No Homebrew, no admin password, no Gatekeeper: Homebrew disabled its Wine
# cask on 2026-09-01 (it no longer passes Homebrew's notarization policy),
# but the package itself is unchanged and runs fine once unpacked here.
install_wine_from_winehq() {
  local tarball
  local got
  tarball="$MINTEL_HOME/wine-${WINEHQ_CHANNEL}-${WINEHQ_VERSION}.tar.xz"
  if [[ -f "$tarball" ]] && [[ "$(sha256_of "$tarball")" != "$WINEHQ_SHA256" ]]; then
    rm -f "$tarball"
  fi
  if [[ ! -f "$tarball" ]]; then
    run_logged "Downloading Wine ${WINEHQ_VERSION} from WineHQ (about 185 MB)" \
      curl -fL --retry 3 --retry-delay 5 -o "$tarball" "$WINEHQ_URL" \
      || { rm -f "$tarball"; return 1; }
  fi
  got="$(sha256_of "$tarball")"
  if [[ "$got" != "$WINEHQ_SHA256" ]]; then
    # Never run a download that is not the one that was tested.
    bad "The Wine download did not match its checksum (got $got). Not using it."
    rm -f "$tarball"
    return 1
  fi
  good "Wine download verified (SHA-256 matches)"
  rm -rf "$WINE_APP"
  run_logged "Unpacking Wine into $MINTEL_HOME" tar -xJf "$tarball" -C "$MINTEL_HOME" \
    || return 1
  [[ -x "$WINE_APP/Contents/Resources/wine/bin/wine" ]] || {
    bad "Wine unpacked but $WINE_APP/Contents/Resources/wine/bin/wine is missing."
    return 1
  }
  xattr -dr com.apple.quarantine "$WINE_APP" >/dev/null 2>&1 || true
  # Older managed copies and downloads are dead weight now.
  local other
  for other in "$MINTEL_HOME"/Wine\ *.app "$MINTEL_HOME"/wine-*.tar.xz; do
    [[ -e "$other" ]] || continue
    [[ "$other" == "$WINE_APP" || "$other" == "$tarball" ]] && continue
    rm -rf "$other"
  done
  return 0
}

# Version of the Wine copy the installer manages ("" if there is none).
managed_wine_version() {
  local plist
  plist="$WINE_APP/Contents/Info.plist"
  [[ -f "$plist" ]] || return 0
  # the string on the line after the CFBundleShortVersionString key
  sed -n '/CFBundleShortVersionString/{n;s/.*<string>\(.*\)<\/string>.*/\1/p;}' "$plist"
}

# Install or upgrade the managed Wine copy so that it is exactly the pinned
# version. Returns 0 when a usable Wine is available afterwards.
ensure_wine_package() {
  local have
  have="$(managed_wine_version)"
  if [[ -n "$have" && "$have" != "$WINEHQ_VERSION" ]]; then
    say "    Wine $have is installed but MetaTrader needs Wine $WINEHQ_VERSION. Replacing it."
    install_wine_from_winehq || true
  elif [[ -z "$have" ]] && ! find_wine; then
    say "    Installing Wine. This is a large download; be patient."
    install_wine_from_winehq || true
  fi
  WINE_BIN=""
  find_wine
}

# Registry settings MetaTrader needs. Re-applied on every start because a
# Wine update rewrites its defaults.
#  - Windows 11: what MetaTrader's installer expects to see.
#  - No JIT debugger: MetaTrader's anti-debugging check treats a registered
#    debugger as one that is running, and an unattended bot has no use for a
#    crash-handler window anyway.
configure_prefix_for_mt5() {
  local w
  w="$(wine_bin)"
  [[ -n "$w" ]] || return 0
  run_logged "Telling Wine to present itself as Windows 11" \
    "$w" reg add 'HKCU\Software\Wine' /v Version /t REG_SZ /d win11 /f || true
  run_logged "Removing Wine's crash debugger hook (MetaTrader mistakes it for a debugger)" \
    "$w" reg delete 'HKLM\Software\Microsoft\Windows NT\CurrentVersion\AeDebug' /v Debugger /f || true
  "$w" reg delete 'HKLM\Software\Wow6432Node\Microsoft\Windows NT\CurrentVersion\AeDebug' /v Debugger /f >/dev/null 2>&1 || true
  wine_wait
}

# Gatekeeper refuses to run a freshly downloaded app until its quarantine
# flag is cleared. Homebrew used to do this for us with --no-quarantine; new
# versions no longer accept the flag, so it is done here for whichever Wine
# bundle is in use. Errors are ignored: a bundle that was never quarantined
# (or was installed some other way) has nothing to clear.
unquarantine_wine() {
  local real
  local bundle
  [[ -n "$WINE_BIN" ]] || return 0
  real="$(resolve_path "$WINE_BIN")"
  case "$real" in
    *.app/*)
      bundle="${real%%.app/*}.app"
      xattr -dr com.apple.quarantine "$bundle" >/dev/null 2>&1 || true
      ;;
  esac
  for bundle in "$WINE_APP" "/Applications/Wine Stable.app" "/Applications/Wine Crossover.app"; do
    [[ -d "$bundle" ]] && { xattr -dr com.apple.quarantine "$bundle" >/dev/null 2>&1 || true; }
  done
  return 0
}

# MetaQuotes' own MetaTrader 5 for Mac ships with a Wine that MetaTrader is
# built and tested against, and keeps its Windows environment here. Using
# it means no Wine download, no MetaTrader installer, and none of the
# "debugger has been found" trouble newer Wine releases cause MetaTrader.
MT5_APP="${MINTEL_MT5_APP:-/Applications/MetaTrader 5.app}"
MT5_APP_PREFIX="${MINTEL_MT5_PREFIX:-$HOME/Library/Application Support/net.metaquotes.wine.metatrader5}"

# Use the installed MetaTrader 5 app (Wine + environment) if it is complete.
# Sets WINE_BIN/WINE_DIR/WINE_PREFIX/MT5_TERMINAL and returns 0 when it is.
use_existing_mt5() {
  local w
  local term
  term="$MT5_APP_PREFIX/drive_c/Program Files/MetaTrader 5/terminal64.exe"
  [[ -f "$term" ]] || return 1
  for w in "$MT5_APP/Contents/SharedSupport/wine/bin/wine64" \
           "$MT5_APP/Contents/SharedSupport/wine/bin/wine"; do
    [[ -x "$w" ]] || continue
    WINE_BIN="$w"
    WINE_DIR="$(cd "$(dirname "$w")" && pwd -P)"
    WINE_PREFIX="$MT5_APP_PREFIX"
    MT5_TERMINAL='C:\Program Files\MetaTrader 5\terminal64.exe'
    USING_EXISTING_MT5="yes"
    return 0
  done
  return 1
}

ensure_wine() {
  step "Setting up Wine (this is what lets MetaTrader 5 run on a Mac)"
  if use_existing_mt5; then
    export WINEPREFIX="$WINE_PREFIX"
    export WINEDEBUG="-all"
    good "Using the MetaTrader 5 app you already have: $MT5_APP"
    good "Its Wine: $WINE_BIN"
    good "Its Windows environment: $WINE_PREFIX"
    if run_logged "Checking that Wine runs" "$WINE_BIN" --version; then
      # The separate Wine and environment from earlier runs are dead weight.
      rm -rf "$MINTEL_HOME"/Wine\ *.app "$MINTEL_HOME"/wine-*.tar.xz "$MINTEL_HOME/wine"
      return 0
    fi
    bad "That Wine did not start. Falling back to a separate Wine."
    USING_EXISTING_MT5=""
    WINE_BIN=""; WINE_DIR=""; WINE_PREFIX="$MINTEL_HOME/wine"; MT5_TERMINAL=""
  fi
  ensure_wine_standalone
}

# The original route: WineHQ's package plus MetaTrader's installer, both
# inside the bot's own folder. Only used when there is no MetaTrader 5 app.
ensure_wine_standalone() {
  step "Setting up Wine (this is what lets MetaTrader 5 run on a Mac)"
  # WineHQ's own package: no Homebrew and no password. (Homebrew's wine casks
  # are disabled since 2026-09-01, so Homebrew is only a fallback if that
  # download fails and it happens to be available.)
  ensure_wine_package || true
  if ! find_wine && ensure_homebrew; then
    say "    ${DIM}macOS may ask for your password. That is the installer, not the bot.${RESET}"
    run_logged "Installing wine-stable with Homebrew (fallback)" \
      brew install --cask wine-stable || true
    find_wine || true
  fi
  if find_wine; then
    good "Wine is installed: $WINE_BIN"
  else
    bad "Wine could not be installed. The output above says why; the full log is $SETUP_LOG"
    return 1
  fi
  unquarantine_wine
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  local stamp
  local made_with
  stamp="$WINE_PREFIX/.mintel-wine-version"
  made_with="$(cat "$stamp" 2>/dev/null || true)"
  if [[ ! -d "$WINE_PREFIX/drive_c" ]]; then
    say "    ${DIM}Creating the Windows environment (first time only)...${RESET}"
    # wineboot is a Wine builtin, so it runs through wine itself rather than
    # relying on a wineboot script being on PATH.
    WINEDLLOVERRIDES="mscoree,mshtml=" run_logged "Creating the Windows environment" \
      "$WINE_BIN" wineboot --init || true
    wine_wait
  elif [[ "$made_with" != "$WINEHQ_VERSION" ]]; then
    # A different Wine now owns this environment: let it update its files.
    WINEDLLOVERRIDES="mscoree,mshtml=" run_logged "Updating the Windows environment for Wine $WINEHQ_VERSION" \
      "$WINE_BIN" wineboot --update || true
    wine_wait
  fi
  if [[ -d "$WINE_PREFIX/drive_c" ]]; then
    printf '%s\n' "$WINEHQ_VERSION" > "$stamp"
    good "Windows environment ready at $WINE_PREFIX"
  else
    bad "The Windows environment could not be created. See $SETUP_LOG"
    return 1
  fi
  configure_prefix_for_mt5
}

WINE_PY='C:\Python311\python.exe'
WINE_PY_DIR=""
ensure_wine_python() {
  step "Installing Windows Python inside Wine"
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  WINE_PY_DIR="$WINE_PREFIX/drive_c/Python311"
  local wine
  wine="$(wine_bin)"
  if [[ -f "$WINE_PY_DIR/python.exe" ]]; then
    good "Windows Python already installed"
  else
    # The "embeddable" build: a plain zip, no MSI installer to go wrong under
    # Wine. It only needs site-packages switching on and pip bootstrapped.
    local zip
    zip="$MINTEL_HOME/python-win-embed.zip"
    if [[ ! -f "$zip" ]]; then
      run_logged "Downloading Windows Python" \
        curl -fL -o "$zip" \
        "https://www.python.org/ftp/python/3.11.9/python-3.11.9-embed-amd64.zip" \
        || { bad "Could not download Windows Python."; return 1; }
    fi
    mkdir -p "$WINE_PY_DIR"
    run_logged "Unpacking Windows Python" unzip -q -o "$zip" -d "$WINE_PY_DIR" \
      || { bad "Could not unpack Windows Python."; return 1; }
    # Enable site-packages (the embeddable build ships with it commented out).
    if [[ -f "$WINE_PY_DIR/python311._pth" ]]; then
      sed -i '' 's/^#import site/import site/' "$WINE_PY_DIR/python311._pth"
    fi
    if [[ -f "$WINE_PY_DIR/python.exe" ]]; then
      good "Windows Python installed"
    else
      bad "Windows Python did not unpack correctly."
      return 1
    fi
  fi

  # Windows Python only sees the paths its ._pth file lists, so name the
  # site-packages folder explicitly rather than trusting the site module.
  if [[ -f "$WINE_PY_DIR/python311._pth" ]] \
     && ! grep -q 'Lib\\site-packages' "$WINE_PY_DIR/python311._pth"; then
    printf 'Lib\\site-packages\n' >> "$WINE_PY_DIR/python311._pth"
  fi
  mkdir -p "$WINE_PY_DIR/Lib/site-packages"

  # tzdata: Windows Python has no time-zone database of its own, and the bot
  # needs one for session times. Check a real lookup, not just the import.
  if wine_py -c "import MetaTrader5, numpy, tzdata; from zoneinfo import ZoneInfo; ZoneInfo('Asia/Tokyo')" >/dev/null 2>&1; then
    good "MetaTrader5 package already working inside Wine"
  else
    # Fetch the Windows wheels with the Mac's own Python. That leaves nothing
    # for Wine to download, so a Wine networking quirk cannot stall the setup.
    local wheels
    wheels="$WINE_PREFIX/drive_c/wheels"
    mkdir -p "$wheels"
    run_logged "Downloading the MetaTrader5 package (Windows build)" \
      "$VENV_DIR/bin/python" -m pip download MetaTrader5 tzdata \
        --platform win_amd64 --python-version 3.11 --implementation cp \
        --only-binary=:all: --dest "$wheels" || true

    if ! wine_py -m pip --version >/dev/null 2>&1; then
      local getpip
      getpip="$WINE_PREFIX/drive_c/get-pip.py"
      if [[ ! -f "$getpip" ]]; then
        run_logged "Downloading pip" curl -fL -o "$getpip" "https://bootstrap.pypa.io/get-pip.py" || true
      fi
      # get-pip carries pip inside itself; this needs no network under Wine.
      [[ -f "$getpip" ]] && run_logged "Installing pip inside Wine" \
        "$wine" "$WINE_PY" 'C:\get-pip.py' --no-warn-script-location || true
      wine_wait
    fi

    if wine_py -m pip --version >/dev/null 2>&1; then
      if ls "$wheels"/[Mm]eta[Tt]rader5-*.whl >/dev/null 2>&1; then
        run_logged "Installing the MetaTrader5 package inside Wine (offline)" \
          "$wine" "$WINE_PY" -m pip install --no-warn-script-location \
            --no-index --find-links 'C:\wheels' MetaTrader5 tzdata || true
      else
        run_logged "Installing the MetaTrader5 package inside Wine" \
          "$wine" "$WINE_PY" -m pip install --no-warn-script-location MetaTrader5 tzdata || true
      fi
      wine_wait
    fi

    if ! wine_py -c "import MetaTrader5" >/dev/null 2>&1 \
       && ls "$wheels"/[Mm]eta[Tt]rader5-*.whl >/dev/null 2>&1; then
      # pip itself would not run under Wine. A wheel is a zip laid out exactly
      # as site-packages expects, so unpack the two we need straight in.
      local whl
      for whl in "$wheels"/*.whl; do
        run_logged "Unpacking $(basename "$whl") into Windows Python" \
          unzip -q -o "$whl" -d "$WINE_PY_DIR/Lib/site-packages" || true
      done
    fi

    # One package at a time, so a crash names its culprit.
    wine_py_imports tzdata || { bad "Windows Python cannot even load a pure-Python package. See $SETUP_LOG"; return 1; }
    if ! wine_py_imports numpy; then
      # The newest numpy can crash under an older Wine (MetaTrader's own
      # Wine is 8.0). numpy 1.26 is the last of the previous line and is
      # what most people run MetaTrader's Python package with under Wine.
      warn "The current numpy crashes under this Wine. Trying numpy 1.26 instead."
      run_logged "Downloading numpy 1.26 (Windows build)" \
        "$VENV_DIR/bin/python" -m pip download "numpy==1.26.4" \
          --platform win_amd64 --python-version 3.11 --implementation cp \
          --only-binary=:all: --dest "$wheels" || true
      if wine_py -m pip --version >/dev/null 2>&1; then
        run_logged "Installing numpy 1.26 inside Wine (offline)" \
          wine_py -m pip install --no-warn-script-location --no-index \
            --find-links 'C:\wheels' --force-reinstall --no-deps "numpy==1.26.4" || true
      else
        rm -rf "$WINE_PY_DIR/Lib/site-packages/numpy" "$WINE_PY_DIR/Lib/site-packages/numpy.libs" \
               "$WINE_PY_DIR"/Lib/site-packages/numpy-*.dist-info
        for whl in "$wheels"/numpy-1.26.4-*.whl; do
          [[ -f "$whl" ]] && run_logged "Unpacking $(basename "$whl") into Windows Python" \
            unzip -q -o "$whl" -d "$WINE_PY_DIR/Lib/site-packages" || true
        done
      fi
      wine_wait
      wine_py_imports numpy || { bad "numpy does not work inside this Wine, even the older one. See $SETUP_LOG"; return 1; }
    fi
    wine_py_imports MetaTrader5 || { bad "The MetaTrader5 package does not load inside this Wine. See $SETUP_LOG"; return 1; }
    if run_logged "Checking time zones inside Wine" \
         wine_py -c "from zoneinfo import ZoneInfo; print(ZoneInfo('Asia/Tokyo'))"; then
      good "MetaTrader5 package working inside Wine"
    else
      bad "Time zones do not work inside Wine. See $SETUP_LOG"
      return 1
    fi
  fi

  # The bridge is what the bot talks to. Prove Windows Python can load it
  # (and therefore the bot's own code) before anything depends on that.
  # Tried twice: on an upgrade the running watchdog may be replacing the old
  # bridge at this very moment, and its kill can catch this check (exit 143).
  if run_logged "Checking the bridge loads inside Wine" \
       wine_py "$(to_z_path "$APP_DIR/mintel/broker/bridge_server.py")" --help \
     || { sleep 3; run_logged "Checking the bridge loads inside Wine (again)" \
       wine_py "$(to_z_path "$APP_DIR/mintel/broker/bridge_server.py")" --help; }; then
    good "Bridge loads inside Wine"
  else
    bad "The bridge does not load inside Wine. The output above says why; full log: $SETUP_LOG"
    return 1
  fi
}

# Run Windows Python without Wine's "Program Error" popup: that dialog sits
# there until someone clicks it, which is a hang for an unattended setup.
# A crash then simply comes back as a non-zero exit status.
wine_py() {
  WINEDLLOVERRIDES="winedbg.exe=d" WINEDEBUG="-all" "$(wine_bin)" "$WINE_PY" "$@"
}

# Does `import <module>` work in Windows Python? Visible and logged.
wine_py_imports() {
  local mod
  mod="${1:-}"
  run_logged "Checking $mod inside Wine" wine_py -c "import $mod; print('$mod ok', getattr($mod, '__version__', ''))"
}

# A Mac path as Windows Python inside Wine sees it (Wine's Z: drive is /).
to_z_path() {
  printf 'Z:%s' "${1:-}" | tr '/' '\\'
}

# A path inside the prefix's drive_c as Windows sees it.
to_windows_path() {
  local p
  p="${1:-}"
  p="${p#$WINE_PREFIX/drive_c}"
  p="C:${p}"
  printf '%s' "$p" | tr '/' '\\'
}

ensure_mt5() {
  step "Installing MetaTrader 5"
  export WINEPREFIX="$WINE_PREFIX"
  export WINEDEBUG="-all"
  local wine
  local found
  wine="$(wine_bin)"
  if [[ -n "$USING_EXISTING_MT5" ]]; then
    good "MetaTrader 5 is already installed: $MT5_TERMINAL"
    good "Log in there with your account as usual; the bot will use that login."
    return 0
  fi
  found="$(find "$WINE_PREFIX/drive_c" -maxdepth 6 -name terminal64.exe 2>/dev/null | head -1)"
  if [[ -n "$found" ]]; then
    MT5_TERMINAL="$(to_windows_path "$found")"
    good "MetaTrader 5 found: $MT5_TERMINAL"
    return 0
  fi
  local installer
  installer="$MINTEL_HOME/mt5setup.exe"
  if [[ ! -f "$installer" ]]; then
    run_logged "Downloading MetaTrader 5" curl -fL -o "$installer" \
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
  run_logged "Running the MetaTrader 5 installer" "$wine" "$installer" || true
  wine_wait
  found="$(find "$WINE_PREFIX/drive_c" -maxdepth 6 -name terminal64.exe 2>/dev/null | head -1)"
  if [[ -n "$found" ]]; then
    MT5_TERMINAL="$(to_windows_path "$found")"
    good "MetaTrader 5 installed: $MT5_TERMINAL"
  else
    bad "MetaTrader 5 was not found after installation. See $SETUP_LOG"
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
  MINTEL_WINEBIN="$WINE_BIN" \
  MINTEL_TERMINAL="$MT5_TERMINAL" MINTEL_DASH="$DASH_PORT" \
  MINTEL_BRIDGE="$BRIDGE_PORT" MINTEL_PASSWORD="$password" \
  "$VENV_DIR/bin/python" - <<'PYEOF'
import json, os, pathlib, sys
sys.path.insert(0, os.environ.get("MINTEL_APP", "."))
data = pathlib.Path(os.environ["MINTEL_DATA"])
data.mkdir(parents=True, exist_ok=True)

def f(name, default):
    # "3", "3%", " 3 % " all mean 3 percent. Anything else falls back to
    # the default, and the shell side reports which value was actually used.
    raw = (os.environ.get(name) or "").replace("%", "").strip()
    try:
        return float(raw or default)
    except ValueError:
        return float(default)

max_risk = f("MINTEL_MAX", 1.5)
cfg = {
    "version": 1,
    "mode": os.environ["MINTEL_MODE"],
    "live_marker": os.environ.get("MINTEL_MARKER", ""),
    "magic": 990311,
    "tracking_start_utc": __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc).replace(microsecond=0).isoformat(),
    "account_login": int(os.environ.get("MINTEL_LOGIN") or 0),
    "account_server": os.environ.get("MINTEL_SERVER", ""),
    "mt5_terminal_path": os.environ.get("MINTEL_TERMINAL", ""),
    "broker_mode": "bridge",
    "bridge_host": "127.0.0.1",
    "bridge_port": int(os.environ.get("MINTEL_BRIDGE") or 8790),
    "wine_prefix": os.environ.get("MINTEL_WINE", ""),
    "wine_python": os.environ.get("MINTEL_WINEPY", ""),
    "wine_binary": os.environ.get("MINTEL_WINEBIN", ""),
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
# Re-entering the settings must never move the measuring clock or drop the
# other bots' sections: carry them over from the file that is already there.
# (7 Oct: answering "y" to "Change them?" restarted the page's count from now.)
existing_path = data / "config.json"
if existing_path.exists():
    try:
        old = json.loads(existing_path.read_text())
    except Exception:
        old = {}
    if isinstance(old, dict):
        # (The Rapid Momentum Rider and Financial Ian keep their own files,
        # data/rider.json and data/ian.json, which nothing here rewrites; a
        # "rider" or "ian" block put in config.json by hand is carried too.
        # "tnb" is Trend & Breakout's: LIVE or PAPER and its paper settings.)
        for key in ("tracking_start_utc", "tracking_strategy", "account_reset_utc", "account_reset_balance",
                    "runner", "bandbreaker", "crowd", "rider", "ian", "tnb"):
            if key in old:
                cfg[key] = old[key]
(data / "config.json").write_text(json.dumps(cfg, indent=2, sort_keys=True))

# The secrets file also holds the GitHub token the reports and the ten-minute
# pulse publish with. Re-entering the settings must keep everything that is
# already there and only replace the password that was just typed.
# (7 Oct: answering "y" to "Change them?" dropped the token, and the Mac went
# silent for the rest of the day while the bot itself ran fine.)
secrets = data / "secrets.json"
old_secrets = {}
if secrets.exists():
    try:
        old_secrets = json.loads(secrets.read_text())
    except Exception:
        old_secrets = {}
    if not isinstance(old_secrets, dict):
        old_secrets = {}
new_secrets = dict(old_secrets)
typed = os.environ.get("MINTEL_PASSWORD", "")
if typed or not new_secrets.get("account_password"):
    new_secrets["account_password"] = typed
new_secrets.setdefault("api_keys", {})
secrets.write_text(json.dumps(new_secrets, indent=2))
os.chmod(secrets, 0o600)
if not new_secrets.get("github_token"):
    print("NOTE: no GitHub token saved - the nightly report and the ten-minute pulse will not publish "
          "until you run: python -m mintel.ops.report_upload --config <data>/config.json --set-token")
if not new_secrets.get("coinversa_api_key"):
    print("NOTE: no Coinversa key saved - the Crowd Fader will show NO KEY until you run: "
          "python -m mintel.crowd --config <data>/config.json --set-key")
if not new_secrets.get("databento_api_key"):
    print("NOTE: no Databento key saved - Financial Ian stays DATA-DEGRADED (no signals, no trades) until you "
          "double-click INSTALL-FINANCIAL-IAN and paste one")

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
    # Report what was saved, not what was typed.
    local saved
    saved="$("$VENV_DIR/bin/python" -c "import json,sys; c=json.load(open(sys.argv[1]))['risk']; print(f\"Normal risk: {c['base_risk_pct']:g}%   Maximum risk: {c['max_risk_pct']:g}%   Daily stop: {c['max_daily_loss_pct']:g}%\")" "$CONFIG" 2>/dev/null || true)"
    good "Mode: $mode   ${saved:-Normal risk: ${base_risk}%   Maximum risk: ${max_risk}%}"
  else
    bad "Settings could not be saved."
    return 1
  fi
}

apply_bot_modes() {
  # 8 Oct: Aaron's instruction - every bot LIVE on the demo account. Done once
  # by the installer (a marker file records it), so a later choice to put a
  # bot back to PAPER with the modes command is never undone by a reinstall.
  local marker="$DATA_DIR/.all-bots-live-2026-10-08"
  [[ -f "$marker" ]] && return 0
  [[ -f "$CONFIG" ]] || return 0
  if lineup_done; then                       # superseded by the 9 Oct line-up: never undo it
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    return 0
  fi
  step "Switching every bot to LIVE"
  if (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --live runner bandbreaker crowd 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /'; then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    CODE_CHANGED="yes"                       # the bots read their mode at start: restart them
    good "Trend & Breakout, Momentum Runner, Band Breaker and Crowd Fader trade LIVE"
  else
    warn "Could not switch the bots to LIVE. Run: cd $APP_DIR && $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --live all"
  fi
  return 0
}

apply_scalper_paper() {
  # 8 Oct afternoon: Aaron's instruction was the Rapid Scalper back to PAPER.
  # Superseded on 9 Oct: the scalper is retired (apply_rider_live switches it
  # OFF and the watchdog no longer starts it), and the modes command refuses
  # PAPER for it. Only the marker is kept, so the quick check of a running
  # bot still finds it.
  local marker="$DATA_DIR/.scalper-paper-2026-10-08"
  if [[ -f "$marker" || ! -f "$CONFIG" ]]; then return 0; fi
  date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
  return 0
}

apply_rider_live() {
  # 9 Oct: Aaron's instruction - the Rapid Momentum Rider replaces the Rapid
  # Scalper on the demo account: the scalper is retired (scalper.json OFF;
  # the watchdog no longer starts it) and the Rider trades LIVE at GBP 1 a
  # pip. Once only (marker), like the switches above, so a later choice made
  # with the modes command is never undone by a reinstall.
  local marker="$DATA_DIR/.rider-live-2026-10-09"
  [[ -f "$marker" ]] && return 0
  [[ -f "$CONFIG" ]] || return 0
  if lineup_done; then                       # the line-up already did this (and more): never undo it
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    return 0
  fi
  step "The Rapid Momentum Rider replaces the Rapid Scalper"
  if (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --off scalper --live rider --gbp-per-pip 1.0 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /'; then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    CODE_CHANGED="yes"                       # the bots read their mode at start: restart them
    good "Rapid Scalper: retired (OFF). Rapid Momentum Rider: LIVE at GBP 1.00 a pip"
  else
    warn "Could not switch the Rapid Momentum Rider on. Run: cd $APP_DIR && $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --off scalper --live rider --gbp-per-pip 1.0"
  fi
  return 0
}

apply_ian_enabled() {
  # 9 Oct: Financial Ian joins in PAPER. Without an institutional data-feed
  # key it runs DATA-DEGRADED - no signals, no trades - and says so on its
  # page. A LIVE choice already made is left alone. Once only (marker).
  local marker="$DATA_DIR/.ian-enabled-2026-10-09"
  [[ -f "$marker" ]] && return 0
  [[ -f "$CONFIG" ]] || return 0
  if lineup_done; then                       # the line-up set Ian already: never undo it
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    return 0
  fi
  step "Financial Ian"
  if [[ "$(bot_file_mode ian)" == "LIVE" ]]; then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    good "Financial Ian: LIVE (left as it is)"
    return 0
  fi
  if (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --paper ian 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /'; then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    CODE_CHANGED="yes"
    good "Financial Ian: PAPER. With no data-feed key it stays DATA-DEGRADED: no signals, no trades (INSTALL-FINANCIAL-IAN adds one)"
  else
    warn "Could not switch Financial Ian on. Run: cd $APP_DIR && $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --paper ian"
  fi
  return 0
}

LINEUP_MARKER_NAME=".lineup-2026-10-09"

# lineup_done - has the 9 Oct line-up been set on this Mac? (its marker)
lineup_done() {
  [[ -f "$DATA_DIR/$LINEUP_MARKER_NAME" ]]
}

# rider_pip_value - the Rapid Momentum Rider's GBP per pip from
# data/rider.json, as 1.00; empty when the file names no figure.
rider_pip_value() {
  [[ -x "$VENV_DIR/bin/python" && -f "$DATA_DIR/rider.json" ]] || return 0
  "$VENV_DIR/bin/python" -c 'import json, sys
try:
    v = json.load(open(sys.argv[1])).get("user_pip_value_gbp")
    print("%.2f" % float(v) if v else "")
except Exception:
    print("")' "$DATA_DIR/rider.json" 2>/dev/null || true
}

# apply_lineup - 9 Oct evening, Aaron's decision. LIVE: the Momentum Runner
# (the winner), the Rapid Momentum Rider (GBP 1.00 a pip) and Financial Ian
# (it can only trade once a CME data feed is configured). PAPER: Trend &
# Breakout (real prices, simulated orders; the Momentum Runner still rides
# its index entries), the Band Breaker and the Crowd Fader. The retired
# Rapid Scalper stays OFF. It runs after the one-time steps above, so on a
# fresh install it has the last word; once only (marker), so a later
# choice made with the modes command is never undone by a reinstall, and
# once it is set the older one-time steps never run again. The Rider's
# figure is set to GBP 1.00 only when rider.json names none (once set, the
# figure is Aaron's).
apply_lineup() {
  local marker="$DATA_DIR/$LINEUP_MARKER_NAME"
  [[ -f "$marker" ]] && return 0
  [[ -f "$CONFIG" ]] || return 0
  step "The bots' line-up"
  local ok
  ok=1
  if [[ -n "$(rider_pip_value)" ]]; then
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --off scalper \
        --paper tnb bandbreaker crowd --live runner rider ian 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /' || ok=0
  else
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --off scalper \
        --paper tnb bandbreaker crowd --live runner rider ian --gbp-per-pip 1.0 2>&1) \
      | sed -e '/Restart the bot/d' -e 's/^/  /' || ok=0
  fi
  if (( ok )); then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
    CODE_CHANGED="yes"                       # the bots read their mode at start: restart them
    good "The line-up is set:"
    print_lineup
  else
    warn "Could not set the line-up. Run: cd $APP_DIR && $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --paper tnb bandbreaker crowd --live runner rider ian"
  fi
  return 0
}

# apply_ian_binance - 9 Oct, Aaron: "I don't have the Databento key, just do
# anything closest so we can get it live". Unless a Databento key is already
# saved (then the feed is the CME FX book through Databento - set now if
# ian.json did not name it yet), Financial Ian's feed becomes Binance's
# PUBLIC crypto order book (no account, no key) and it trades BTC and ETH
# through the broker's crypto CFDs; Ian stays LIVE. Only data/ian.json is
# touched - never another bot's mode. The websocket-client package is added
# when it is missing (Ian has a built-in client if that fails). Runs after
# the line-up; once only (marker), so a later choice is never undone.
apply_ian_binance() {
  local marker="$DATA_DIR/.ian-binance-2026-10-09"
  [[ -f "$marker" ]] && return 0
  [[ -f "$CONFIG" ]] || return 0
  step "Financial Ian's data feed"
  local feed
  feed="$(cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ian --config "$CONFIG" --free-feed 2>/dev/null | tail -1)"
  if [[ "$feed" != "binance" && "$feed" != "cme" ]]; then
    warn "Could not set Financial Ian's data feed. Set \"feed\": {\"vendor\": \"binance\"} in $DATA_DIR/ian.json"
    return 0
  fi
  if [[ "$(bot_file_mode ian)" != "LIVE" ]]; then
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --live ian 2>&1) \
      | sed -e '/Restart the bot/d' -e 's/^/  /' \
      || warn "Could not switch Financial Ian to LIVE. Run: cd $APP_DIR && $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --live ian"
  fi
  if [[ "$feed" == "binance" ]] && [[ -z "${MINTEL_NO_PIP:-}" ]] \
     && ! "$VENV_DIR/bin/python" -c 'import websocket' >/dev/null 2>&1; then
    "$VENV_DIR/bin/python" -m pip install --quiet --disable-pip-version-check --retries 1 --timeout 30 \
        "websocket-client>=1.6,<2" >/dev/null 2>&1 \
      && good "WebSocket package installed for the live stream" \
      || say "    The WebSocket package did not install: Ian uses its own built-in WebSocket client instead."
  fi
  date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"
  CODE_CHANGED="yes"                         # Ian reads its feed at start: restart it
  if [[ "$feed" == "binance" ]]; then
    good "Financial Ian: $(bot_file_mode ian) on Binance's public crypto order book, trading BTC and ETH through IC Markets CFDs (no key needed). The CME FX feed can be added later."
  else
    good "Financial Ian: $(bot_file_mode ian), its feed set to the CME FX futures book through Databento (a Databento key is saved)."
  fi
  return 0
}

# print_lineup - which bots are LIVE and which are PAPER, in plain English,
# read back from the files the bots themselves read.
print_lineup() {
  local entry
  local key
  local label
  local mode
  local pip
  local live=""
  local paper=""
  local off=""
  pip="$(rider_pip_value)"
  for entry in "tnb|Trend & Breakout" "runner|Momentum Runner" "rider|Rapid Momentum Rider" \
               "bandbreaker|Band Breaker" "crowd|Crowd Fader" "ian|Financial Ian"; do
    key="${entry%%|*}"
    label="${entry#*|}"
    case "$key" in
      rider|ian) mode="$(bot_file_mode "$key")" ;;
      tnb) mode="$(bot_mode tnb LIVE)" ;;
      *) mode="$(bot_mode "$key")" ;;
    esac
    if [[ "$key" == "rider" && -n "$pip" ]]; then
      label="$label (GBP $pip a pip)"
    fi
    case "$mode" in
      LIVE) live="${live:+$live, }$label" ;;
      PAPER) paper="${paper:+$paper, }$label" ;;
      *) off="${off:+$off, }$label" ;;
    esac
  done
  say "    LIVE  - real orders on the account: ${live:-none}"
  say "    PAPER - real prices, simulated orders, never in the account: ${paper:-none}"
  if [[ -n "$off" ]]; then
    say "    OFF   - not started: $off"
  fi
  case "$(bot_mode runner)" in
    LIVE) say "    The Momentum Runner rides Trend & Breakout's index entries with its own real orders." ;;
    PAPER) say "    The Momentum Runner rides Trend & Breakout's index entries on paper." ;;
  esac
  say "    The Rapid Scalper stays retired (OFF)."
  if [[ "$(bot_file_mode ian)" == "LIVE" ]]; then
    say "    Financial Ian can only trade once a CME data feed is configured (INSTALL-FINANCIAL-IAN)."
  fi
}

# bot_file_mode NAME - the mode in a bot's own file in the data folder
# (data/rider.json, data/ian.json): LIVE, PAPER or OFF, PAPER when the file
# or the mode is missing or not readable (the bots' own default).
bot_file_mode() {
  local name="${1:-}"
  local py=""
  local mode=""
  if [[ -x "$VENV_DIR/bin/python" ]]; then
    py="$VENV_DIR/bin/python"
  else
    py="$(command -v python3 2>/dev/null || true)"
  fi
  if [[ -n "$py" && -n "$name" && -f "$DATA_DIR/$name.json" ]]; then
    mode="$("$py" -c 'import json, sys
try:
    m = str(json.load(open(sys.argv[1])).get("mode") or "").strip().upper()
    print(m if m in ("OFF", "PAPER", "LIVE") else "PAPER")
except Exception:
    print("PAPER")' "$DATA_DIR/$name.json" 2>/dev/null || true)"
  fi
  printf '%s\n' "${mode:-PAPER}"
}

# bot_check NAME - one plain line on the Rapid Momentum Rider (rider) or
# Financial Ian (ian), from its own status file. Exit 0: running and
# reporting; 2: its file says OFF; 1: anything else.
bot_check() {
  local name="${1:-}"
  (cd "$APP_DIR" 2>/dev/null && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --check "$name" 2>&1)
}

# wait_bot_line NAME SECONDS - wait (at most SECONDS) for the bot to report
# in its current mode, then print its one line.
wait_bot_line() {
  local name="${1:-}"
  local limit="${2:-120}"
  local waited=0
  local line=""
  local code=1
  while :; do
    line="$(bot_check "$name")"
    code=$?
    if (( code == 2 )); then
      break
    fi
    if (( code == 0 )) && [[ "$line" != *"restart it for"* ]]; then
      break
    fi
    if (( waited >= limit )); then
      break
    fi
    sleep 5
    waited=$((waited + 5))
  done
  printf '%s\n' "${line:-$name: no answer yet}"
  return "$code"
}

# restart_if_mode_changed NAME - a running bot reads its mode at start: when
# its file now says another mode, stop that process and let the watchdog
# start it again (within a check) in the new mode. Nothing else is touched.
restart_if_mode_changed() {
  local name
  local line
  name="${1:-}"
  line="$(bot_check "$name")"
  case "$line" in
    *"restart it for"*)
      if is_running "$name"; then
        say "    Restarting $name so it runs in its new mode (the watchdog starts it again)."
        kill "$(tr -d '[:space:]' < "$DATA_DIR/$name.pid")" 2>/dev/null || true
        sleep 3
      fi
      ;;
  esac
  return 0
}

# setup_rider - the Rapid Momentum Rider LIVE in place of the Rapid Scalper.
# GBP 1 a pip only when rider.json names no figure yet: once set, the figure
# is Aaron's and is never changed here.
setup_rider() {
  step "Rapid Momentum Rider: LIVE, in place of the Rapid Scalper"
  local has_pip
  has_pip="$("$VENV_DIR/bin/python" -c 'import json, sys
try:
    print("yes" if json.load(open(sys.argv[1])).get("user_pip_value_gbp") else "")
except Exception:
    print("")' "$DATA_DIR/rider.json" 2>/dev/null || true)"
  local ok
  ok=1
  if [[ -n "$has_pip" ]]; then
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --off scalper --live rider 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /' || ok=0
  else
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --off scalper --live rider --gbp-per-pip 1.0 2>&1) | sed -e '/Restart the bot/d' -e 's/^/  /' || ok=0
  fi
  if (( ok )); then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$DATA_DIR/.rider-live-2026-10-09"
    good "Rapid Momentum Rider: LIVE. Rapid Scalper: retired"
    return 0
  fi
  bad "The Rapid Momentum Rider could not be switched on (see the lines above)."
  return 1
}

# setup_ian - Financial Ian on (PAPER unless it is already LIVE), with an
# optional Databento key asked for ONCE and never shown. The databento
# package is installed only when a key is saved.
setup_ian() {
  step "Financial Ian"
  say "    Financial Ian reads the CME futures order book. That needs a data-feed key"
  say "    (Databento: see docs/ops/financial_ian_data_feed.md). Without one it runs"
  say "    safely in DATA-DEGRADED mode: no signals and no trades."
  say ""
  say "    ${BOLD}Paste your Databento API key, or just press Enter to skip.${RESET} Nothing is shown as you type."
  (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ian --config "$CONFIG" --set-key 2>&1) | sed 's/^/    /'
  local has_key
  has_key="$("$VENV_DIR/bin/python" -c 'import json, sys
try:
    print("yes" if json.load(open(sys.argv[1])).get("databento_api_key") else "")
except Exception:
    print("")' "$SECRETS" 2>/dev/null || true)"
  if [[ -n "$has_key" ]]; then
    good "A Databento key is saved in $SECRETS (readable only by you, never in the code)"
    chmod 600 "$SECRETS" 2>/dev/null || true
    if "$VENV_DIR/bin/python" -c 'import databento' >/dev/null 2>&1; then
      good "The databento package is installed"
    else
      run_logged "Installing the databento package" "$VENV_DIR/bin/python" -m pip install databento \
        || warn "The databento package did not install: Financial Ian will say the feed is DOWN until it does."
    fi
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --ian-feed databento 2>&1) \
      | sed -e '/Restart the bot/d' -e 's/^/  /' || warn "Could not name the Databento feed in ian.json."
  else
    say "    No key: Financial Ian stays DATA-DEGRADED (no signals, no trades). Double-click this again any time."
  fi
  if [[ "$(bot_file_mode ian)" != "LIVE" ]]; then
    (cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" --paper ian 2>&1) \
      | sed -e '/Restart the bot/d' -e 's/^/  /' || { bad "Financial Ian could not be switched on."; return 1; }
  fi
  date -u +%Y-%m-%dT%H:%M:%SZ > "$DATA_DIR/.ian-enabled-2026-10-09"
  good "Financial Ian: $(bot_file_mode ian)"
  return 0
}

# start_rider_research - the Rapid Momentum Rider's research on the last ten
# days of the broker's REAL ticks, run in the background once a day (marker)
# and uploaded to GitHub, so real-tick results arrive without anyone typing
# anything. Skipped when today's summary is already there. It must never
# starve the trading bots: it runs at the lowest priority (nice -n 19) on
# one worker process (--workers 1), logging to logs/rider-research.log.
start_rider_research() {
  local today
  today="$(date -u +%Y-%m-%d)"
  [[ -x "$VENV_DIR/bin/python" && -f "$CONFIG" ]] || return 0
  if [[ ! -f "$APP_DIR/mintel/rider/research.py" && ! -f "$APP_DIR/mintel/rider/research/__main__.py" ]]; then
    return 0                                 # not in this version
  fi
  # 9 Oct: until the time fix, MetaTrader answered every past-tick request with
  # the hour before, so the stored ticks are empty and that day's report says
  # INSUFFICIENT DATA. Once, throw those ticks away and let today run again.
  if [[ ! -f "$DATA_DIR/.rider-ticks-refetch-2026-10-09" ]]; then
    rm -rf "$DATA_DIR/rider-research/ticks"
    rm -f "$DATA_DIR/.rider-research-$today" "$DATA_DIR/rider-research/$today/summary.json"
    : > "$DATA_DIR/.rider-ticks-refetch-2026-10-09"
  fi
  [[ -f "$DATA_DIR/rider-research/$today/summary.json" ]] && return 0
  [[ -f "$DATA_DIR/.rider-research-$today" ]] && return 0
  if pgrep -f "mintel.rider.research" >/dev/null 2>&1; then
    return 0                                 # one is already running (started by hand)
  fi
  mkdir -p "$LOG_DIR"
  : > "$DATA_DIR/.rider-research-$today"
  (cd "$APP_DIR" && nohup nice -n 19 "$VENV_DIR/bin/python" -m mintel.rider.research --config "$CONFIG" --days 10 \
      --upload --workers 1 >>"$LOG_DIR/rider-research.log" 2>&1 &)
  good "Rapid Momentum Rider research on the last 10 days of real ticks started in the background, at the lowest priority so the bots come first (log: $LOG_DIR/rider-research.log)"
  return 0
}

refresh_config_paths() {
  # Paths can change between runs (Wine reinstalled, MT5 moved). Keep the
  # stored settings but re-point them at what actually exists now.
  [[ -f "$CONFIG" ]] || return 0
  MINTEL_TERMINAL="$MT5_TERMINAL" MINTEL_WINE="$WINE_PREFIX" \
  MINTEL_WINEPY="$WINE_PY" MINTEL_WINEBIN="$WINE_BIN" CONFIG="$CONFIG" \
  "$VENV_DIR/bin/python" - <<'PYEOF'
import json, os, pathlib
path = pathlib.Path(os.environ["CONFIG"])
cfg = json.loads(path.read_text())
for key, env in (("mt5_terminal_path", "MINTEL_TERMINAL"),
                 ("wine_prefix", "MINTEL_WINE"),
                 ("wine_python", "MINTEL_WINEPY"),
                 ("wine_binary", "MINTEL_WINEBIN")):
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
    <key>WINEDLLOVERRIDES</key><string>mscoree,mshtml=</string>
    <key>PATH</key><string>${WINE_DIR:+$WINE_DIR:}/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
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
  install_report_agent
  if launchctl load "$PLIST_PATH" >/dev/null 2>&1; then
    good "It now starts automatically when you log in, and restarts itself if it ever stops"
  else
    warn "Automatic start could not be registered. The bot still runs while this window's setup is in effect."
  fi
}

install_report_agent() {
  # Publishes the day's record every evening at 22:10 local time, ten
  # minutes after the broker's day closes (00:00 server = 22:00 UK summer).
  local label
  local plist
  label="com.mintel.report"
  plist="$HOME/Library/LaunchAgents/$label.plist"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$VENV_DIR/bin/python</string>
    <string>-m</string><string>mintel.ops.report_upload</string>
    <string>--config</string><string>$CONFIG</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>22</integer><key>Minute</key><integer>10</integer></dict>
  <key>StandardOutPath</key><string>$LOG_DIR/report.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/report.err.log</string>
</dict>
</plist>
PLIST
  launchctl unload "$plist" >/dev/null 2>&1
  launchctl load "$plist" >/dev/null 2>&1 || true
  install_pulse_agent
}

install_pulse_agent() {
  # Every ten minutes, whether or not the bot is alive: one line in
  # data/uptime.log saying UP or DOWN (and why), published to the reports
  # branch.  This is how an outage becomes a number in the day's review
  # instead of a surprise the next morning.
  local label
  local plist
  label="com.mintel.pulse"
  plist="$HOME/Library/LaunchAgents/$label.plist"
  cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$VENV_DIR/bin/python</string>
    <string>-m</string><string>mintel.ops.pulse</string>
    <string>--config</string><string>$CONFIG</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>600</integer>
  <key>StandardOutPath</key><string>$LOG_DIR/pulse.out.log</string>
  <key>StandardErrorPath</key><string>$LOG_DIR/pulse.err.log</string>
</dict>
</plist>
PLIST
  launchctl unload "$plist" >/dev/null 2>&1
  launchctl load "$plist" >/dev/null 2>&1 || true
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
  # Only ever on the first install. After that the icons are the user's to
  # move, file away or delete; nothing needs them (the bot runs from
  # $APP_DIR), so an update must never put them back.
  local marker="$DATA_DIR/.desktop-icons-placed"
  if [[ -f "$marker" ]]; then
    install_new_bot_icons "$desktop"
    install_test_ian_icon "$desktop"
    good "Desktop icons: left as you arranged them (the bot does not need them)"
    return 0
  fi
  local name
  for name in "Start Trading Bot" "Stop Trading Bot" "Self Test" "Day Review" "Send Report"; do
    local src="$APP_DIR/deploy/mac/$name.command"
    [[ -f "$src" ]] || continue
    cp -f "$src" "$desktop/$name.command"
    chmod +x "$desktop/$name.command"
    # Strip the quarantine flag so macOS does not refuse to open the copy we
    # just made ourselves.
    xattr -d com.apple.quarantine "$desktop/$name.command" >/dev/null 2>&1
  done
  mkdir -p "$DATA_DIR" && : > "$marker"
  install_new_bot_icons "$desktop"
  install_test_ian_icon "$desktop"
  good "Double-click ${BOLD}Start Trading Bot${RESET} on your Desktop from now on"
}

# The Rapid Momentum Rider's and Financial Ian's own icons (9 Oct), placed
# once like the first ones, and never put back after that.
install_new_bot_icons() {
  local desktop
  local marker
  desktop="${1:-$HOME/Desktop}"
  marker="$DATA_DIR/.desktop-icons-rider-ian-placed"
  [[ -f "$marker" ]] && return 0
  [[ -d "$desktop" ]] || return 0
  local name
  for name in "SETUP-RAPID-RIDER" "START-RAPID-RIDER" "INSTALL-FINANCIAL-IAN" "START-FINANCIAL-IAN"; do
    local src="$APP_DIR/deploy/mac/$name.command"
    [[ -f "$src" ]] || continue
    cp -f "$src" "$desktop/$name.command"
    chmod +x "$desktop/$name.command"
    xattr -d com.apple.quarantine "$desktop/$name.command" >/dev/null 2>&1
  done
  mkdir -p "$DATA_DIR" && : > "$marker"
  good "Rapid Momentum Rider and Financial Ian icons placed on your Desktop"
}

# Financial Ian's test on past CME data (9 Oct): its own icon, placed once.
install_test_ian_icon() {
  local desktop
  local marker
  local ian_test_icon
  desktop="${1:-$HOME/Desktop}"
  marker="$DATA_DIR/.desktop-icon-test-ian-placed"
  ian_test_icon="$APP_DIR/deploy/mac/TEST-FINANCIAL-IAN.command"
  [[ -f "$marker" ]] && return 0
  [[ -d "$desktop" && -f "$ian_test_icon" ]] || return 0
  cp -f "$ian_test_icon" "$desktop/TEST-FINANCIAL-IAN.command"
  chmod +x "$desktop/TEST-FINANCIAL-IAN.command"
  xattr -d com.apple.quarantine "$desktop/TEST-FINANCIAL-IAN.command" >/dev/null 2>&1
  mkdir -p "$DATA_DIR" && : > "$marker"
  good "TEST-FINANCIAL-IAN placed on your Desktop (tests Ian on past CME data; shows the price first)"
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
  local wine
  wine="$(wine_bin)"
  mkdir -p "$LOG_DIR" "$DATA_DIR"
  # Start with algorithmic trading switched ON: a terminal started plainly
  # comes up with the Algo Trading button off and the bot cannot trade.
  printf '[Experts]\nAllowLiveTrading=1\nAllowDllImport=0\nEnabled=1\n' > "$DATA_DIR/mt5-start.ini"
  nohup "$wine" "$MT5_TERMINAL" "/config:$(to_z_path "$DATA_DIR/mt5-start.ini")" >>"$LOG_DIR/mt5.out.log" 2>&1 &
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
  # A new version must replace EVERYTHING that is running, whether or not
  # the supervisor looks alive: an older supervisor does not stop its
  # children when replaced, and the old trader would carry on unwatched.
  if [[ -n "$CODE_CHANGED" ]]; then
    say "    A new version was installed. Restarting the bot so it runs it."
    launchctl unload "$PLIST_PATH" >/dev/null 2>&1
    local name
    for name in watchdog trader bridge rider ian scalper; do
      [[ -f "$DATA_DIR/$name.pid" ]] && kill "$(tr -d '[:space:]' < "$DATA_DIR/$name.pid")" 2>/dev/null
      rm -f "$DATA_DIR/$name.pid"
    done
    sleep 2
    pkill -f "bridge_server.py" 2>/dev/null || true
    pkill -f "mintel.run --config" 2>/dev/null || true
    pkill -f "mintel.ops.watchdog --config" 2>/dev/null || true
    pkill -f "mintel.scalper.run --config" 2>/dev/null || true
    pkill -f "mintel.rider.run --config" 2>/dev/null || true
    pkill -f "mintel.ian.run --config" 2>/dev/null || true
    sleep 2
    launchctl load "$PLIST_PATH" >/dev/null 2>&1
  fi
  # The Rapid Scalper is retired: its last report must not sit on disk
  # looking like a part of the system that has stopped.
  rm -f "$DATA_DIR/heartbeats/scalper.heartbeat.json"
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
try:
    snap = json.load(sys.stdin)
except Exception:
    print("    The status page is still starting. Open http://127.0.0.1:8787 in a minute.")
    sys.exit(0)
status = snap.get("status") or {}
health = snap.get("health") or {}
running = status.get("bot") == "RUNNING" and not health.get("safe_mode")
print(f"    {'[ OK ]' if running else '[WARN]'} {health.get('summary', 'starting up')}")
for check in health.get("checks", []):
    if check.get("severity") != "OK":
        print(f"           {check['severity']}: {check['message']}")
print(f"    Mode        : {status.get('mode', '?')}")
paper = str(status.get("tnb_mode") or "").upper() == "PAPER"
if paper:
    print("    Trend & Breakout: PAPER - real prices, simulated orders (never in the account)")
print(f"    Open trades : {status.get('open_positions', 0)}")
print(f"    Today       : {status.get('today_pnl', 0)} {status.get('currency', '')}"
      + (" (Trend & Breakout's practice, not money)" if paper else ""))
print(f"    Last scan   : {status.get('last_scan', 'never')}")
thinking = snap.get("thinking") or []
if thinking:
    print("    Currently watching:")
    for item in thinking[:5]:
        print(f"      {item['rank']}. {item['symbol']:<9}{item['direction']:<6}"
              f"{item['score']:>5.0f}  {item['tier']}")
PYEOF
  # the two bots that run as their own processes, one line each
  local b
  for b in rider ian; do
    say "    $(bot_check "$b" | tail -1)"
  done
}

open_dashboard() {
  open "http://127.0.0.1:$DASH_PORT" >/dev/null 2>&1 || true
}

# bot_mode BLOCK [DEFAULT] - the mode a bot is set to in config.json
# ("runner", "bandbreaker", "crowd" or "tnb" -> "mode"): LIVE, PAPER or OFF.
# DEFAULT (PAPER, the bots' own default; LIVE for Trend & Breakout, "tnb")
# when the file or the block says nothing. Read with a JSON parser, never a
# pattern, so a mode inside another block cannot be mistaken for this one.
bot_mode() {
  local block="${1:-}"
  local fallback="${2:-PAPER}"
  local py=""
  local mode=""
  if [[ -x "$VENV_DIR/bin/python" ]]; then
    py="$VENV_DIR/bin/python"
  else
    py="$(command -v python3 2>/dev/null || true)"
  fi
  if [[ -n "$py" && -n "$block" && -f "$CONFIG" ]]; then
    mode="$("$py" -c 'import json, sys
try:
    block = json.load(open(sys.argv[1])).get(sys.argv[2]) or {}
    print(str(block.get("mode") or sys.argv[3]).upper())
except Exception:
    print(sys.argv[3])' "$CONFIG" "$block" "$fallback" 2>/dev/null || true)"
  fi
  printf '%s\n' "${mode:-$fallback}"
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
  # Every bot's mode, read from the files the bots themselves read: the
  # Rapid Momentum Rider and Financial Ian from data/rider.json and
  # data/ian.json, the other three from config.json.
  say "  Trend & Breakout: ${BOLD}$(bot_mode tnb LIVE)${RESET} the main trader; on PAPER: real prices, simulated orders (its own tab)"
  say "  Rapid Momentum Rider: ${BOLD}$(bot_file_mode rider)${RESET} rides the FX market that has just started running (data/rider.json; page /rider)"
  say "  Momentum Runner: ${BOLD}$(bot_mode runner)${RESET} rides Trend & Breakout's index entries with a 3 R trail (its own tab on the page)"
  say "  Band Breaker:    ${BOLD}$(bot_mode bandbreaker)${RESET} intraday index momentum, New York session (its own tab)"
  say "  Crowd Fader:     ${BOLD}$(bot_mode crowd)${RESET} positioning from Coinversa, faded once the price turns (its own tab)"
  say "  Financial Ian:   ${BOLD}$(bot_file_mode ian)${RESET} the CME futures order book, traded on the spot pair (data/ian.json; page /ian)"
  say "  Rapid Scalper:   retired 9 Oct, replaced by the Rapid Momentum Rider (its records are kept)"
  say "  LIVE places real orders under the bot's own magic number; PAPER simulates and is never in Overall."
  say "  To switch any bot: $VENV_DIR/bin/python -m mintel.ops.modes --config $CONFIG --live all"
  say "  (or --paper / --off, then Stop Trading Bot and Start Trading Bot; Trend & Breakout by name: --paper tnb)"
  if [[ -f "$CONFIG" ]]; then
    if ! "$VENV_DIR/bin/python" -c "import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get('coinversa_api_key') else 1)" "$DATA_DIR/secrets.json" 2>/dev/null; then
      say "  ${RED}No Coinversa key saved: the Crowd Fader cannot read anything.${RESET}"
      say "  Fix: $VENV_DIR/bin/python -m mintel.crowd --config $CONFIG --set-key"
    fi
    if ! "$VENV_DIR/bin/python" -c "import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get('github_token') else 1)" "$DATA_DIR/secrets.json" 2>/dev/null; then
      bad "No GitHub token saved: the nightly report and the ten-minute pulse will NOT publish."
      say "  Fix: $VENV_DIR/bin/python -m mintel.ops.report_upload --config $CONFIG --set-token"
    fi
    if ! "$VENV_DIR/bin/python" -c "import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get('databento_api_key') else 1)" "$DATA_DIR/secrets.json" 2>/dev/null; then
      say "  No Databento key saved: Financial Ian runs DATA-DEGRADED (no signals, no trades)."
      say "  Fix: double-click INSTALL-FINANCIAL-IAN and paste the key."
    fi
    say ""
  fi
  if (( ${#FAILURES[@]} )); then
    say "  Nothing has been started. Fix the problem below and double-click"
    say "  the icon again - everything already done is skipped."
    say "  Setup log: $SETUP_LOG"
    say ""
    say "  ${RED}Problems:${RESET}"
    printf '    - %s\n' "${FAILURES[@]}"
    say ""
    if (( ${#WARNINGS[@]} )); then
      say "  ${YELLOW}Warnings:${RESET}"
      printf '    - %s\n' "${WARNINGS[@]}"
      say ""
    fi
    return 1
  fi
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
  return 0
}

stop_everything() {
  step "Stopping the bot"
  launchctl unload "$PLIST_PATH" >/dev/null 2>&1
  local name
  for name in watchdog trader bridge rider ian scalper; do
    local pidfile="$DATA_DIR/$name.pid"
    if [[ -f "$pidfile" ]]; then
      local pid; pid="$(tr -d '[:space:]' < "$pidfile")"
      kill "$pid" 2>/dev/null && good "$name stopped" || warn "$name was not running"
      rm -f "$pidfile"
    fi
  done
  # Belt and braces: anything of ours still alive by name, whatever happened
  # to the pid files (a bridge started under Wine survived the pid file once).
  sleep 1
  pkill -f "bridge_server.py" 2>/dev/null || true
  pkill -f "mintel.run --config" 2>/dev/null || true
  pkill -f "mintel.ops.watchdog --config" 2>/dev/null || true
  pkill -f "mintel.scalper.run --config" 2>/dev/null || true
  pkill -f "mintel.rider.run --config" 2>/dev/null || true
  pkill -f "mintel.ian.run --config" 2>/dev/null || true
  # The retired Rapid Scalper's last report goes too (see start_everything).
  rm -f "$DATA_DIR/heartbeats/scalper.heartbeat.json"
  if [[ -f "$DATA_DIR/caffeinate.pid" ]]; then
    kill "$(cat "$DATA_DIR/caffeinate.pid")" 2>/dev/null
    rm -f "$DATA_DIR/caffeinate.pid"
  fi
  say ""
  say "  Any open trades are still open, and still have their stop loss held"
  say "  by the broker. The bot is simply no longer managing them."
  say ""
}
