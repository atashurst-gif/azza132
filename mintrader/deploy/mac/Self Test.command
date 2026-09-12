#!/bin/bash
#
#  SELF TEST - proves the Mac side works before any broker is involved.
#
#  It starts the real bridge, the real trader and the real dashboard, with the
#  built-in simulator standing in for MetaTrader 5. No account is needed, no
#  order can reach a broker, and nothing it does touches your real settings.
#
#  If this passes, everything on the Mac is sound and any remaining problem is
#  MetaTrader, Wine, or your broker login.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

find_source() {
  local candidate="$SCRIPT_DIR"
  for _ in 1 2 3 4; do
    [[ -d "$candidate/mintel" ]] && { printf '%s' "$candidate"; return 0; }
    candidate="$(dirname "$candidate")"
  done
  [[ -d "$HOME/MarketBot/app/mintel" ]] && { printf '%s' "$HOME/MarketBot/app"; return 0; }
  return 1
}

SOURCE_DIR="$(find_source)" || { echo "Cannot find the program files."; read -r -p "Press Enter. " _; exit 1; }
VENV_PY="$HOME/MarketBot/venv/bin/python"
[[ -x "$VENV_PY" ]] || VENV_PY="$(command -v python3)"

WORK="$(mktemp -d /tmp/mintel-selftest.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

echo ""
echo "=============================================================="
echo "   SELF TEST - no broker, no real money, nothing at risk"
echo "=============================================================="
echo ""
echo "  Working folder: $WORK"
echo "  This takes about a minute."
echo ""

cd "$SOURCE_DIR" || exit 1
"$VENV_PY" - "$WORK" <<'PYEOF'
import json, os, socket, subprocess, sys, time, urllib.request
from pathlib import Path

work = Path(sys.argv[1])
data, logs = work / "data", work / "logs"
data.mkdir(parents=True); logs.mkdir(parents=True)

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); return s.getsockname()[1]

bridge_port, dash_port = free_port(), free_port()
(data / "bridge-token.txt").write_text("selftest")
(data / "config.json").write_text(json.dumps({
    "version": 1, "mode": "DEMO", "magic": 990311, "broker_mode": "bridge",
    "bridge_host": "127.0.0.1", "bridge_port": bridge_port,
    "aggression": "NORMAL",
    "risk": {"base_risk_pct": 0.5, "max_risk_pct": 1.5},
    "universe": {"max_symbols": 6},
    "scan": {"scan_interval_seconds": 2.0},
    "news": {"enabled": True, "store_path": "calendar.sqlite"},
    "ops": {"data_dir": str(data), "log_dir": str(logs),
            "dashboard_host": "127.0.0.1", "dashboard_port": dash_port},
}, indent=2))

procs = []
def spawn(args, name):
    p = subprocess.Popen([sys.executable, *args],
                         stdout=open(logs / f"{name}.log", "w"),
                         stderr=subprocess.STDOUT, start_new_session=True)
    procs.append(p); return p

def wait(check, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if check():
                print(f"  [ OK ] {what}"); return True
        except Exception:
            pass
        time.sleep(1)
    print(f"  [FAIL] {what}"); return False

failures = 0
try:
    spawn(["-m", "mintel.broker.bridge_server", "--backend", "sim",
           "--port", str(bridge_port),
           "--token-file", str(data / "bridge-token.txt"),
           "--sim-bars", "3000", "--sim-live", "--sim-speed", "30"],
          "bridge")
    ok = wait(lambda: socket.create_connection(("127.0.0.1", bridge_port), 2),
              60, "the MetaTrader bridge starts and answers")
    failures += 0 if ok else 1

    spawn(["-m", "mintel.run", "--config", str(data / "config.json")], "trader")
    url = f"http://127.0.0.1:{dash_port}/api"
    get = lambda: json.loads(urllib.request.urlopen(url, timeout=5).read())
    ok = wait(lambda: get() is not None, 120, "the trader starts and the status page answers")
    failures += 0 if ok else 1

    if ok:
        snap = get()
        st, health = snap["status"], snap["health"]
        for label, good in (
                ("the trader can see the broker through the bridge", st.get("mt5_connected")),
                ("market data is arriving", st.get("data_live")),
                ("it is in DEMO mode", st.get("mode") == "DEMO"),
                ("nothing is in SAFE MODE", not health.get("safe_mode"))):
            print(f"  [{' OK ' if good else 'FAIL'}] {label}")
            failures += 0 if good else 1
        ok = wait(lambda: get()["thinking"], 90, "it is scanning and ranking markets")
        failures += 0 if ok else 1
        if ok:
            print("\n  What it is looking at in the simulation:")
            for item in get()["thinking"][:5]:
                print(f"    {item['rank']}. {item['symbol']:<9}{item['direction']:<6}"
                      f"{item['score']:>5.0f}  {item['tier']:<12}"
                      f"{item['tactic'].replace('_', ' ').title()}")
finally:
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), 15)
        except Exception:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=15)
        except Exception:
            p.kill()

print()
if failures:
    print(f"  RESULT: {failures} check(s) failed. Logs are in {logs}")
    sys.exit(1)
print("  RESULT: everything on the Mac side works.")
print("  Any remaining problem is MetaTrader, Wine, or the broker login.")
PYEOF
STATUS=$?
echo ""
read -r -p "Press Enter to close. " _
exit $STATUS
