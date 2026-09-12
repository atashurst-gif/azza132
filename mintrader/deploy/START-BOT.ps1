#requires -Version 5.1
<#
.SYNOPSIS
    ONE COMMAND to start or check the trading bot.

.DESCRIPTION
    Safe to run as often as you like.  If everything is already running it
    simply reports that it is healthy and starts nothing.  It never starts a
    second trader - two traders on one account would double every trade.

    What it does:
      * starts MetaTrader 5 if it is not running
      * starts the watchdog if it is not running
      * starts the trader if it is not running
      * prints the health summary and the dashboard address

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\START-BOT.ps1
#>

[CmdletBinding()]
param(
    [string] $InstallDir = "C:\mintel",
    [switch] $FromSetup,
    [switch] $Restart
)

$ErrorActionPreference = "Continue"

$appDir     = Join-Path $InstallDir "app"
$dataDir    = Join-Path $InstallDir "data"
$logDir     = Join-Path $InstallDir "logs"
$venvPy     = Join-Path $InstallDir "venv\Scripts\python.exe"
$configPath = Join-Path $dataDir "config.json"

function Say  { param($m) Write-Host $m }
function Good { param($m) Write-Host "    [ OK ] $m" -ForegroundColor Green }
function Warn { param($m) Write-Host "    [WARN] $m" -ForegroundColor Yellow }
function Bad  { param($m) Write-Host "    [FAIL] $m" -ForegroundColor Red }

if (-not $FromSetup) {
    Say ""
    Say "=============================================================="
    Say "  MARKET INTELLIGENCE TRADER - START / CHECK"
    Say "=============================================================="
}

if (-not (Test-Path $venvPy)) {
    Bad "Not installed yet. Run SETUP-AND-START.ps1 first."
    exit 2
}
if (-not (Test-Path $configPath)) {
    Bad "No configuration found. Run SETUP-AND-START.ps1 first."
    exit 2
}

$cfg = Get-Content $configPath -Raw | ConvertFrom-Json
$dashPort = 8787
if ($cfg.ops -and $cfg.ops.dashboard_port) { $dashPort = $cfg.ops.dashboard_port }

# ---------------------------------------------------------------- restart ----
if ($Restart) {
    Say ""
    Say "Stopping the bot..."
    foreach ($name in @("trader", "watchdog")) {
        $pidFile = Join-Path $dataDir "$name.pid"
        if (Test-Path $pidFile) {
            $processId = (Get-Content $pidFile -Raw).Trim()
            try {
                Stop-Process -Id ([int]$processId) -Force -ErrorAction Stop
                Good "$name stopped"
            } catch { Warn "$name was not running" }
            Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Seconds 2
}

# -------------------------------------------------------------------- MT5 ----
Say ""
Say "MetaTrader 5"
if (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue) {
    Good "already running"
} elseif ($cfg.mt5_terminal_path -and (Test-Path $cfg.mt5_terminal_path)) {
    Start-Process -FilePath $cfg.mt5_terminal_path
    Start-Sleep -Seconds 20
    if (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue) {
        Good "started"
    } else { Bad "it would not start" }
} else {
    Bad "the terminal path in the configuration does not exist"
}

# --------------------------------------------------------- helper: running ---
function Test-Running {
    param([string] $Name)
    $pidFile = Join-Path $dataDir "$Name.pid"
    if (-not (Test-Path $pidFile)) { return $false }
    $raw = (Get-Content $pidFile -Raw).Trim()
    if (-not $raw) { return $false }
    try {
        $p = Get-Process -Id ([int]$raw) -ErrorAction Stop
        return $null -ne $p
    } catch { return $false }
}

# --------------------------------------------------------------- watchdog ----
Say ""
Say "Watchdog"
if (Test-Running "watchdog") {
    Good "already running"
} else {
    $wd = Start-Process -FilePath $venvPy -PassThru -WindowStyle Hidden `
        -WorkingDirectory $appDir `
        -ArgumentList @("-m", "mintel.ops.watchdog", "--config", $configPath) `
        -RedirectStandardOutput (Join-Path $logDir "watchdog.out.log") `
        -RedirectStandardError  (Join-Path $logDir "watchdog.err.log")
    Set-Content -Path (Join-Path $dataDir "watchdog.pid") -Value $wd.Id
    Start-Sleep -Seconds 2
    if (Test-Running "watchdog") { Good "started (process $($wd.Id))" }
    else { Bad "it would not start - see $logDir\watchdog.err.log" }
}

# ----------------------------------------------------------------- trader ----
Say ""
Say "Trader"
if (Test-Running "trader") {
    Good "already running - nothing to start"
} else {
    # The watchdog will also start the trader; starting it here means the
    # dashboard is up immediately rather than after the first watchdog tick.
    $tr = Start-Process -FilePath $venvPy -PassThru -WindowStyle Hidden `
        -WorkingDirectory $appDir `
        -ArgumentList @("-m", "mintel.run", "--config", $configPath) `
        -RedirectStandardOutput (Join-Path $logDir "trader.out.log") `
        -RedirectStandardError  (Join-Path $logDir "trader.err.log")
    Start-Sleep -Seconds 8
    if (Test-Running "trader") { Good "started (process $($tr.Id))" }
    else {
        Bad "it would not start - see $logDir\trader.err.log"
        if (Test-Path (Join-Path $logDir "trader.err.log")) {
            Get-Content (Join-Path $logDir "trader.err.log") -Tail 15 |
                ForEach-Object { Write-Host "      $_" -ForegroundColor DarkGray }
        }
    }
}

# ----------------------------------------------------------------- health ----
Say ""
Say "Health"
Start-Sleep -Seconds 5
try {
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:$dashPort/health" `
        -TimeoutSec 15 -ErrorAction Stop
    $summary = $health.health.summary
    if ($health.health.safe_mode) { Warn $summary }
    elseif ($health.health.entries_allowed) { Good $summary }
    else { Warn $summary }
    foreach ($c in $health.health.checks) {
        if ($c.severity -ne "OK") {
            Write-Host "      $($c.severity): $($c.message)" -ForegroundColor Yellow
        }
    }
    Say ""
    Say "    Mode        : $($health.status.mode)"
    Say "    Open trades : $($health.status.open_positions)"
    Say "    Today       : $($health.status.today_pnl) $($health.status.currency)"
    Say "    Last scan   : $($health.status.last_scan)"
    if ($health.thinking) {
        Say ""
        Say "    Currently watching:"
        foreach ($item in $health.thinking | Select-Object -First 5) {
            Say ("      {0}. {1,-8} {2,-5} {3,5:N0}  {4}" -f `
                $item.rank, $item.symbol, $item.direction, $item.score,
                $item.tier)
        }
    }
} catch {
    Warn "The status page did not answer yet. Give it a minute, then open:"
    Warn "  http://127.0.0.1:$dashPort"
}

Say ""
Say "=============================================================="
Say "  Status page :  http://127.0.0.1:$dashPort"
Say "  Results page:  http://127.0.0.1:$dashPort/results"
Say "=============================================================="
Say ""
exit 0
