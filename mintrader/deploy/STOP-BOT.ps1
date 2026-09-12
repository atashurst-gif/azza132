#requires -Version 5.1
<#
.SYNOPSIS
    Stop the trading bot.

.DESCRIPTION
    Stops the trader and the watchdog.  Open positions are NOT closed and
    their broker-side stops remain in place, so existing risk stays protected
    while the bot is down.  Use -CloseAll only if you want out of the market.
#>
[CmdletBinding()]
param(
    [string] $InstallDir = "C:\mintel",
    [switch] $DisableAutoStart
)

$dataDir = Join-Path $InstallDir "data"
Write-Host ""
Write-Host "Stopping the bot..."

# The watchdog goes first: stopping the trader while the watchdog is alive
# would simply cause it to be restarted.
foreach ($name in @("watchdog", "trader")) {
    $pidFile = Join-Path $dataDir "$name.pid"
    if (Test-Path $pidFile) {
        $processId = (Get-Content $pidFile -Raw).Trim()
        try {
            Stop-Process -Id ([int]$processId) -Force -ErrorAction Stop
            Write-Host "    [ OK ] $name stopped" -ForegroundColor Green
        } catch {
            Write-Host "    [WARN] $name was not running" -ForegroundColor Yellow
        }
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "    [WARN] no record of $name running" -ForegroundColor Yellow
    }
}

if ($DisableAutoStart) {
    foreach ($task in @("MintelTrader", "MintelKeepAlive")) {
        try {
            Disable-ScheduledTask -TaskName $task -ErrorAction Stop | Out-Null
            Write-Host "    [ OK ] scheduled task $task disabled" -ForegroundColor Green
        } catch {
            Write-Host "    [WARN] could not disable $task" -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "Any open trades are still open, and still have their stop loss"
Write-Host "held by the broker. The bot is simply no longer managing them."
Write-Host ""
