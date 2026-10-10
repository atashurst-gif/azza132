#requires -Version 5.1
<#
.SYNOPSIS
    RAPID MOMENTUM RIDER - SETUP on the Windows VPS.

.DESCRIPTION
    Puts the Rapid Momentum Rider on LIVE in place of the retired Rapid
    Scalper (GBP 1 a pip unless a figure is already set in data\rider.json),
    makes sure everything is installed and running through the usual
    machinery (SETUP-AND-START.ps1 the first time or when these are new
    program files, START-BOT.ps1 otherwise), and ends with the Rider's health
    in one line, for example:

        RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 28 markets

    Safe to run any time: the watchdog starts the Rider, and the Rider
    refuses to run twice. Double-click SETUP-RAPID-RIDER.cmd to run it.

    10 Oct: the Rider was switched OFF on 9 Oct (its own 10-day test on real
    prices found no edge). While it is off, or once the Formula 1 step has
    run, this asks first and only goes on if you type LIVE; anything else
    changes nothing (Confirm-RiderLive in mintel_bots.ps1).
#>
[CmdletBinding()]
param([string] $InstallDir = "C:\mintel")

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "mintel_bots.ps1")
$P = Get-MintelPaths $InstallDir

Write-Host ""
Write-Host "=============================================================="
Write-Host "  RAPID MOMENTUM RIDER - SETUP"
Write-Host "=============================================================="

# 10 Oct: the Rider was switched OFF on 9 Oct (its own test on real prices
# found no edge). While it is off, or once the Formula 1 step has run, ask
# first - before anything else runs: only LIVE, typed as it is, goes on;
# anything else changes nothing.
$confirmed = $false
if (Test-RiderSacked $P) {
    if (-not (Confirm-RiderLive $P)) {
        Write-Host ""
        exit 1
    }
    $confirmed = $true
}

$here = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path.TrimEnd("\")
$fromInstall = $here -ieq $P.App.TrimEnd("\")
if (-not (Test-MintelInstalled $P) -or -not $fromInstall) {
    # The first time, or new program files: the full setup. It keeps your
    # settings and keys unless you choose to change them.
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "SETUP-AND-START.ps1") -InstallDir $InstallDir
    if (-not (Test-MintelInstalled $P)) {
        Write-Host "    [FAIL] The bot is not installed yet: see the lines above." -ForegroundColor Red
        exit 1
    }
}

# A first install switches the Rider off itself (the Formula 1 step): ask then too
if (-not $confirmed -and -not (Confirm-RiderLive $P)) {
    Write-Host ""
    exit 1
}

Write-Host ""
Write-Host "Rapid Momentum Rider: LIVE, in place of the Rapid Scalper"
if (-not (Set-RiderLive $P)) {
    Write-Host "    [FAIL] The Rapid Momentum Rider could not be switched on (see the lines above)." -ForegroundColor Red
    exit 1
}

# Start whatever is not running (nothing that is), then restart only the
# Rider if it is still running in its old mode: the watchdog starts it again.
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $P.App "deploy\START-BOT.ps1") -InstallDir $InstallDir
Restart-BotIfModeChanged $P "rider"

$b = Wait-BotLine $P "rider" 150
$colour = if ($b.Code -eq 0) { "Green" } else { "Yellow" }
Write-Host ""
Write-Host "  $($b.Line)" -ForegroundColor $colour
Write-Host "  Its page: http://127.0.0.1:8787/rider"
Write-Host ""
if ($b.Code -eq 0) { exit 0 } else { exit 1 }
