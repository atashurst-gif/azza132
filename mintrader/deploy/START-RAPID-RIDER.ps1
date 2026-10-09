#requires -Version 5.1
<#
.SYNOPSIS
    RAPID MOMENTUM RIDER - START OR CHECK on the Windows VPS.

.DESCRIPTION
    Runs START-BOT.ps1 (it starts the watchdog if it is not running, and the
    watchdog starts the Rider with everything else), then prints the Rider's
    health in one line. It changes no setting and never starts a second
    copy of anything. Double-click START-RAPID-RIDER.cmd to run it.
#>
[CmdletBinding()]
param([string] $InstallDir = "C:\mintel")

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "mintel_bots.ps1")
$P = Get-MintelPaths $InstallDir

if (-not (Test-MintelInstalled $P)) {
    Write-Host "    [FAIL] Not installed yet: run SETUP-RAPID-RIDER.cmd (or SETUP-AND-START.cmd) first." -ForegroundColor Red
    exit 2
}
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $P.App "deploy\START-BOT.ps1") -InstallDir $InstallDir

$b = Wait-BotLine $P "rider" 150
$colour = if ($b.Code -eq 0) { "Green" } elseif ($b.Code -eq 2) { "Gray" } else { "Yellow" }
Write-Host ""
Write-Host "  $($b.Line)" -ForegroundColor $colour
if ((Get-BotFileMode $P "rider") -eq "OFF") {
    Write-Host "  It is switched OFF. Run SETUP-RAPID-RIDER.cmd to put it on LIVE."
}
Write-Host "  Its page: http://127.0.0.1:8787/rider"
Write-Host ""
if ($b.Code -eq 1) { exit 1 } else { exit 0 }
