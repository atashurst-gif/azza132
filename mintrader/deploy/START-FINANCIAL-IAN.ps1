#requires -Version 5.1
<#
.SYNOPSIS
    FINANCIAL IAN - START OR CHECK on the Windows VPS.

.DESCRIPTION
    Runs START-BOT.ps1 (it starts the watchdog if it is not running, and the
    watchdog starts Financial Ian with everything else), then prints Ian's
    health in one line. It changes no setting and never starts a second
    copy of anything. Double-click START-FINANCIAL-IAN.cmd to run it.
#>
[CmdletBinding()]
param([string] $InstallDir = "C:\mintel")

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "mintel_bots.ps1")
$P = Get-MintelPaths $InstallDir

if (-not (Test-MintelInstalled $P)) {
    Write-Host "    [FAIL] Not installed yet: run INSTALL-FINANCIAL-IAN.cmd (or SETUP-AND-START.cmd) first." -ForegroundColor Red
    exit 2
}
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $P.App "deploy\START-BOT.ps1") -InstallDir $InstallDir

$b = Wait-BotLine $P "ian" 120
$colour = if ($b.Code -eq 0) { "Green" } elseif ($b.Code -eq 2) { "Gray" } else { "Yellow" }
Write-Host ""
Write-Host "  $($b.Line)" -ForegroundColor $colour
if ((Get-BotFileMode $P "ian") -eq "OFF") {
    Write-Host "  It is switched OFF. Run INSTALL-FINANCIAL-IAN.cmd to switch it on."
}
Write-Host "  Its page: http://127.0.0.1:8787/ian"
Write-Host ""
if ($b.Code -eq 1) { exit 1 } else { exit 0 }
