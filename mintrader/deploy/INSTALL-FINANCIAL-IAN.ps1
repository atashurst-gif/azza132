#requires -Version 5.1
<#
.SYNOPSIS
    FINANCIAL IAN - INSTALL on the Windows VPS.

.DESCRIPTION
    Asks ONCE for an optional Databento API key (nothing is shown as you
    type; press Enter to skip) and saves it in data\secrets.json - never in
    the code. Installs the databento package into the bot's Python only when
    a key is saved, names the Databento feed in data\ian.json, switches
    Financial Ian on in PAPER (a LIVE choice already made is kept), makes
    sure everything is running (START-BOT.ps1) and prints Ian's health in one
    line, for example:

        FINANCIAL IAN - HEALTHY, PAPER, feed NOT CONFIGURED

    Without a key Ian runs safely DATA-DEGRADED: no signals, no trades.
    Double-click INSTALL-FINANCIAL-IAN.cmd to run it.
#>
[CmdletBinding()]
param([string] $InstallDir = "C:\mintel")

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "mintel_bots.ps1")
$P = Get-MintelPaths $InstallDir

Write-Host ""
Write-Host "=============================================================="
Write-Host "  FINANCIAL IAN - INSTALL"
Write-Host "=============================================================="

$here = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path.TrimEnd("\")
$fromInstall = $here -ieq $P.App.TrimEnd("\")
if (-not (Test-MintelInstalled $P) -or -not $fromInstall) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "SETUP-AND-START.ps1") -InstallDir $InstallDir
    if (-not (Test-MintelInstalled $P)) {
        Write-Host "    [FAIL] The bot is not installed yet: see the lines above." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "    Financial Ian reads the CME futures order book. That needs a data-feed key"
Write-Host "    (Databento: see docs\ops\financial_ian_data_feed.md). Without one it runs"
Write-Host "    safely in DATA-DEGRADED mode: no signals and no trades."
Write-Host ""
Write-Host "    Paste your Databento API key, or just press Enter to skip. Nothing is shown as you type."
$r = Invoke-MintelPython $P @("-m", "mintel.ian", "--config", $P.Config, "--set-key")
foreach ($l in $r.Lines) { Write-Host "      $l" }

$hasKey = $false
try { $hasKey = [bool]((Get-Content $P.Secrets -Raw | ConvertFrom-Json).databento_api_key) } catch { }
if ($hasKey) {
    Write-Host "    [ OK ] A Databento key is saved in $($P.Secrets) (never in the code)" -ForegroundColor Green
    $probe = Invoke-MintelPython $P @("-c", "import databento")
    if ($probe.Code -ne 0) {
        Write-Host "    Installing the databento package..."
        $pip = Invoke-MintelPython $P @("-m", "pip", "install", "databento")
        $pip.Lines | Select-Object -Last 3 | ForEach-Object { Write-Host "      $_" }
        if ($pip.Code -ne 0) {
            Write-Host "    [WARN] The databento package did not install: Ian will say the feed is DOWN until it does." -ForegroundColor Yellow
        }
    } else {
        Write-Host "    [ OK ] The databento package is installed" -ForegroundColor Green
    }
    [void](Invoke-Modes $P @("--ian-feed", "databento"))
} else {
    Write-Host "    No key: Financial Ian stays DATA-DEGRADED (no signals, no trades). Run this again any time."
}
if (-not (Enable-Ian $P)) {
    Write-Host "    [FAIL] Financial Ian could not be switched on (see the lines above)." -ForegroundColor Red
    exit 1
}
Write-Host "    [ OK ] Financial Ian: $(Get-BotFileMode $P 'ian')" -ForegroundColor Green

# Everything running (nothing that is, started twice); then restart only Ian
# so it reads its feed and its key now - the watchdog starts it again.
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $P.App "deploy\START-BOT.ps1") -InstallDir $InstallDir
if (Test-BotRunning $P "ian") {
    [void](Stop-Bot $P "ian")
    Start-Sleep -Seconds 3
}

$b = Wait-BotLine $P "ian" 120
$colour = if ($b.Code -eq 0) { "Green" } else { "Yellow" }
Write-Host ""
Write-Host "  $($b.Line)" -ForegroundColor $colour
Write-Host "  Its page: http://127.0.0.1:8787/ian"
Write-Host ""
if ($b.Code -eq 0) { exit 0 } else { exit 1 }
