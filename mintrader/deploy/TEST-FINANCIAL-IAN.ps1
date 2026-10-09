#requires -Version 5.1
<#
.SYNOPSIS
    FINANCIAL IAN - TEST ON PAST DATA (Windows VPS).

.DESCRIPTION
    Lower risk first: before any live CME feed is paid for, this tests
    Financial Ian on REAL PAST CME futures order-book data bought from
    Databento's historical service (priced per request).

      1. Installs the databento package into the bot's Python if missing.
      2. Asks ONCE for the Databento API key if none is saved (nothing is
         shown as you type; kept in data\secrets.json, never printed).
         Saving it does NOT switch Financial Ian's live feed on.
      3. Asks Databento what the test costs and shows it first, e.g.
             This test will download about 2.1 GB of CME order-book data
             for 5 days and Databento will charge about $37.40.
             Go ahead? (y/N)
         Nothing is bought unless you type y. Days already downloaded are
         never bought again. A cheaper first look (two days):
             TEST-FINANCIAL-IAN.cmd -Days 2
      4. Replays the data through Ian's own engine (no MetaTrader, no
         orders), writes the report, uploads it to GitHub
         (reports/ian-research/<date>/) and prints the verdict.

    It never restarts, stops or changes the running bots, and the test runs
    at Idle priority so the bots always come first.
    Double-click TEST-FINANCIAL-IAN.cmd to run it.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [string] $InstallDir = "C:\mintel",
    [int] $Days = 0,
    [Parameter(ValueFromRemainingArguments = $true)] [string[]] $Extra = @()
)

$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "mintel_bots.ps1")
$P = Get-MintelPaths $InstallDir

Write-Host ""
Write-Host "=============================================================="
Write-Host "  FINANCIAL IAN - TEST ON PAST CME DATA"
Write-Host "=============================================================="

if (-not (Test-MintelInstalled $P)) {
    Write-Host "    The bot is not installed here yet: run SETUP-AND-START first, then this again." -ForegroundColor Yellow
    exit 1
}

# The program to test: the copy next to this script when it has the test, otherwise the installed one.
$here = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$app = $null
foreach ($cand in @($here, $P.App)) {
    if (Test-Path (Join-Path $cand "mintel\ian\research\historical.py")) { $app = $cand; break }
}
if (-not $app) {
    Write-Host "    This copy of the bot does not have the past-data test yet: run SETUP-AND-START from the newest download first." -ForegroundColor Yellow
    exit 1
}
Write-Host "    Testing the Financial Ian code in: $app"

function Invoke-Low([string[]] $Arguments) {
    # The bot's Python in this console (so it can ask y/N and read a hidden key), at Idle priority.
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $P.Py
    $psi.Arguments = ($Arguments | ForEach-Object { '"' + ($_ -replace '"', '\"') + '"' }) -join ' '
    $psi.WorkingDirectory = $app
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    try { $proc.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::Idle } catch { }
    $proc.WaitForExit()
    return $proc.ExitCode
}

$probe = Invoke-MintelPython $P @("-c", "import databento")
if ($probe.Code -ne 0) {
    Write-Host ""
    Write-Host "    Installing the databento package into the bot's Python (one time) ..."
    if ((Invoke-Low @("-m", "pip", "install", "--quiet", "--disable-pip-version-check", "databento")) -ne 0) {
        Write-Host "    The databento package did not install (see above). Nothing was bought." -ForegroundColor Red
        exit 1
    }
}

$hasKey = $false
try { $hasKey = [bool]((Get-Content $P.Secrets -Raw | ConvertFrom-Json).databento_api_key) } catch { }
if (-not $hasKey) {
    Write-Host ""
    Write-Host "    No Databento API key is saved yet. Paste it below - it starts with db- (Databento portal -> API keys)."
    Write-Host "    Nothing is shown as you type."
    if ((Invoke-Low @("-m", "mintel.ian.research.historical", "--config", $P.Config, "--set-key")) -ne 0) {
        Write-Host "    No key saved: nothing was bought." -ForegroundColor Yellow
        exit 1
    }
}

$testArgs = @("-m", "mintel.ian.research.historical", "--config", $P.Config, "--upload")
if ($Days -gt 0) { $testArgs += @("--days", "$Days") }
$code = Invoke-Low ($testArgs + $Extra)
Write-Host ""
# 10 is the test's own "nothing was bought in this run" and nothing else returns it (Python exits 1 on a
# crash, 2 on a bad option, 4 when Ctrl+C stopped it after buying had started).
switch ($code) {
    0 { Write-Host "  Finished. The report is in $($P.Data)\ian-research\ (and on GitHub when the upload worked)." -ForegroundColor Green }
    10 { Write-Host "  Nothing was bought." }
    default { Write-Host "  It stopped - see the lines above. Anything already downloaded was paid for and stays on disk (it is never bought again: run this again to carry on)." -ForegroundColor Yellow }
}
exit $code
