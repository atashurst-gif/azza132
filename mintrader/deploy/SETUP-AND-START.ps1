#requires -Version 5.1
<#
.SYNOPSIS
    ONE COMMAND setup and start for the Market Intelligence Trader on a
    Windows VPS.

.DESCRIPTION
    Run this once.  It does everything:

      1.  checks Windows, PowerShell and administrator rights
      2.  installs Python if it is missing
      3.  creates the folders and the Python environment
      4.  installs the Python packages (including MetaTrader5)
      5.  asks you - once - for your MT5 account, risk settings and mode
      6.  stores the configuration, with the password in a separate file
      7.  finds or installs MetaTrader 5 and starts the terminal
      8.  copies the MQL5 helper into the terminal's folder if present
      9.  registers scheduled tasks so everything restarts after a reboot
     10.  starts the watchdog, the trader and the dashboard
     11.  verifies MT5 is connected, prices are live, news works and trading
          is permitted
     12.  runs a safe smoke test (no orders are placed)
     13.  prints a final status block

    After this, normal use is ONE command: .\START-BOT.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\SETUP-AND-START.ps1
#>

[CmdletBinding()]
param(
    [string] $InstallDir = "C:\mintel",
    [switch] $NonInteractive,
    [switch] $SkipPythonInstall,
    [switch] $NoAutoStart
)

$ErrorActionPreference = "Stop"
$ProgressPreference    = "SilentlyContinue"
$script:Failures = @()
$script:Warnings = @()

# --------------------------------------------------------------- output ------
function Say      { param($m) Write-Host $m }
function Step     { param($m) Write-Host ""; Write-Host "==> $m" -ForegroundColor Cyan }
function Good     { param($m) Write-Host "    [ OK ] $m" -ForegroundColor Green }
function Warn     { param($m) Write-Host "    [WARN] $m" -ForegroundColor Yellow
                   $script:Warnings += $m }
function Bad      { param($m) Write-Host "    [FAIL] $m" -ForegroundColor Red
                   $script:Failures += $m }

function Ask {
    param([string] $Question, [string] $Default = "", [switch] $Secret)
    if ($NonInteractive) { return $Default }
    $suffix = if ($Default) { " [$Default]" } else { "" }
    if ($Secret) {
        $secure = Read-Host "$Question$suffix" -AsSecureString
        $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        if ([string]::IsNullOrWhiteSpace($plain)) { return $Default }
        return $plain
    }
    $answer = Read-Host "$Question$suffix"
    if ([string]::IsNullOrWhiteSpace($answer)) { return $Default }
    return $answer
}

Say ""
Say "=============================================================="
Say "  MARKET INTELLIGENCE TRADER - ONE COMMAND SETUP"
Say "=============================================================="

# ------------------------------------------------------- 1. prerequisites ----
Step "Checking this machine"

if ($PSVersionTable.PSVersion.Major -lt 5) {
    Bad "PowerShell 5.1 or newer is required (found $($PSVersionTable.PSVersion))."
    throw "PowerShell too old"
}
Good "PowerShell $($PSVersionTable.PSVersion)"

$isAdmin = ([Security.Principal.WindowsPrincipal] `
    [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if ($isAdmin) { Good "Running as administrator" }
else {
    Warn "Not running as administrator. Automatic restart after reboot cannot be registered."
    Warn "Close this window, right-click PowerShell, choose 'Run as administrator', and run the command again."
}

$os = Get-CimInstance Win32_OperatingSystem
Good "$($os.Caption) ($($os.OSArchitecture))"
$freeGb = [math]::Round((Get-PSDrive ($InstallDir.Substring(0,1))).Free / 1GB, 1)
if ($freeGb -lt 5) { Warn "Only $freeGb GB of free disk space." }
else { Good "$freeGb GB of free disk space" }

# ------------------------------------------------------------ 2. python ------
Step "Checking Python"

function Get-PythonExe {
    foreach ($candidate in @("python", "python3", "py")) {
        try {
            $v = & $candidate -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null
            if ($LASTEXITCODE -eq 0 -and $v) {
                $parts = $v.Trim().Split(".")
                if ([int]$parts[0] -ge 3 -and [int]$parts[1] -ge 9) {
                    $resolved = (Get-Command $candidate -ErrorAction SilentlyContinue).Source
                    return @{ Exe = $resolved; Version = $v.Trim() }
                }
            }
        } catch { }
    }
    return $null
}

$python = Get-PythonExe
if (-not $python -and -not $SkipPythonInstall) {
    Warn "Python 3.9+ was not found. Downloading and installing it now."
    $url = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
    $installer = Join-Path $env:TEMP "python-installer.exe"
    try {
        Invoke-WebRequest -Uri $url -OutFile $installer -UseBasicParsing
        Start-Process -FilePath $installer -Wait -ArgumentList @(
            "/quiet", "InstallAllUsers=1", "PrependPath=1", "Include_pip=1",
            "Include_test=0")
        $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") +
                    ";" + [Environment]::GetEnvironmentVariable("Path", "User")
        $python = Get-PythonExe
    } catch {
        Bad "Python could not be installed automatically: $($_.Exception.Message)"
    }
}
if (-not $python) {
    Bad "Python 3.9+ is required. Install it from python.org and run this again."
    throw "no python"
}
Good "Python $($python.Version) at $($python.Exe)"

# ------------------------------------------------------------ 3. folders -----
Step "Creating folders"

$projectSource = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$appDir  = Join-Path $InstallDir "app"
$dataDir = Join-Path $InstallDir "data"
$logDir  = Join-Path $InstallDir "logs"
foreach ($d in @($InstallDir, $appDir, $dataDir, $logDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}
Good "Install folder: $InstallDir"

if ($projectSource -ne $appDir) {
    Write-Host "    copying program files..."
    foreach ($item in @("mintel", "tests", "deploy", "pytest.ini",
                        "requirements.txt", "README.md")) {
        $src = Join-Path $projectSource $item
        if (Test-Path $src) {
            Copy-Item -Path $src -Destination $appDir -Recurse -Force
        }
    }
    Good "Program files copied to $appDir"
} else {
    Good "Running from the install folder already"
}

# ------------------------------------------------------- 4. python packages --
Step "Setting up the Python environment"

$venv    = Join-Path $InstallDir "venv"
$venvPy  = Join-Path $venv "Scripts\python.exe"
if (-not (Test-Path $venvPy)) {
    & $python.Exe -m venv $venv
}
if (-not (Test-Path $venvPy)) {
    Bad "The Python environment could not be created."
    throw "venv failed"
}
Good "Environment: $venv"

& $venvPy -m pip install --upgrade pip --quiet
$requirements = Join-Path $appDir "requirements.txt"
if (-not (Test-Path $requirements)) {
    Set-Content -Path $requirements -Encoding ASCII -Value @(
        "MetaTrader5>=5.0.45",
        "numpy>=1.24",
        "scikit-learn>=1.3",
        "psutil>=5.9")
}
& $venvPy -m pip install -r $requirements --quiet
if ($LASTEXITCODE -ne 0) {
    Warn "Some packages failed to install. Retrying the essential ones."
    & $venvPy -m pip install MetaTrader5 psutil --quiet
}
$mt5Ok = & $venvPy -c "import MetaTrader5; print('ok')" 2>$null
if ($mt5Ok -match "ok") { Good "MetaTrader5 Python package installed" }
else { Bad "The MetaTrader5 Python package is not available. The trader cannot run without it." }

# ------------------------------------------------- 5. interactive settings ---
$configPath = Join-Path $dataDir "config.json"
$existing   = Test-Path $configPath

Step "Configuration"
if ($existing) {
    Good "An existing configuration was found at $configPath"
    $reconfigure = (Ask "Change your settings? (y/N)" "N")
} else {
    $reconfigure = "y"
}

if ($reconfigure -match "^[Yy]") {
    Say ""
    Say "    I need a few details. This is the only time you will be asked."
    Say ""
    $login    = Ask "    MT5 account number"
    $password = Ask "    MT5 password" -Secret
    $server   = Ask "    MT5 server name (exactly as shown in MT5)"
    Say ""
    Say "    DEMO is strongly recommended until you have seen it run."
    $modeAns  = Ask "    Trade on DEMO or LIVE?" "DEMO"
    $mode     = if ($modeAns -match "^[Ll]") { "LIVE" } else { "DEMO" }
    $marker   = ""
    if ($mode -eq "LIVE") {
        Say ""
        Say "    LIVE means real money can be lost." -ForegroundColor Yellow
        $confirm = Ask "    Type exactly I-UNDERSTAND-THIS-TRADES-REAL-MONEY to confirm"
        if ($confirm -eq "I-UNDERSTAND-THIS-TRADES-REAL-MONEY") {
            $marker = $confirm
        } else {
            Warn "Not confirmed - staying on DEMO."
            $mode = "DEMO"
        }
    }
    Say ""
    $baseRisk = Ask "    Normal risk per trade, in percent of the account" "0.5"
    $maxRisk  = Ask "    MAXIMUM risk per trade, in percent (never exceeded)" "1.5"
    $dailyMax = Ask "    Stop trading for the day after losing this percent" "3.0"
    $aggr     = Ask "    Aggression: CONSERVATIVE / NORMAL / AGGRESSIVE / MAXIMUM" "NORMAL"
    $newsKey  = Ask "    Optional external news API key (press Enter to skip)" ""

    $cfg = [ordered]@{
        version        = 1
        mode           = $mode
        live_marker    = $marker
        magic          = 990311
        account_login  = [int]$login
        account_server = $server
        mt5_terminal_path = ""
        aggression     = $aggr.ToUpper()
        risk = [ordered]@{
            base_risk_pct           = [double]$baseRisk
            max_risk_pct            = [double]$maxRisk
            min_risk_pct            = 0.1
            max_total_risk_pct      = [math]::Max([double]$maxRisk * 3, 4.0)
            max_correlated_risk_pct = [math]::Max([double]$maxRisk * 1.5, 2.0)
            max_open_positions      = 8
            max_positions_per_symbol = 1
            max_daily_loss_pct      = [double]$dailyMax
            max_drawdown_pct        = 12.0
            min_margin_level_pct    = 300.0
            max_rejects_per_10min   = 6
            max_spread_multiple     = 3.0
            max_orders_per_hour     = 30
            use_fractional_kelly    = $true
            kelly_fraction          = 0.25
            kelly_min_samples       = 60
        }
        universe = [ordered]@{
            groups = @("FX_MAJOR", "FX_MINOR", "GOLD", "SILVER", "INDEX",
                       "ENERGY")
            max_symbols       = 60
            min_history_bars  = 400
            exclude_patterns  = @("BTC", "ETH", ".b", "-2", "TEST")
            require_full_spec = $true
        }
        news = [ordered]@{
            enabled            = $true
            use_mt5_calendar   = $true
            store_path         = "calendar.sqlite"
            external_adapters  = @()
            api_keys           = @{}
            max_staleness_seconds = 900.0
        }
        ops = [ordered]@{
            data_dir       = $dataDir
            log_dir        = $logDir
            dashboard_host = "127.0.0.1"
            dashboard_port = 8787
        }
    }
    $cfg | ConvertTo-Json -Depth 6 | Set-Content -Path $configPath -Encoding UTF8

    $secrets = [ordered]@{
        account_password = $password
        api_keys         = if ($newsKey) { @{ default = $newsKey } } else { @{} }
    }
    $secretPath = Join-Path $dataDir "secrets.json"
    $secrets | ConvertTo-Json -Depth 4 | Set-Content -Path $secretPath -Encoding UTF8
    # Lock the secrets file down to this user and administrators only.
    try {
        $acl = Get-Acl $secretPath
        $acl.SetAccessRuleProtection($true, $false)
        foreach ($who in @("$env:USERDOMAIN\$env:USERNAME", "BUILTIN\Administrators")) {
            $acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule(
                $who, "FullControl", "Allow")))
        }
        Set-Acl -Path $secretPath -AclObject $acl
        Good "Password stored in $secretPath (readable only by you)"
    } catch {
        Warn "Could not tighten permissions on the password file."
    }
    Good "Configuration written to $configPath"
    Good "Mode: $mode   Normal risk: $baseRisk%   Maximum risk: $maxRisk%"
}

# ------------------------------------------------------------- 6. MT5 --------
Step "MetaTrader 5"

$terminalPaths = @(
    "C:\Program Files\MetaTrader 5\terminal64.exe",
    "C:\Program Files (x86)\MetaTrader 5\terminal64.exe"
) + (Get-ChildItem "C:\Program Files" -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like "*MetaTrader*" } |
        ForEach-Object { Join-Path $_.FullName "terminal64.exe" })

$terminal = $terminalPaths | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $terminal) {
    Warn "MetaTrader 5 was not found. Downloading the installer."
    try {
        $mt5Installer = Join-Path $env:TEMP "mt5setup.exe"
        Invoke-WebRequest -Uri "https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe" `
            -OutFile $mt5Installer -UseBasicParsing
        Say "    The MetaTrader 5 installer will now open."
        Say "    Accept the defaults, then log in with your account when it asks."
        Start-Process -FilePath $mt5Installer -Wait
        $terminal = $terminalPaths | Where-Object { Test-Path $_ } | Select-Object -First 1
    } catch {
        Bad "MetaTrader 5 could not be downloaded: $($_.Exception.Message)"
    }
}

if ($terminal) {
    Good "Terminal: $terminal"
    $raw = Get-Content $configPath -Raw | ConvertFrom-Json
    $raw.mt5_terminal_path = $terminal
    $raw | ConvertTo-Json -Depth 6 | Set-Content -Path $configPath -Encoding UTF8
    if (-not (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue)) {
        Start-Process -FilePath $terminal
        Start-Sleep -Seconds 20
    }
    if (Get-Process -Name "terminal64" -ErrorAction SilentlyContinue) {
        Good "MetaTrader 5 is running"
    } else {
        Bad "MetaTrader 5 did not start."
    }
} else {
    Bad "MetaTrader 5 is not installed. Install it, log in, then run this again."
}

# --------------------------------------------------- 7. MQL5 helper copy -----
$mql5Source = Join-Path $appDir "deploy\mql5"
if (Test-Path $mql5Source) {
    Step "Copying the MQL5 helper"
    $terminalData = Get-ChildItem "$env:APPDATA\MetaQuotes\Terminal" -Directory `
        -ErrorAction SilentlyContinue |
        Where-Object { Test-Path (Join-Path $_.FullName "MQL5") }
    foreach ($t in $terminalData) {
        $dest = Join-Path $t.FullName "MQL5\Experts\Mintel"
        New-Item -ItemType Directory -Force -Path $dest | Out-Null
        Copy-Item "$mql5Source\*" $dest -Recurse -Force
        Good "Copied to $dest"
    }
    if (-not $terminalData) { Warn "No MT5 data folder found for the MQL5 helper." }
}

# ------------------------------------------- 8. scheduled tasks (autostart) --
Step "Automatic restart after a reboot"

$startScript = Join-Path $appDir "deploy\START-BOT.ps1"
if ($NoAutoStart) {
    Warn "Skipped at your request (-NoAutoStart)."
} elseif (-not $isAdmin) {
    Warn "Administrator rights are needed to register the restart task."
    Warn "Re-run this script as administrator later to enable it."
} else {
    try {
        $action  = New-ScheduledTaskAction -Execute "powershell.exe" `
            -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$startScript`" -InstallDir `"$InstallDir`""
        $trigger = New-ScheduledTaskTrigger -AtStartup
        $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries -StartWhenAvailable `
            -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 2) `
            -ExecutionTimeLimit (New-TimeSpan -Days 0)
        Register-ScheduledTask -TaskName "MintelTrader" -Action $action `
            -Trigger $trigger -Settings $settings -RunLevel Highest `
            -User "SYSTEM" -Force | Out-Null
        Good "Scheduled task 'MintelTrader' registered - it starts at boot"

        # A second task keeps it honest every few minutes in case the boot task
        # ran before MT5 was ready.
        $keepAlive = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(3) `
            -RepetitionInterval (New-TimeSpan -Minutes 5)
        Register-ScheduledTask -TaskName "MintelKeepAlive" -Action $action `
            -Trigger $keepAlive -Settings $settings -RunLevel Highest `
            -User "SYSTEM" -Force | Out-Null
        Good "Scheduled task 'MintelKeepAlive' registered - it re-checks every 5 minutes"
    } catch {
        Bad "The scheduled tasks could not be registered: $($_.Exception.Message)"
    }
}

# -------------------------------------------------------- 9. start it all ----
Step "Starting the bot"
& powershell -NoProfile -ExecutionPolicy Bypass -File $startScript `
    -InstallDir $InstallDir -FromSetup

# ------------------------------------------------------------ 10. verify -----
Step "Verifying everything"

$verify = & $venvPy -m mintel.verify --config $configPath 2>&1
Say ($verify -join "`n")
if ($LASTEXITCODE -ne 0) {
    Bad "One or more checks failed - see the report above."
}

# ------------------------------------------------------------- 11. summary ---
Say ""
Say "=============================================================="
if ($script:Failures.Count -eq 0) {
    Say "  SETUP COMPLETE - THE BOT IS RUNNING" -ForegroundColor Green
} else {
    Say "  SETUP FINISHED WITH PROBLEMS" -ForegroundColor Red
}
Say "=============================================================="
Say ""
Say "  Status page :  http://127.0.0.1:8787"
Say "  Results page:  http://127.0.0.1:8787/results"
Say "  Logs        :  $logDir"
Say "  Data        :  $dataDir"
Say ""
Say "  To start or check it in future, run ONE command:"
Say "      powershell -ExecutionPolicy Bypass -File `"$startScript`""
Say ""
if ($script:Warnings.Count) {
    Say "  Warnings:" -ForegroundColor Yellow
    $script:Warnings | ForEach-Object { Say "    - $_" }
}
if ($script:Failures.Count) {
    Say "  Problems that need fixing:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Say "    - $_" }
    exit 1
}
exit 0
