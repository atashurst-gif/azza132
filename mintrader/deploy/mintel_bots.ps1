#requires -Version 5.1
<#
.SYNOPSIS
    Shared helpers for the Rapid Momentum Rider and Financial Ian scripts.

.DESCRIPTION
    Dot-sourced by SETUP-RAPID-RIDER.ps1, START-RAPID-RIDER.ps1,
    INSTALL-FINANCIAL-IAN.ps1, START-FINANCIAL-IAN.ps1, START-BOT.ps1 and
    SETUP-AND-START.ps1. Nothing here starts a process of its own except the
    once-a-day research run: the watchdog (started by START-BOT.ps1) starts
    the Rider and Ian, and each refuses to run twice.

    Every mode change goes through the modes command
    (python -m mintel.ops.modes), which checks every file before it writes
    anything and refuses a bot whose magic number is not its own.
#>

function Get-MintelPaths {
    param([string] $InstallDir = "C:\mintel")
    $data = Join-Path $InstallDir "data"
    return @{
        Install = $InstallDir
        App     = Join-Path $InstallDir "app"
        Data    = $data
        Logs    = Join-Path $InstallDir "logs"
        Py      = Join-Path $InstallDir "venv\Scripts\python.exe"
        Config  = Join-Path $data "config.json"
        Secrets = Join-Path $data "secrets.json"
    }
}

function Test-MintelInstalled {
    param($P)
    return (Test-Path $P.Py) -and (Test-Path $P.Config)
}

function Invoke-MintelPython {
    # Runs the environment's Python in the program folder and returns
    # @{ Code = exit code; Lines = every line it printed }. Never throws:
    # a Python message on stderr is just another line here.
    param($P, [string[]] $Arguments)
    $ErrorActionPreference = "Continue"
    $old = Get-Location
    try {
        Set-Location $P.App
        $lines = & $P.Py @Arguments 2>&1 | ForEach-Object { "$_" }
        $code = $LASTEXITCODE
    } catch {
        $lines = @("$($_.Exception.Message)")
        $code = 1
    } finally {
        Set-Location $old
    }
    return @{ Code = $code; Lines = @($lines) }
}

function Invoke-Modes {
    # python -m mintel.ops.modes --config <config> <arguments>, its lines
    # printed indented (without the restart reminder: the scripts restart
    # what needs it themselves). Returns $true when it changed what was asked.
    param($P, [string[]] $Arguments)
    $r = Invoke-MintelPython $P (@("-m", "mintel.ops.modes", "--config", $P.Config) + $Arguments)
    foreach ($l in $r.Lines) {
        if ($l -notmatch "^Restart the bot") { Write-Host "      $l" }
    }
    return ($r.Code -eq 0)
}

function Get-BotFileMode {
    # The mode in data\<name>.json: LIVE, PAPER or OFF; PAPER when the file or
    # the mode is missing or not readable (the bots' own default).
    param($P, [string] $Name)
    $path = Join-Path $P.Data "$Name.json"
    if (-not (Test-Path $path)) { return "PAPER" }
    try {
        $m = "$((Get-Content $path -Raw | ConvertFrom-Json).mode)".Trim().ToUpper()
        if (@("OFF", "PAPER", "LIVE") -contains $m) { return $m }
    } catch { }
    return "PAPER"
}

function Get-BotLine {
    # One plain line on the Rider (rider) or Ian (ian) from its own status
    # file: @{ Code = 0 running / 2 OFF / 1 anything else; Line = the line }.
    param($P, [string] $Name)
    $r = Invoke-MintelPython $P @("-m", "mintel.ops.modes", "--config", $P.Config, "--check", $Name)
    $line = ($r.Lines | Select-Object -Last 1)
    if (-not $line) { $line = "$Name : no answer yet" }
    return @{ Code = $r.Code; Line = "$line" }
}

function Wait-BotLine {
    # Wait (at most $Seconds) for the bot to report in its current mode; the
    # last line it gave.
    param($P, [string] $Name, [int] $Seconds = 120)
    $waited = 0
    while ($true) {
        $b = Get-BotLine $P $Name
        if ($b.Code -eq 2) { return $b }
        if ($b.Code -eq 0 -and $b.Line -notmatch "restart it for") { return $b }
        if ($waited -ge $Seconds) { return $b }
        Start-Sleep -Seconds 5
        $waited += 5
    }
}

function Test-BotRunning {
    param($P, [string] $Name)
    $pidFile = Join-Path $P.Data "$Name.pid"
    if (-not (Test-Path $pidFile)) { return $false }
    $raw = "$(Get-Content $pidFile -Raw)".Trim()
    if (-not $raw) { return $false }
    try { return $null -ne (Get-Process -Id ([int]$raw) -ErrorAction Stop) } catch { return $false }
}

function Stop-Bot {
    # Stop one bot process by its pid file. With the watchdog running it is
    # started again within a check; its open trades keep their broker stops.
    param($P, [string] $Name)
    $pidFile = Join-Path $P.Data "$Name.pid"
    if (-not (Test-Path $pidFile)) { return $false }
    $raw = "$(Get-Content $pidFile -Raw)".Trim()
    try {
        Stop-Process -Id ([int]$raw) -Force -ErrorAction Stop
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        return $true
    } catch {
        return $false
    }
}

function Restart-BotIfModeChanged {
    # A running bot reads its mode at start: when its file now says another
    # mode, stop that one process and let the watchdog start it again.
    param($P, [string] $Name)
    $b = Get-BotLine $P $Name
    if ($b.Line -match "restart it for" -and (Test-BotRunning $P $Name)) {
        Write-Host "    Restarting $Name so it runs in its new mode (the watchdog starts it again)."
        [void](Stop-Bot $P $Name)
        Start-Sleep -Seconds 3
    }
}

function Test-RiderHasPipValue {
    param($P)
    $path = Join-Path $P.Data "rider.json"
    if (-not (Test-Path $path)) { return $false }
    try { return [bool]((Get-Content $path -Raw | ConvertFrom-Json).user_pip_value_gbp) } catch { return $false }
}

function Set-RiderLive {
    # The Rapid Momentum Rider LIVE in place of the retired Rapid Scalper.
    # -SetPip writes GBP 1 a pip (Aaron's instruction for the first switch);
    # otherwise GBP 1 only when rider.json names no figure yet, because once
    # set the figure is Aaron's.
    param($P, [switch] $SetPip)
    $modeArgs = @("--off", "scalper", "--live", "rider")
    if ($SetPip -or -not (Test-RiderHasPipValue $P)) { $modeArgs += @("--gbp-per-pip", "1.0") }
    $ok = Invoke-Modes $P $modeArgs
    if ($ok) {
        Set-Content -Path (Join-Path $P.Data ".rider-live-2026-10-09") -Value (Get-Date).ToUniversalTime().ToString("s")
    }
    return $ok
}

function Enable-Ian {
    # Financial Ian on: PAPER unless it is already LIVE. Without a data-feed
    # key it runs DATA-DEGRADED - no signals, no trades.
    param($P)
    $ok = $true
    if ((Get-BotFileMode $P "ian") -ne "LIVE") { $ok = Invoke-Modes $P @("--paper", "ian") }
    if ($ok) {
        Set-Content -Path (Join-Path $P.Data ".ian-enabled-2026-10-09") -Value (Get-Date).ToUniversalTime().ToString("s")
    }
    return $ok
}

function Get-ConfigBotMode {
    # The mode a bot is set to in config.json ("runner", "bandbreaker",
    # "crowd" or "tnb" -> "mode"): LIVE, PAPER or OFF; $Default when the file
    # or the block says nothing (PAPER, the bots' own default; LIVE for
    # Trend & Breakout, "tnb").
    param($P, [string] $Block, [string] $Default = "PAPER")
    if (-not (Test-Path $P.Config)) { return $Default }
    try {
        $raw = Get-Content $P.Config -Raw | ConvertFrom-Json
        $b = $raw.$Block
        if ($null -eq $b) { return $Default }
        $m = "$($b.mode)".Trim().ToUpper()
        if (@("OFF", "PAPER", "LIVE") -contains $m) { return $m }
    } catch { }
    return $Default
}

function Get-RiderPipValue {
    # The Rapid Momentum Rider's GBP per pip from data\rider.json, or "".
    param($P)
    $path = Join-Path $P.Data "rider.json"
    if (-not (Test-Path $path)) { return "" }
    try {
        $v = (Get-Content $path -Raw | ConvertFrom-Json).user_pip_value_gbp
        if ($v) { return ("{0:N2}" -f [double]$v) }
    } catch { }
    return ""
}

function Write-LineUp {
    # Which bots are LIVE and which are PAPER, in plain English, read back
    # from the files the bots themselves read.
    param($P)
    $live = @()
    $paper = @()
    $off = @()
    $pip = Get-RiderPipValue $P
    $bots = @(
        @("tnb", "Trend & Breakout"), @("runner", "Momentum Runner"), @("rider", "Rapid Momentum Rider"),
        @("bandbreaker", "Band Breaker"), @("crowd", "Crowd Fader"), @("ian", "Financial Ian"))
    foreach ($b in $bots) {
        $key = $b[0]
        $label = $b[1]
        if ($key -eq "rider" -or $key -eq "ian") { $mode = Get-BotFileMode $P $key }
        elseif ($key -eq "tnb") { $mode = Get-ConfigBotMode $P "tnb" "LIVE" }
        else { $mode = Get-ConfigBotMode $P $key "PAPER" }
        if ($key -eq "rider" -and $pip) { $label = "$label (GBP $pip a pip)" }
        if ($mode -eq "LIVE") { $live += $label }
        elseif ($mode -eq "PAPER") { $paper += $label }
        else { $off += $label }
    }
    $liveText = if ($live.Count) { $live -join ", " } else { "none" }
    $paperText = if ($paper.Count) { $paper -join ", " } else { "none" }
    Write-Host "    LIVE  - real orders on the account: $liveText"
    Write-Host "    PAPER - real prices, simulated orders, never in the account: $paperText"
    if ($off.Count) { Write-Host "    OFF   - not started: $($off -join ', ')" }
    $runner = Get-ConfigBotMode $P "runner" "PAPER"
    if ($runner -eq "LIVE") { Write-Host "    The Momentum Runner rides Trend & Breakout's index entries with its own real orders." }
    elseif ($runner -eq "PAPER") { Write-Host "    The Momentum Runner rides Trend & Breakout's index entries on paper." }
    Write-Host "    The Rapid Scalper stays retired (OFF)."
    if ((Get-BotFileMode $P "ian") -eq "LIVE") {
        Write-Host "    Financial Ian can only trade once a CME data feed is configured (INSTALL-FINANCIAL-IAN.cmd)."
    }
}

function Set-LineUp {
    # 9 Oct evening, Aaron's decision. LIVE: the Momentum Runner (the
    # winner), the Rapid Momentum Rider (GBP 1.00 a pip) and Financial Ian
    # (it can only trade once a CME data feed is configured). PAPER: Trend &
    # Breakout (real prices, simulated orders; the Momentum Runner still
    # rides its index entries), the Band Breaker and the Crowd Fader. The
    # retired Rapid Scalper stays OFF. The Rider's figure is set to GBP 1.00
    # only when rider.json names none (once set, it is Aaron's).
    param($P)
    $modeArgs = @("--off", "scalper", "--paper", "tnb", "bandbreaker", "crowd", "--live", "runner", "rider", "ian")
    if (-not (Test-RiderHasPipValue $P)) { $modeArgs += @("--gbp-per-pip", "1.0") }
    $ok = Invoke-Modes $P $modeArgs
    if ($ok) {
        Set-Content -Path (Join-Path $P.Data ".lineup-2026-10-09") -Value (Get-Date).ToUniversalTime().ToString("s")
    }
    return $ok
}

function Invoke-LineUpOnce {
    # 9 Oct, once per machine (marker files): the Rapid Momentum Rider LIVE
    # at GBP 1 a pip in place of the retired Rapid Scalper, Financial Ian in
    # PAPER, and then - last, so it has the final word on a fresh install -
    # the evening's line-up (Set-LineUp). Once the line-up is set the older
    # steps never run again, and a later choice made with the modes command
    # is never undone. Returns $true when a mode was changed (restart).
    param($P)
    $changed = $false
    $lineUp = Join-Path $P.Data ".lineup-2026-10-09"
    if (Test-Path $lineUp) {
        foreach ($m in @(".rider-live-2026-10-09", ".ian-enabled-2026-10-09")) {
            $path = Join-Path $P.Data $m
            if (-not (Test-Path $path)) { Set-Content -Path $path -Value (Get-Date).ToUniversalTime().ToString("s") }
        }
        return $false
    }
    if (-not (Test-Path (Join-Path $P.Data ".rider-live-2026-10-09"))) {
        Write-Host "    The Rapid Momentum Rider replaces the Rapid Scalper:"
        if (Set-RiderLive $P -SetPip) { Write-Host "    [ OK ] Rapid Scalper retired (OFF); Rapid Momentum Rider LIVE at GBP 1.00 a pip" -ForegroundColor Green }
        else { Write-Host "    [WARN] the Rapid Momentum Rider could not be switched on (see above)" -ForegroundColor Yellow }
    }
    if (-not (Test-Path (Join-Path $P.Data ".ian-enabled-2026-10-09"))) {
        Write-Host "    Financial Ian:"
        if (Enable-Ian $P) { Write-Host "    [ OK ] Financial Ian: $(Get-BotFileMode $P 'ian') (DATA-DEGRADED, no trades, until a data-feed key is saved)" -ForegroundColor Green }
        else { Write-Host "    [WARN] Financial Ian could not be switched on (see above)" -ForegroundColor Yellow }
    }
    Write-Host "    The bots' line-up:"
    if (Set-LineUp $P) {
        $changed = $true
        Write-Host "    [ OK ] The line-up is set:" -ForegroundColor Green
        Write-LineUp $P
    } else {
        Write-Host "    [WARN] the line-up could not be set (see above). Run SETUP-AND-START.cmd again." -ForegroundColor Yellow
    }
    return $changed
}

function Set-IanBinanceOnce {
    # 9 Oct, Aaron: "I don't have the Databento key, just do anything closest
    # so we can get it live". Unless a Databento key is already saved (then the
    # feed is the CME FX book through Databento - set now if ian.json did not
    # name it yet), Financial Ian reads Binance's PUBLIC crypto order
    # book (no account, no key) and trades BTC and ETH through the broker's
    # crypto CFDs; Ian stays LIVE. Only data\ian.json is touched - never
    # another bot's mode. Once per machine (marker), after the line-up.
    # Returns $true when it changed something (Ian must restart to read it).
    param($P)
    $marker = Join-Path $P.Data ".ian-binance-2026-10-09"
    if (Test-Path $marker) { return $false }
    if (-not (Test-MintelInstalled $P)) { return $false }
    Write-Host "    Financial Ian's data feed:"
    $r = Invoke-MintelPython $P @("-m", "mintel.ian", "--config", $P.Config, "--free-feed")
    $feed = "$($r.Lines | Select-Object -Last 1)".Trim()
    if ($r.Code -ne 0 -or ($feed -ne "binance" -and $feed -ne "cme")) {
        Write-Host "    [WARN] Financial Ian's data feed could not be set ($feed). Set `"feed`": {`"vendor`": `"binance`"} in $($P.Data)\ian.json" -ForegroundColor Yellow
        return $false
    }
    if ((Get-BotFileMode $P "ian") -ne "LIVE") {
        if (-not (Invoke-Modes $P @("--live", "ian"))) {
            Write-Host "    [WARN] Financial Ian could not be switched to LIVE (see above)" -ForegroundColor Yellow
        }
    }
    Set-Content -Path $marker -Value (Get-Date).ToUniversalTime().ToString("s")
    $mode = Get-BotFileMode $P "ian"
    if ($feed -eq "binance") {
        Write-Host "    [ OK ] Financial Ian: $mode on Binance's public crypto order book, trading BTC and ETH through IC Markets CFDs (no key needed). The CME FX feed can be added later." -ForegroundColor Green
    } else {
        Write-Host "    [ OK ] Financial Ian: $mode, its feed set to the CME FX futures book through Databento (a Databento key is saved)." -ForegroundColor Green
    }
    return $true
}

function Start-RiderResearch {
    # The Rapid Momentum Rider's research on the last ten days of the
    # broker's REAL ticks, in the background, at most once a day (marker),
    # uploaded to GitHub - so real-tick results arrive without anyone typing
    # anything. Skipped when today's summary is already there. It must never
    # starve the trading bots: one worker process (--workers 1) at the
    # lowest priority Windows has (Idle; BelowNormal if Idle is refused),
    # logging to logs\rider-research.log.
    param($P)
    $today = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
    if (Test-Path (Join-Path $P.Data "rider-research\$today\summary.json")) { return }
    $marker = Join-Path $P.Data ".rider-research-$today"
    if (Test-Path $marker) { return }
    $module = Join-Path $P.App "mintel\rider\research.py"
    $package = Join-Path $P.App "mintel\rider\research\__main__.py"
    if (-not ((Test-Path $module) -or (Test-Path $package))) { return }     # not in this version
    Set-Content -Path $marker -Value (Get-Date).ToUniversalTime().ToString("s")
    try {
        $proc = Start-Process -FilePath $P.Py -WindowStyle Hidden -WorkingDirectory $P.App -PassThru `
            -ArgumentList @("-m", "mintel.rider.research", "--config", "`"$($P.Config)`"", "--days", "10", "--upload", "--workers", "1") `
            -RedirectStandardOutput (Join-Path $P.Logs "rider-research.log") `
            -RedirectStandardError  (Join-Path $P.Logs "rider-research.err.log")
        $priority = "Idle"
        try { $proc.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::Idle }
        catch {
            $priority = "BelowNormal"
            try { $proc.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal } catch { $priority = "Normal" }
        }
        Write-Host "    [ OK ] Rapid Momentum Rider research on the last 10 days of real ticks started in the background ($priority priority, one worker, so the bots come first)" -ForegroundColor Green
    } catch {
        Write-Host "    [WARN] the Rider research could not start: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function Write-BotLines {
    # One line each for the two bots that run as their own processes.
    param($P)
    foreach ($name in @("rider", "ian")) {
        $b = Get-BotLine $P $name
        $colour = if ($b.Code -eq 0) { "Green" } elseif ($b.Code -eq 2) { "Gray" } else { "Yellow" }
        Write-Host "    $($b.Line)" -ForegroundColor $colour
    }
}
