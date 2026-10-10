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

function Test-RiderSacked {
    # 10 Oct: was the Rapid Momentum Rider switched off on purpose? The
    # Formula 1 step ran on this machine (its marker), or rider.json says OFF.
    param($P)
    return ((Test-Path (Join-Path $P.Data ".formula1-2026-10-09")) -or ((Get-BotFileMode $P "rider") -eq "OFF"))
}

function Confirm-RiderLive {
    # 10 Oct: the Rapid Momentum Rider was switched OFF on 9 Oct (its own
    # 10-day test on real prices found no edge). SETUP-RAPID-RIDER put it
    # back on LIVE in one double-click; now, while it is off or once the
    # Formula 1 step has run (its marker), it asks first. Only the word LIVE,
    # typed as it is, goes on; anything else, or no answer, changes nothing.
    # $true: go on; $false: leave everything as it is (and say so).
    param($P)
    if (-not (Test-RiderSacked $P)) { return $true }
    Write-Host ""
    Write-Host "  The Rapid Momentum Rider was switched OFF on 9 Oct: its own 10-day test on real prices found no" -ForegroundColor Yellow
    Write-Host "  edge, so you switched it off and put Formula 1 live instead." -ForegroundColor Yellow
    Write-Host "  Putting it back on LIVE means it trades real money again."
    Write-Host ""
    $answer = ""
    try { $answer = Read-Host "  Type LIVE and press Enter to put it back on LIVE - anything else changes nothing" } catch { $answer = "" }
    if ("$answer".Trim() -ceq "LIVE") {
        Write-Host "  You typed LIVE: putting the Rapid Momentum Rider back on LIVE."
        # 10 Oct review: remember the answer, so the Formula 1 one-time step -
        # when it has not run on this machine yet - never switches it straight
        # back OFF a moment later (Set-Formula1Once leaves the Rider alone)
        try {
            if (-not (Test-Path $P.Data)) { New-Item -ItemType Directory -Force -Path $P.Data | Out-Null }
            Set-Content -Path (Join-Path $P.Data ".rider-live-confirmed") -Value (Get-Date).ToUniversalTime().ToString("s")
        } catch { }
        return $true
    }
    Write-Host ""
    Write-Host "  Nothing changed: the Rapid Momentum Rider stays OFF."
    return $false
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
    $tnbWords = Get-TnbModeWords $P
    $bots = @(
        @("tnb", "Trend & Breakout"), @("runner", "Momentum Runner"), @("rider", "Rapid Momentum Rider"),
        @("bandbreaker", "Band Breaker"), @("crowd", "Crowd Fader"), @("ian", "Financial Ian"))
    foreach ($b in $bots) {
        $key = $b[0]
        $label = $b[1]
        if ($key -eq "rider" -or $key -eq "ian") { $mode = Get-BotFileMode $P $key }
        elseif ($key -eq "tnb") { $mode = Get-ConfigBotMode $P "tnb" "LIVE" }
        else { $mode = Get-ConfigBotMode $P $key "PAPER" }
        if ($key -eq "tnb" -and "$tnbWords".StartsWith("PAPER - ")) { continue }    # part LIVE, part PAPER: its own line
        if ($key -eq "rider" -and $pip -and $mode -ne "OFF") { $label = "$label (GBP $pip a pip)" }
        if ($mode -eq "LIVE") { $live += $label }
        elseif ($mode -eq "PAPER") { $paper += $label }
        else { $off += $label }
    }
    $liveText = if ($live.Count) { $live -join ", " } else { "none" }
    $paperText = if ($paper.Count) { $paper -join ", " } else { "none" }
    Write-Host "    LIVE  - real orders on the account: $liveText"
    Write-Host "    PAPER - real prices, simulated orders, never in the account: $paperText"
    if ("$tnbWords".StartsWith("PAPER - ")) {
        Write-Host "    Trend & Breakout: $tnbWords - those trades are real orders on the account; the rest is simulated."
    }
    if ($off.Count) { Write-Host "    OFF   - not started: $($off -join ', ')" }
    if ((Get-BotFileMode $P "rider") -eq "OFF") {
        Write-Host "    The Rapid Momentum Rider is switched off (9 Oct: its own test on real prices found no edge)."
    }
    $runner = Get-ConfigBotMode $P "runner" "PAPER"
    if ($runner -eq "LIVE") { Write-Host "    The Momentum Runner rides Trend & Breakout's index entries with its own real orders." }
    elseif ($runner -eq "PAPER") { Write-Host "    The Momentum Runner rides Trend & Breakout's index entries on paper." }
    $risk = Get-RiskWords $P
    if ($risk) { Write-Host "    Trend & Breakout's money a trade: $risk." }
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
    # 10 Oct: Formula 1 already set (this line-up failed on an earlier run):
    # the same line-up, except that the Rider Aaron sacked on 9 Oct evening
    # stays OFF.
    if (Test-Path (Join-Path $P.Data ".formula1-2026-10-09")) {
        $modeArgs = @("--off", "scalper", "rider", "--paper", "tnb", "bandbreaker", "crowd", "--live", "runner", "ian")
    }
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
    if (Test-Path (Join-Path $P.Data ".formula1-2026-10-09")) {
        # 10 Oct: the Rider is sacked (Formula 1 is set): this older step must never switch it back on
        $path = Join-Path $P.Data ".rider-live-2026-10-09"
        if (-not (Test-Path $path)) { Set-Content -Path $path -Value (Get-Date).ToUniversalTime().ToString("s") }
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

function Get-TnbLiveGroups {
    # The kinds of market whose Trend & Breakout orders go to the account
    # while it is on PAPER (config.json "tnb" -> "live_groups"), e.g.
    # @("FX_MINOR"); empty when it is LIVE or none are set.
    param($P)
    if (-not (Test-Path $P.Config)) { return @() }
    try {
        $b = (Get-Content $P.Config -Raw | ConvertFrom-Json).tnb
        if ($null -eq $b -or "$($b.mode)".Trim().ToUpper() -ne "PAPER") { return @() }
        return @(@($b.live_groups) | Where-Object { "$_".Trim() } | ForEach-Object { "$_".Trim().ToUpper() })
    } catch { return @() }
}

function Get-TnbLiveTactics {
    # 10 Oct: the approaches whose orders go real on those markets
    # (config.json "tnb" -> "live_tactics"), e.g. @("MOMENTUM_CONTINUATION");
    # empty when every approach does. 10 Oct review: read as the trader reads
    # it - a name it does not trade is left out, and @("NONE") when the list
    # names no approach it trades (or is not a list): every market on paper.
    # The names are mintel/engine/tactics.py's APPROACH_NAMES (a test keeps
    # the two the same).
    param($P)
    $names = @("MOMENTUM_CONTINUATION", "TREND_PULLBACK", "BREAKOUT_ACCEPTANCE", "SESSION_EXPANSION",
               "SQUEEZE_RELEASE", "RANGE_REJECTION", "FAILED_BREAKOUT_RECLAIM", "LIQUIDITY_SWEEP_REVERSAL",
               "VWAP_REVERSION", "NEWS_CONTINUATION", "BREAKOUT_RETEST")
    if (-not (Test-Path $P.Config)) { return @() }
    try {
        $b = (Get-Content $P.Config -Raw | ConvertFrom-Json).tnb
        if ($null -eq $b -or $null -eq $b.live_tactics) { return @() }
        $t = $b.live_tactics
        if ($t -is [string]) {
            if ($t -eq "") { return @() }
            $t = @($t)
        } elseif (-not ($t -is [System.Array])) {
            return @("NONE")
        }
        $known = @()
        foreach ($x in @($t)) {
            $a = "$x".Trim().ToUpper()
            if ($a.EndsWith("_2X")) { $a = $a.Substring(0, $a.Length - 3) }
            if (($names -ccontains $a) -and -not ($known -ccontains $a)) { $known += $a }
        }
        if (@($t).Count -gt 0 -and $known.Count -eq 0) { return @("NONE") }
        return $known
    } catch { return @() }
}

function Get-TnbModeWords {
    # Trend & Breakout's mode in plain words: LIVE, PAPER, or "PAPER - minor
    # currency pairs LIVE (Formula 1)" when some kinds of market still trade
    # for real (9 Oct: never plain PAPER while some of its orders are real).
    param($P)
    $mode = Get-ConfigBotMode $P "tnb" "LIVE"
    $groups = @(Get-TnbLiveGroups $P)
    if ($mode -ne "PAPER" -or $groups.Count -eq 0) { return $mode }
    $words = @()
    $f1 = ""
    foreach ($g in $groups) {
        if ($g -eq "FX_MINOR") { $words += "minor currency pairs"; $f1 = " (Formula 1)" }
        elseif ($g -eq "FX_MAJOR") { $words += "major currency pairs" }
        elseif ($g -eq "INDEX") { $words += "indices" }
        else { $words += $g.ToLower().Replace("_", " ") }
    }
    # 10 Oct: with tnb.live_tactics, "PAPER - minor currency pairs LIVE for momentum continuation only (Formula 1)"
    $tactics = @(Get-TnbLiveTactics $P)
    if ($tactics.Count -eq 1 -and $tactics[0] -eq "NONE") {
        # 10 Oct review: a list naming no approach it trades - the trader keeps every market on paper
        return "PAPER - every market (tnb.live_tactics names no approach it trades)"
    }
    $only = ""
    if ($tactics.Count -gt 0) {
        $only = " for " + (($tactics | ForEach-Object { $_.ToLower().Replace("_", " ") }) -join " and ") + " only"
        if (-not ($tactics.Count -eq 1 -and $tactics[0] -eq "MOMENTUM_CONTINUATION")) { $f1 = "" }
    }
    return "PAPER - $($words -join ', ') LIVE$only$f1"
}

function Get-RiskWords {
    # The money a trade from config.json's risk block, as "0.7% of the
    # balance a trade, at most GBP 13"; "" when it says nothing.
    param($P)
    if (-not (Test-Path $P.Config)) { return "" }
    try {
        $r = (Get-Content $P.Config -Raw | ConvertFrom-Json).risk
        if ($null -eq $r -or $null -eq $r.base_risk_pct) { return "" }
        $text = "$([double]$r.base_risk_pct)% of the balance a trade"
        if ($r.max_risk_money) { $text += ", at most GBP $([double]$r.max_risk_money)" }
        return $text
    } catch { return "" }
}

function Invoke-ModesOrSay {
    # One call of the modes command for Set-Formula1Once: quiet when it
    # works; when it does not, its own words and a [WARN] with the exact
    # command to type by hand. Returns $true when it changed what was asked.
    param($P, [string[]] $Arguments)
    $r = Invoke-MintelPython $P (@("-m", "mintel.ops.modes", "--config", $P.Config) + $Arguments)
    if ($r.Code -eq 0) { return $true }
    foreach ($l in $r.Lines) {
        if ($l -notmatch "^Restart the bot") { Write-Host "      $l" }
    }
    Write-Host "    [WARN] Could not run: $($Arguments -join ' '). Run it by hand: cd `"$($P.App)`"; & `"$($P.Py)`" -m mintel.ops.modes --config `"$($P.Config)`" $($Arguments -join ' ')" -ForegroundColor Yellow
    return $false
}

function Set-Formula1Once {
    # 9 Oct, about 19:30 UK, Aaron: "Sack the rider off and let's get trend
    # and breakout live risking 0.2 higher pip average per trade", then "Just
    # Formula 1" and "About GBP 13 a trade". The Rapid Momentum Rider is
    # switched OFF (its own 10-day test on real prices found no edge); Trend &
    # Breakout stays on PAPER except its minor currency pair trades (EURGBP,
    # AUDJPY and similar: Formula 1), which go to the account with real
    # money - the rest stays on paper and still feeds the Momentum Runner;
    # the money a trade becomes 0.7% of the balance (was 0.5%), at most GBP
    # 13 (was 10). No other risk limit and no other bot's money a trade is
    # touched. After every other 9 Oct step, so on a fresh install it has the
    # last word; once per machine (marker, written only when every call
    # worked), so a later choice made with the modes command is never undone.
    # Returns $true when it changed something (the bots must restart).
    param($P)
    $marker = Join-Path $P.Data ".formula1-2026-10-09"
    if (Test-Path $marker) { return $false }
    if (-not (Test-MintelInstalled $P)) { return $false }
    Write-Host "    Formula 1 live, the Rapid Momentum Rider off:"
    $done = 0
    $fails = 0
    $calls = @(@("--off", "rider"), @("--tnb-live-groups", "FX_MINOR"), @("--risk-pct", "0.7", "--risk-money", "13"))
    $riderWords = "OFF"
    if (Test-Path (Join-Path $P.Data ".rider-live-confirmed")) {
        # 10 Oct review: Aaron typed LIVE for the Rider at the SETUP-RAPID-RIDER
        # question (Confirm-RiderLive), after being told why it was switched
        # off: that later choice stands, so the Rider is left as it is
        $calls = @($calls[1], $calls[2])
        $riderWords = "left as you chose (you typed LIVE for it)"
        Write-Host "    Rapid Momentum Rider: left as you chose - you typed LIVE for it."
    }
    foreach ($call in $calls) {
        if (Invoke-ModesOrSay $P $call) { $done += 1 } else { $fails += 1 }
    }
    if ($fails -gt 0) {
        Write-Host "    [WARN] Formula 1 is not fully set ($fails of $($calls.Count) steps failed, see above). Run SETUP-AND-START.cmd again, or the command shown." -ForegroundColor Yellow
        return ($done -gt 0)
    }
    Set-Content -Path $marker -Value (Get-Date).ToUniversalTime().ToString("s")
    Write-Host "    [ OK ] Rapid Momentum Rider: $riderWords. Trend & Breakout: minor currency pairs LIVE (Formula 1), the rest on PAPER. Risk: 0.7% of the balance a trade, at most GBP 13" -ForegroundColor Green
    return $true
}

function Set-Formula1ChoicesOnce {
    # 10 Oct, Aaron: "fine to go with your suggestions" - yes to the four
    # questions put to him that morning:
    #   1. only Formula 1's momentum continuation trades on minor currency
    #      pairs use real money (--tnb-live-tactics MOMENTUM_CONTINUATION);
    #   2. a flat GBP 13 a trade, no top-opportunity size, until Formula 1
    #      has 20 live trades (--top-size off);
    #   3. no new Trend & Breakout entry in the 15 minutes before a
    #      high-importance release on either currency (--news-before-minutes 15);
    #   4. the daily-loss stop counts real money only (--daily-loss-practice
    #      ignore); its 3% limit is not touched.
    # One call of the modes command, which checks every file first and
    # writes nothing when it refuses. Right after Set-Formula1Once, and only
    # once Formula 1 itself is set (its marker). Once per machine (marker,
    # written only when the call worked), so a later choice made with the
    # modes command is never undone. When Formula 1 is not live here just now
    # (its minor pairs taken off LIVE by hand, or Trend & Breakout put fully
    # LIVE), the approach choice cannot apply - the modes command refuses it
    # with nothing live - so the other three are set without it, and it says
    # so plainly with the command that puts Formula 1 back on with it. (10 Oct
    # review: fully LIVE has its own warning - there every trade of Trend &
    # Breakout's is real money, and its command puts it back on PAPER first.)
    # Returns $true when it changed something (the trader must restart).
    param($P)
    $marker = Join-Path $P.Data ".formula1-choices-2026-10-10"
    if (Test-Path $marker) { return $false }
    if (-not (Test-MintelInstalled $P)) { return $false }
    if (-not (Test-Path (Join-Path $P.Data ".formula1-2026-10-09"))) { return $false }
    Write-Host "    Your choices for Formula 1:"
    $rest = @("--top-size", "off", "--news-before-minutes", "15", "--daily-loss-practice", "ignore")
    $groups = @(Get-TnbLiveGroups $P)
    $tactics = @(Get-TnbLiveTactics $P)
    $liveNow = ($groups.Count -gt 0) -and -not ($tactics.Count -eq 1 -and $tactics[0] -eq "NONE")
    # 10 Oct review: put fully LIVE by hand, EVERY Trend & Breakout trade is real (the trader reads
    # anything but PAPER as LIVE) - a different warning
    $allLive = ((Get-ConfigBotMode $P "tnb" "LIVE") -ne "PAPER")
    if ($liveNow) { $modeArgs = @("--tnb-live-tactics", "MOMENTUM_CONTINUATION") + $rest } else { $modeArgs = $rest }
    $r = Invoke-MintelPython $P (@("-m", "mintel.ops.modes", "--config", $P.Config) + $modeArgs)
    if ($r.Code -ne 0) {
        foreach ($l in $r.Lines) {
            if ($l -notmatch "^Restart the bot") { Write-Host "      $l" }
        }
        Write-Host "    [WARN] Could not set your choices for Formula 1 (nothing was changed). Run it by hand: cd `"$($P.App)`"; & `"$($P.Py)`" -m mintel.ops.modes --config `"$($P.Config)`" $($modeArgs -join ' ')" -ForegroundColor Yellow
        return $false
    }
    Set-Content -Path $marker -Value (Get-Date).ToUniversalTime().ToString("s")
    Write-Host "    [ OK ] Your choices for Formula 1 are set:" -ForegroundColor Green
    foreach ($l in $r.Lines) {
        if ($l -notmatch "^Restart the bot") { Write-Host "      $l" }
    }
    if ($allLive) {
        Write-Host "    [WARN] Trend & Breakout is fully LIVE on this computer just now (put there by hand with the modes command): every trade of it, minor pairs included, is real money, so the first choice - only its momentum continuation trades with real money - cannot apply until it is back on PAPER. To put it back on PAPER with Formula 1 and that choice: cd `"$($P.App)`"; & `"$($P.Py)`" -m mintel.ops.modes --config `"$($P.Config)`" --paper tnb --tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" -ForegroundColor Yellow
    } elseif (-not $liveNow) {
        Write-Host "    [WARN] Formula 1 is not live on this computer just now (no minor-pair trade of Trend & Breakout goes to the real broker - changed by hand with the modes command), so the first choice - only its momentum continuation trades with real money - was not applied. To put Formula 1 back on live with it: cd `"$($P.App)`"; & `"$($P.Py)`" -m mintel.ops.modes --config `"$($P.Config)`" --tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" -ForegroundColor Yellow
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
    # 9 Oct: the Rider is switched OFF (its own test found no edge): no more
    # research runs for it, and nothing to say about that
    if ((Get-BotFileMode $P "rider") -eq "OFF") { return }
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
