@echo off
rem FINANCIAL IAN - TEST ON PAST DATA. Double-click me.
rem Runs TEST-FINANCIAL-IAN.ps1: shows Databento's price for past CME data first and
rem buys nothing unless you type y. No administrator rights are needed: it never
rem touches the running bots, and the test runs at Idle priority.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0TEST-FINANCIAL-IAN.ps1" %*
pause
