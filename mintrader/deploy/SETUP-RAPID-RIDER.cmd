@echo off
rem Double-click me. Runs SETUP-RAPID-RIDER.ps1 as administrator (needed for the
rem scheduled tasks that keep the bot running after a reboot).
net session >nul 2>&1
if %errorlevel% neq 0 (
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0SETUP-RAPID-RIDER.ps1"
pause
