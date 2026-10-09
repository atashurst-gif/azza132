@echo off
rem Double-click me. Runs STOP-BOT.ps1: stops the bot; open trades keep their broker stops.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0STOP-BOT.ps1"
pause
