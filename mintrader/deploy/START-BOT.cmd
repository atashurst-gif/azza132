@echo off
rem Double-click me. Runs START-BOT.ps1: starts what is not running, then reports.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0START-BOT.ps1"
pause
