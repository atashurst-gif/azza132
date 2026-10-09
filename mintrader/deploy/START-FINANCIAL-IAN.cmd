@echo off
rem Double-click me. Runs START-FINANCIAL-IAN.ps1: starts what is not running, then reports.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0START-FINANCIAL-IAN.ps1"
pause
