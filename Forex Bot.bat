@echo off
rem Forex Bot: opens the app (platform server + MetaTrader 5 connector in one window).
rem The first time it sets up the Python environment and the shortcuts (a few minutes); a setup that did not
rem finish (no .venv\.forexbot-ok) is run again.
cd /d "%~dp0"
rem a folder the app moved away from (it says so in this file) starts nothing any more
if exist "MUTAT-LA-D-Forex-Bot.txt" (type "MUTAT-LA-D-Forex-Bot.txt" & pause & exit /b 1)
if not exist ".venv\.forexbot-ok" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "launcher\install.ps1" || (pause & exit /b 1)
)
start "" ".venv\Scripts\pythonw.exe" "launcher\forexbot.py"
