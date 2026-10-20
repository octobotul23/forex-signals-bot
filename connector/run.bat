@echo off
rem First run creates a private Python environment, then starts the connector.
cd /d "%~dp0"
if not exist .venv (
  echo Setting up, this takes a minute the first time...
  py -3 -m venv .venv || python -m venv .venv
  .venv\Scripts\python.exe -m pip install --upgrade pip >nul
  .venv\Scripts\python.exe -m pip install -r requirements.txt
)
.venv\Scripts\python.exe connector.py %*
pause
