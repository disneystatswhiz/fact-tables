@echo off
setlocal ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
REM Always start in the folder of this .bat
cd /d "%~dp0"

REM Encoding: avoid cp1252 issues
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM Create venv if missing
if not exist "venv\Scripts\python.exe" (
  echo [INFO] Creating venv...
  python -m venv venv
)

REM Activate venv
call "venv\Scripts\activate.bat"

REM Install deps if requirements.txt exists
if exist requirements.txt (
  echo [INFO] Ensuring dependencies...
  python -m pip install -r requirements.txt -q
)

REM Run the pipeline
echo [INFO] Running pipeline...
python src\main.py
set EXITCODE=%ERRORLEVEL%

REM Surface latest log file
for /f "delims=" %%F in ('dir /b /o-d logs\run_*.log 2^>nul') do (
  echo [INFO] Log: logs\%%F
  goto :afterlog
)
:afterlog

exit /b %EXITCODE%
