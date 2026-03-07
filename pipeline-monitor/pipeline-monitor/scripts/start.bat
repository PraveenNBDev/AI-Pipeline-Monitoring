@echo off
REM ─────────────────────────────────────────────────────────────
REM  DataPulse — AI Pipeline Monitor
REM  Windows Start Script
REM ─────────────────────────────────────────────────────────────

echo.
echo   ⚡ DataPulse — AI Pipeline Monitor
echo   ────────────────────────────────────

SET SCRIPT_DIR=%~dp0..
SET BACKEND_DIR=%SCRIPT_DIR%\backend
SET ENV_FILE=%SCRIPT_DIR%\.env

REM ── Check Python ──────────────────────────────────────────────
python --version >nul 2>&1
IF ERRORLEVEL 1 (
  echo   ✗ Python not found. Install from https://python.org
  pause
  exit /b 1
)

REM ── Create .env if not exists ─────────────────────────────────
IF NOT EXIST "%ENV_FILE%" (
  copy "%SCRIPT_DIR%\.env.example" "%ENV_FILE%"
  echo   ⚠  .env created. Please edit it and add your ANTHROPIC_API_KEY
  notepad "%ENV_FILE%"
)

REM ── Create virtualenv if needed ───────────────────────────────
IF NOT EXIST "%SCRIPT_DIR%\.venv" (
  echo   Creating virtual environment...
  python -m venv "%SCRIPT_DIR%\.venv"
)

CALL "%SCRIPT_DIR%\.venv\Scripts\activate.bat"
echo   ✓ Virtual environment activated

REM ── Install dependencies ──────────────────────────────────────
echo   Installing backend dependencies...
pip install -q -r "%BACKEND_DIR%\requirements.txt"
echo   ✓ Dependencies installed

REM ── Start server ──────────────────────────────────────────────
echo.
echo   🚀 Starting DataPulse on http://localhost:8000
echo.
echo   Open your browser at: http://localhost:8000
echo   Press Ctrl+C to stop
echo.

cd /d "%BACKEND_DIR%"
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

pause
