@echo off
title Fraud Analysis Tool - Launcher
echo ========================================
echo    Fraud Analysis Tool - Starting...
echo ========================================
echo.

cd /d "%~dp0"

echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://python.org
    pause
    exit /b 1
)

echo Starting Streamlit application...
echo.
echo The app will open in your default browser.
echo Keep this window open while using the app.
echo Press Ctrl+C to stop the server.
echo ========================================
echo.

python -m streamlit run src/app.py --server.headless true

pause
