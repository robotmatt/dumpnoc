@echo off
setlocal EnableDelayedExpansion

:: 0. Find a working Python command
set PY_CMD=python
py --version >nul 2>&1
if !ERRORLEVEL! equ 0 (
    set PY_CMD=py
)
echo [INFO] Using Python command: !PY_CMD!

:: 1. Check/Install Dependencies
echo [INFO] Checking dependencies...
!PY_CMD! -c "import streamlit; import firebase_admin; import sqlalchemy; import pandas; import playwright; import bs4; import google.cloud.firestore" >nul 2>&1
if !ERRORLEVEL! neq 0 (
    echo [INFO] Installing missing dependencies globally...
    !PY_CMD! -m pip install --upgrade pip
    !PY_CMD! -m pip install -r requirements.txt
    !PY_CMD! -m playwright install
) else (
    echo [INFO] Dependencies are already installed.
)

:: 2. Ingest data (optional)
set /p ingest_choice="Ingest pairing and IOE data now? (y/n): "
if /i "!ingest_choice!"=="y" (
    !PY_CMD! ingest_data.py
)

:: 3. Run Streamlit App
echo [INFO] Starting NOC Mobile Scraper...
!PY_CMD! -m streamlit run app.py
