@echo off
set VENV_NAME=venv

:: 1. Check/Create Venv
if not exist %VENV_NAME% (
    echo Creating virtual environment '%VENV_NAME%'...
    python -m venv %VENV_NAME%
)

:: 2. Activate Venv
call %VENV_NAME%\Scripts\activate

:: 3. Check Dependencies in Venv
python -c "import streamlit" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Installing dependencies into virtual environment...
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    playwright install
) else (
    echo Dependencies already installed.
)

:: 4. Ingest data (optional)
set /p ingest_choice="Ingest pairing and IOE data now? (y/n): "
if /i "%ingest_choice%"=="y" (
    python ingest_data.py
)

:: 5. Run App
echo Starting NOC Mobile Scraper...
streamlit run app.py
