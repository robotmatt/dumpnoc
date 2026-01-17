#!/bin/bash

# Configuration
VENV_NAME="venv"
PYTHON_BIN="python3"

# 1. Check/Create Venv
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating virtual environment '$VENV_NAME'..."
    $PYTHON_BIN -m venv "$VENV_NAME"
fi

# 2. Use venv's python directly for all subsequent commands
VENV_PYTHON="./$VENV_NAME/bin/python3"

# Ensure venv python exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: Virtual environment python not found at $VENV_PYTHON"
    echo "Cleaning up broken venv and retrying..."
    rm -rf "$VENV_NAME"
    $PYTHON_BIN -m venv "$VENV_NAME"
    if [ ! -f "$VENV_PYTHON" ]; then
        echo "Failed to create virtual environment. Please check your python installation."
        exit 1
    fi
fi

# 3. Check/Install Dependencies
if ! "$VENV_PYTHON" -c "import streamlit; import firebase_admin" &> /dev/null; then
    echo "Installing dependencies into virtual environment..."
    "$VENV_PYTHON" -m pip install --upgrade pip
    "$VENV_PYTHON" -m pip install -r requirements.txt
    "$VENV_PYTHON" -m playwright install chromium
else
    echo "Dependencies already installed."
fi

# 4. Ingest data (optional)
read -p "Ingest pairing and IOE data now? (y/n): " ingest_choice
if [[ "$ingest_choice" == "y" || "$ingest_choice" == "Y" ]]; then
    "$VENV_PYTHON" ingest_data.py
fi

# 5. Run App
echo "Starting NOC Mobile Scraper..."
"$VENV_PYTHON" -m streamlit run app.py
