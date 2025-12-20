#!/bin/bash

# Configuration
VENV_NAME="venv"

# 1. Check/Create Venv
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating virtual environment '$VENV_NAME'..."
    python3 -m venv $VENV_NAME
fi

# 2. Activate Venv
source "$VENV_NAME/bin/activate"

# 3. Check Dependencies in Venv
if ! python3 -c "import streamlit" &> /dev/null; then
    echo "Installing dependencies into virtual environment..."
    pip install --upgrade pip
    pip install -r requirements.txt
    playwright install
else
    echo "Dependencies already installed."
fi

# 4. Ingest data (optional)
read -p "Ingest pairing and IOE data now? (y/n): " ingest_choice
if [[ "$ingest_choice" == "y" || "$ingest_choice" == "Y" ]]; then
    ./venv/bin/python3 ingest_data.py
fi

# 5. Run App
echo "Starting NOC Mobile Scraper..."
streamlit run app.py
