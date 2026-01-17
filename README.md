# NOC Mobile Scraper & Archiver

This application scrapes flight and crew data from NOC Mobile and archives it for offline processing.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    playwright install
    ```

2.  **Configuration**:
    *   Create a `.env` file in this directory with your credentials (optional, can also enter in UI):
        ```
        NOC_USERNAME=your_username
        NOC_PASSWORD=your_password
        ```

3.  **Run the App**:
    ```bash
    streamlit run app.py
    ```

> **Note:** The `run_app.sh` script now prompts you to ingest pairing and IOE data on startup. If you answer `y`, all files in the `pairings/` and `ioe/` directories will be processed each time the app starts.

## Usage

*   **Scraper Tab**: Select a start and end date, then click "Start Scraping". The app will launch a browser, log in, and scrape data for each day in the range.
*   **Data Explorer Tab**: View and search the scraped flights and crew data.

## Database

By default, data is stored in `noc_data.db` (SQLite). 

To migrate to **Google Cloud SQL (PostgreSQL)**:
1. Set the `DATABASE_URL` environment variable (e.g., in `.env` or your cloud environment).
   * Format: `postgresql+psycopg2://user:pass@host:5432/dbname`
2. Ensure `psycopg2-binary` is installed.
3. The application will automatically create the necessary tables on first run.

## Google Cloud Firestore Setup
To enable cloud sync, follow the detailed instructions in [FIREBASE.md](file:///c:/Code/dumpnoc/FIREBASE.md).

Quick Summary:
1.  Create a project in the [Google Cloud Console](https://console.cloud.google.com/).
2.  Enable the **Firestore API** and create a Firestore database (Native mode).
3.  Go to **IAM & Admin > Service Accounts** and create a service account.
4.  Grant the service account `Cloud Datastore User` role.
5.  Create a key (JSON) for the service account and download it.
6.  Place the JSON file in the project root (e.g. as `firestore_key.json`).
7.  Update `.env` or set environment variables:
    ```bash
    FIRESTORE_CREDENTIALS="path/to/firestore_key.json"
    ENABLE_CLOUD_SYNC=True
    ```
