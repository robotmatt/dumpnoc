# NOC Mobile Scraper & Archiver

This application scrapes flight and crew data from NOC Mobile and archives it for offline processing, historical auditing, and cloud synchronization.

## Features

- **Automated Scraping**: Scrapes flight details, crew assignments, passenger data, and operational notes.
- **Change History Tracking**: Automatically detects and logs changes to flights (Crew swaps, tail number changes, schedule delays, etc.) with a timestamped audit trail.
- **Scheduled Background Sync**: A dedicated scheduler script to pull data periodically on a configurable interval.
- **IOE Audit Reporting**: Compares scraped flight logs against IOE assignments to verify training completion.
- **Cloud Synchronization**: Optional two-way sync with Google Firestore for data persistence across multiple devices.
- **Historical Data Explorer**: Search and view detailed flight logs for any date in the database.

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
        # Optional:
        DATABASE_URL=sqlite:///noc_data.db
        ENABLE_CLOUD_SYNC=False
        SCRAPE_INTERVAL_HOURS=1
        ```

3.  **Run the Web Interface**:
    ```bash
    streamlit run app.py
    ```

## background Scheduler

The application includes a background scheduler that runs independently of the web UI to keep your data fresh.

1.  **Set the Interval**: In the web UI sidebar, find the **Scheduler Config** section and set your desired interval (e.g., 1 hour).
2.  **Launch the Scheduler**:
    ```bash
    python run_scheduler.py
    ```
    This script will run continuously, logging in to NOC Mobile and performing a sweep of "Today" and "Tomorrow" at the specified interval. It handles session timeouts and browser restarts automatically.

## Usage

*   **📅 Historical Data**: View daily flight logs. Click on a flight number to see detailed operational data and the **Flight Change History**.
*   **📋 Pairings**: View scheduled pairings and compare scheduled vs. actual flown data.
*   **🎓 IOE Audit**: Run reports to see which IOE assignments have been completed and which legs are missing student/check-airman data.
*   **🔄 Sync Data**: Manually trigger pulls from NOC Mobile for specific date ranges or mirror/restore your local database with Google Firestore.

## Database

By default, data is stored in `noc_data.db` (SQLite). 

To migrate to **Google Cloud SQL (PostgreSQL)**:
1. Set the `DATABASE_URL` environment variable (e.g., in `.env` or your cloud environment).
   * Format: `postgresql+psycopg2://user:pass@host:5432/dbname`
2. Ensure `psycopg2-binary` is installed.
3. The application will automatically create the necessary tables on first run.

## Google Cloud Firestore Setup
To enable cloud sync, follow the detailed instructions in [FIREBASE.md](FIREBASE.md).

Quick Summary:
1.  Enable the **Firestore API** in Google Cloud Console.
2.  Create a Service Account with the `Cloud Datastore User` role.
3.  Download the JSON key and place it in the root directory.
4.  Set `ENABLE_CLOUD_SYNC=True` in your `.env`.
