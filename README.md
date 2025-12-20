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
