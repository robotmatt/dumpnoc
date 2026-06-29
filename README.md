# ✈️ NOC Mobile Scraper & Archiver

This application scrapes flight and crew data from NOC Mobile and archives it for offline processing, historical auditing, and cloud synchronization. 

---

## 🚀 Key Features

*   **🕵️ Automated Playwright Scraping**: Automates authentication and session maintenance on NOC Mobile, scraping detailed leg configurations, actual and scheduled flight times, tail numbers, load/passenger details, and crew roles.
*   **⏱️ Continuous Background Scheduler**: Includes a standalone, resilient daemon (`run_scheduler.py`) that periodically sweeps "Today" and "Tomorrow" schedules at a user-defined interval, handling login sessions and browser lifecycle automatically.
*   **🔄 Database Synchronization & Cloud Backups**:
    *   Supports local **SQLite** (`noc_data.db`) or cloud-hosted **PostgreSQL / Google Cloud SQL**.
    *   Provides two-way backup and restore capabilities with **Google Cloud Firestore**, enabling seamless restoration or database mirroring across multiple client instances.
*   **👤 Employee History Search**: Search any employee by name or ID to view their flights flown, assigned pairings, and a chronological log of schedule removals and additions.
*   **🗓️ Roster Management & PDF Export**: Generates monthly rosters detailing active flights, scheduled vs. actual block metrics, and removal audits. Includes one-click export to a clean, print-ready A4 PDF format.
*   **⏱️ Open Time Dashboard**: Monitors unfilled flights to display legs missing key crew positions (Captain, First Officer, or Flight Attendant), filtered by dates and hub bases (IAD, IAH).
*   **🎓 IOE Audit Reports**: Performs programmatic training auditing by cross-referencing scraped flights against PBS-assigned IOE withheld trips:
    *   *Assignments Audit*: Verifies IOE completion and highlights legs that are "Flown (Verified IOE)", "not used for IOE", "Canceled", or "Future".
    *   *Available LCP Trips*: Identifies flights with a Line Check Pilot (LCP) onboard that are not assigned as IOE, highlighting latent training capacity.
    *   *Unscheduled IOE*: Finds flights containing active IOE flags that do not appear in official PBS assignments.
    *   *Ad-Hoc IOE Grouping*: Groups pairings with IOE-flagged legs not present in official lists.

---

## 📂 Monthly Reference Data Setup

To maintain accurate pairings, IOE training audits, and LCP validation, you must update the reference files in the following folders **every month**. The ingestion utility parses these directories to sync the local database:

1.  **📂 `/pairings` (PBS Pairings)**
    *   **Source**: PBS (Preferably CA and FO pairings).
    *   **File Format**: Plain text (`.txt`) files containing the raw pairings grids and leg listings.
    *   **Logic**: The system ignores FA (Flight Attendant) pairings unless specified, focusing on pilots.
2.  **📂 `/ioe` (PBS IOE Trips)**
    *   **Source**: PBS withheld IOE trips.
    *   **File Format**: Plain text (`.txt`) files containing employee IDs, pairing numbers, and start dates.
3.  **📂 `/lcp` (LCP Qualifications list)**
    *   **Source**: MyDaily Line Check Pilot exports.
    *   **File Format**: 
        *   Text (`.txt`) lists in the format: `[5-Digit Employee ID] [Pilot Name]` (e.g. `12345 SMITH JOHN`).
        *   **OR** raw PDF files (`.pdf`) exported directly. The system automatically searches pages for candidates containing the qualification `"XMJ Line Check Pilot"`, parsing their names and employee IDs.

---

## 🛠️ Ingestion & Startup Scripts

We provide convenient wrapper scripts to automate virtual environment setups, package installations, browser binaries downloads, and initial data ingestion:

*   **Windows**: Run `run_app.bat`
*   **Linux / macOS**: Run `./run_app.sh`

Both startup scripts will prompt you:
```text
Ingest pairing and IOE data now? (y/n):
```
Entering `y` automatically triggers `ingest_data.py`, which clears old reference tables and re-imports files from the `/pairings`, `/ioe`, and `/lcp` directories.

---

## ⚙️ Setup & Installation (Manual)

### 1. Install Dependencies
Ensure you have Python 3.8+ installed, then install requirements and download Playwright web browsers:
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
playwright install chromium
```

### 2. Environment Variables (`.env`)
Create a `.env` file in the root directory to store credentials and configurations:
```env
# NOC Mobile Credentials
NOC_USERNAME=your_username
NOC_PASSWORD=your_password

# Database Settings (Default: SQLite)
DATABASE_URL=sqlite:///noc_data.db

# Cloud Sync Settings
ENABLE_CLOUD_SYNC=False
SCRAPE_INTERVAL_HOURS=1
```

---

## 🖥️ Running the Application

### Running the Web Interface
Start the Streamlit web dashboard to search logs, export rosters, view schedules, and trigger syncs:
```bash
streamlit run app.py
```

### Running the Background Scheduler
To run the automated scraper daemon in the background:
```bash
python run_scheduler.py
```
This runs continuously on the configured interval, logging into NOC Mobile to fetch Today's and Tomorrow's legs, and automatically logging the change histories.

### Running File Ingestion Manually
To clear references and parse files inside `/pairings`, `/ioe`, and `/lcp`:
```bash
python ingest_data.py
```
*(If `ENABLE_CLOUD_SYNC` is active, this script will also automatically upload the newly ingested reference tables to Google Firestore).*

---

## 🗄️ Database & Cloud Integration

### PostgreSQL / Google Cloud SQL Setup
To migrate from SQLite to PostgreSQL:
1. Install `psycopg2-binary` in your environment.
2. Update the `DATABASE_URL` in your `.env` or system environment:
   ```env
   DATABASE_URL=postgresql+psycopg2://user:password@host:5432/dbname
   ```
3. The schema and tables will be created automatically on next run.

### Google Cloud Firestore Configuration
To activate Google Cloud Firestore backup and sync:
1. Refer to [FIREBASE.md](FIREBASE.md) for full configuration steps.
2. Place your downloaded Google Service Account JSON key in the root directory as `firestore_key.json`.
3. Set `ENABLE_CLOUD_SYNC=True` in your `.env`.
4. Trigger a **Full Sync** in the "Sync Data" tab of the web application.
