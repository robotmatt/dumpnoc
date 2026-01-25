# NOC Mobile Scraper & Archiver - AI Coding Guidelines

## Architecture Overview
This is a Python application that scrapes flight and crew data from NOC Mobile (aviation operations system) and provides a Streamlit web interface for historical data exploration, IOE audits, and cloud synchronization.

**Core Components:**
- `app.py`: Main Streamlit application with multi-page navigation
- `scraper.py`: Playwright-based web scraper for NOC Mobile data extraction
- `database.py`: SQLAlchemy ORM models (Flight, CrewMember, FlightHistory, etc.)
- `scheduler_worker.py`: Background thread for periodic data syncing
- `ui/*.py`: Streamlit page components (historical, pairings, IOE audit, sync, settings)
- `firestore_lib.py`: Google Firestore integration for cloud backup/sync

**Data Flow:**
1. NOCScraper extracts flight data using headless browser automation
2. Data stored in local SQLite/PostgreSQL with change tracking (FlightHistory)
3. Optional two-way sync with Google Firestore
4. Streamlit UI displays historical flights, pairings, and audit reports

## Key Patterns & Conventions

### Database Access
Always use `get_session()` from `database.py` for DB operations:
```python
from database import get_session
session = get_session()
# ... queries ...
session.close()
```

### Configuration
Settings loaded from `.env` file or environment variables via `config.py`. Runtime settings stored as metadata in DB:
```python
from database import get_metadata, set_metadata
interval = get_metadata(session, "scrape_interval_hours")
```

### Scraping
Use `NOCScraper` class for web automation. Always call `scraper.start()` before use and `scraper.stop()` after:
```python
scraper = NOCScraper(headless=True)
scraper.start()
try:
    if scraper.login(username, password):
        # scrape operations
finally:
    scraper.stop()
```

### Change Tracking
Flight modifications automatically logged to `FlightHistory` table with JSON diffs:
```python
# Changes detected in scraper.py and stored as:
changes_json = json.dumps({'tail_number': {'old': 'N123', 'new': 'N456'}})
```

### Background Scheduling
Scheduler runs in separate thread, configurable via DB metadata. Interval checked every minute.

### UI Pages
Each page in `ui/` follows `render_<page>_tab()` function pattern. Use Streamlit columns for layout.

## Developer Workflows

### Local Development
```bash
pip install -r requirements.txt
playwright install
streamlit run app.py
```

### Background Sync
```bash
python run_scheduler.py  # Runs continuously, checks DB for interval settings
```

### Database Migration
Supports SQLite (default) or PostgreSQL via `DATABASE_URL` env var. Tables auto-created on first run.

### Testing
Run verification scripts in `testing/` directory:
```bash
python testing/verify_data.py
```

### Cloud Setup
Follow `FIREBASE.md` for Firestore configuration. Requires service account JSON key.

## Common Patterns

- **Error Handling**: Scraper handles timeouts/retries automatically
- **Dates**: Use `datetime` objects, UTC fields for consistency
- **Associations**: Many-to-many via `flight_crew_association` table with roles/flags
- **IOE Audit**: Compares scraped flights against `IOEAssignment` records
- **File Ingestion**: `ingest_data.py` parses pairings/IOE text files from `pairings/` and `ioe/` dirs

## Dependencies
- `playwright`: Browser automation
- `streamlit`: Web UI
- `sqlalchemy`: ORM
- `firebase-admin`: Cloud sync
- `beautifulsoup4`: HTML parsing

Reference `README.md` for setup and `FIREBASE.md` for cloud configuration.