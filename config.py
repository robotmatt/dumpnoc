import os
from dotenv import load_dotenv

load_dotenv()

# URLs
BASE_URL = "https://uca.noc.vmc.navblue.cloud/RaidoMobile"
LOGIN_URL = f"{BASE_URL}/Default.aspx"
STATION_OPS_URL = f"{BASE_URL}/Dialogues/Operations/StationOperations.aspx"

# Database
DB_NAME = "noc_data.db"
# Use DATABASE_URL from environment for Cloud SQL, fallback to local SQLite
# For PostgreSQL: postgresql+psycopg2://user:pass@host:port/dbname
DB_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_NAME}")

# Env Vars
NOC_USERNAME = os.getenv("NOC_USERNAME")
NOC_PASSWORD = os.getenv("NOC_PASSWORD")

# Firestore
FIRESTORE_CREDENTIALS = os.getenv("FIRESTORE_CREDENTIALS", "firestore_key.json")
ENABLE_CLOUD_SYNC = os.getenv("ENABLE_CLOUD_SYNC", "False").lower() in ("true", "1", "t")

# Scheduling
SCRAPE_INTERVAL_HOURS = int(os.getenv("SCRAPE_INTERVAL_HOURS", "1"))
SCRAPE_DAYS = int(os.getenv("SCRAPE_DAYS", "1"))

# App Version Number
VERSION = "v1.3"
