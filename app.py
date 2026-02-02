import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
from config import NOC_USERNAME, NOC_PASSWORD, VERSION, SCRAPE_INTERVAL_HOURS, ENABLE_CLOUD_SYNC
from scraper import NOCScraper
from database import get_session, Flight, CrewMember, DailySyncStatus, init_db, FlightHistory, get_metadata, set_metadata
from sqlalchemy import desc, extract
from bid_periods import get_bid_period_date_range, get_bid_period_from_date
from firestore_lib import set_cloud_sync_enabled
from scheduler_worker import start_background_scheduler

# Start Background Scraper
start_background_scheduler()

# Page Config
st.set_page_config(page_title="NOC Mobile Scraper", layout="wide", page_icon="✈️")

# --- Page Definitions ---
from ui.historical import render_historical_tab
from ui.pairings import render_pairings_tab
from ui.ioe import render_ioe_tab
from ui.sync import render_sync_tab
from ui.employee import render_employee_tab
from ui.settings import render_settings_tab

page_hist = st.Page(render_historical_tab, title="Historical Data", icon="📅", default=True)
page_pair = st.Page(render_pairings_tab, title="Pairings", icon="📋", url_path="pairings")
page_ioe = st.Page(render_ioe_tab, title="IOE Audit", icon="🎓", url_path="ioe")
page_emp = st.Page(render_employee_tab, title="Employee History", icon="👤", url_path="employee")
page_sync = st.Page(render_sync_tab, title="Sync Data", icon="🔄", url_path="sync")
page_sett = st.Page(render_settings_tab, title="Settings", icon="⚙️", url_path="settings")

# Initialize navigation but hide sidebar UI
pg = st.navigation([page_hist, page_pair, page_ioe, page_emp, page_sync, page_sett], position="hidden")

st.title("✈️ NOC Mobile Scraper & Archiver")

# --- Top Navigation ---
nc1, nc2, nc3, nc4, nc5, nc6, n_spacer = st.columns([1.2, 1, 1, 1.2, 1, 1, 2])
with nc1:
    st.page_link(page_hist, label="Historical Data", icon="📅")
with nc2:
    st.page_link(page_pair, label="Pairings", icon="📋")
with nc3:
    st.page_link(page_ioe, label="IOE Audit", icon="🎓")
with nc4:
    st.page_link(page_emp, label="Employee", icon="👤")
with nc5:
    st.page_link(page_sync, label="Sync Data", icon="🔄")
with nc6:
    st.page_link(page_sett, label="Settings", icon="⚙️")

st.divider()

# Global CSS for table styling
st.markdown("""
<style>
    th {
        text-align: center !important;
    }
    .stPageLink {
        background-color: transparent !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize DB
if 'db_initialized' not in st.session_state:
    init_db()
    st.session_state.db_initialized = True

# Helper: Get Session
def get_db_session():
    return get_session()

# --- Shared State / Background Init ---
# Ensure credentials from config are in session state if not already set by UI
if "username" not in st.session_state:
    st.session_state["username"] = NOC_USERNAME
if "password" not in st.session_state:
    st.session_state["password"] = NOC_PASSWORD

# Ensure Cloud Sync settings are updated on load (side effect)
session = get_session()
db_cloud_sync = get_metadata(session, "ui_enable_cloud_sync")
session.close()

if db_cloud_sync is not None:
    set_cloud_sync_enabled(db_cloud_sync.lower() == 'true')
else:
    set_cloud_sync_enabled(ENABLE_CLOUD_SYNC)

# --- Global Check ---
# Only block if we aren't on the settings page
current_page = st.session_state.get("current_page")
# Streamlit st.navigation doesn't make it trivial to get current page object here without running,
# but we can check if credentials exist before running any data-intensive page.
# render_sync_tab specifically needs them.

# --- Navigation & Query Params Logic ---
query_params = st.query_params

# Persist Filters from Params to Session State if present
if "pairing" in query_params:
    st.session_state["pairing_search_default"] = query_params["pairing"]

if "date" in query_params:
    try:
        st.session_state["history_date_default"] = datetime.strptime(query_params["date"], "%Y-%m-%d").date()
    except:
        pass

if "flight_num" in query_params:
    st.session_state["history_flight_default"] = query_params["flight_num"]

if "month" in query_params:
    st.session_state["pairing_month_default"] = query_params["month"]

    try:
        st.session_state["pairing_date_default"] = datetime.strptime(query_params["pdate"], "%Y-%m-%d").date()
    except:
        pass

if "emp_id" in query_params:
    st.session_state["employee_search_id"] = query_params["emp_id"]

if "emp_month" in query_params:
    st.session_state["employee_search_month"] = query_params["emp_month"]

# Run the page logic
pg.run()

