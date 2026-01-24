import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
from config import NOC_USERNAME, NOC_PASSWORD, VERSION
from scraper import NOCScraper
from database import get_session, Flight, CrewMember, DailySyncStatus, init_db, FlightHistory
from sqlalchemy import desc, extract
from bid_periods import get_bid_period_date_range, get_bid_period_from_date

# Page Config
st.set_page_config(page_title="NOC Mobile Scraper", layout="wide", page_icon="✈️")

st.title("✈️ NOC Mobile Scraper & Archiver")

# Global CSS for table styling
st.markdown("""
<style>
    th {
        text-align: center !important;
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

# Sidebar - Configuration & Credentials
with st.sidebar:
    st.header("Credentials")
    username = st.text_input("Username", value=NOC_USERNAME if NOC_USERNAME else "")
    password = st.text_input("Password", value=NOC_PASSWORD if NOC_PASSWORD else "", type="password")
    
    st.divider()
    
    # Cloud Sync Toggle
    from config import ENABLE_CLOUD_SYNC
    from database import get_metadata, set_metadata, get_session
    from firestore_lib import set_cloud_sync_enabled
    
    # Initialize from DB or Config
    session = get_session()
    db_cloud_sync = get_metadata(session, "ui_enable_cloud_sync")
    session.close()
    
    if db_cloud_sync is None:
        initial_val = ENABLE_CLOUD_SYNC
    else:
        initial_val = db_cloud_sync.lower() == 'true'
        
    cloud_sync_enabled = st.toggle("Enable Cloud Sync", value=initial_val)
    
    # Update persistence and global state
    if cloud_sync_enabled != initial_val:
        session = get_session()
        set_metadata(session, "ui_enable_cloud_sync", str(cloud_sync_enabled).lower())
        session.close()
    
    set_cloud_sync_enabled(cloud_sync_enabled)
    
    st.divider()
    st.info("NOC Mobile Scraper v1.3")
    
    st.divider()
    st.header("Scheduler Config")
    
    from config import SCRAPE_INTERVAL_HOURS
    from database import get_metadata, set_metadata, get_session
    
    session = get_session()
    current_interval_db = get_metadata(session, "scrape_interval_hours")
    session.close()
    
    initial_interval = int(current_interval_db) if current_interval_db else SCRAPE_INTERVAL_HOURS
    
    new_interval = st.number_input("Scrape Interval (Hours)", min_value=1, max_value=24, value=initial_interval)
    
    if new_interval != initial_interval:
        session = get_session()
        set_metadata(session, "scrape_interval_hours", str(new_interval))
        session.close()
        st.success(f"Updated! Restart scheduler.")

if not username or not password:
    st.warning("Please enter NOC Mobile credentials in the sidebar to proceed.")
    st.stop()

# --- Sync Status Summary ---
session = get_db_session()
from database import get_metadata
global_last_sync = get_metadata(session, "last_successful_sync")
last_sync_rec = session.query(DailySyncStatus).order_by(desc(DailySyncStatus.last_scraped_at)).first()
session.close()

if global_last_sync:
    st.info(f"📊 **Data Freshness:** Last pull performed at {global_last_sync}")
if last_sync_rec:
    st.caption(f"Last data point synced: {last_sync_rec.date.strftime('%Y-%m-%d')} ({last_sync_rec.flights_found} flights)")

# --- Navigation & Query Params ---
query_params = st.query_params

# Define Tabs
NAV_HISTORICAL = "📅 Historical Data"
NAV_PAIRINGS = "📋 Pairings"
NAV_IOE = "🎓 IOE Audit"
NAV_SYNC = "🔄 Sync Data"

tabs = [NAV_HISTORICAL, NAV_PAIRINGS, NAV_IOE, NAV_SYNC]

# Handle incoming navigation params
default_nav_index = 0
if "tab" in query_params:
    t_arg = query_params["tab"]
    if t_arg == "pairings": default_nav_index = 1
    elif t_arg == "ioe": default_nav_index = 2
    elif t_arg == "sync": default_nav_index = 3
    # else 0

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

# Navigation Control
selected_tab = st.radio("Navigation", tabs, index=default_nav_index, horizontal=True, label_visibility="collapsed")

# --- TAB 1: Historical Data ---
if selected_tab == NAV_HISTORICAL:
    from ui.historical import render_historical_tab
    render_historical_tab()

# --- TAB 2: Pairings ---
elif selected_tab == NAV_PAIRINGS:
    from ui.pairings import render_pairings_tab
    render_pairings_tab()

# --- TAB 3: IOE Audit ---
elif selected_tab == NAV_IOE:
    from ui.ioe import render_ioe_tab
    render_ioe_tab()

# --- TAB 4: Sync Data ---
elif selected_tab == NAV_SYNC:
    from ui.sync import render_sync_tab
    render_sync_tab(username, password)
