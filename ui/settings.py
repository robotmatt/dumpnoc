
import streamlit as st
from config import NOC_USERNAME, NOC_PASSWORD, VERSION, SCRAPE_INTERVAL_HOURS, ENABLE_CLOUD_SYNC
from database import get_session, get_metadata, set_metadata
from firestore_lib import set_cloud_sync_enabled

def render_settings_tab():
    st.header("⚙️ Settings")
    
    st.subheader("Credentials")
    username = st.text_input("Username", value=NOC_USERNAME if NOC_USERNAME else "", key="username_input")
    password = st.text_input("Password", value=NOC_PASSWORD if NOC_PASSWORD else "", type="password", key="password_input")
    
    # Store in session state for pages to access
    st.session_state["username"] = username
    st.session_state["password"] = password

    st.divider()
    
    st.subheader("Cloud Sync")
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
    
    if cloud_sync_enabled:
        st.success("✅ Cloud Sync Enabled")
    else:
        st.info("☁️ Cloud Sync is currently disabled.")
        
    st.divider()
    st.subheader("Scheduler Config")
    
    session = get_session()
    current_interval_db = get_metadata(session, "scrape_interval_hours")
    session.close()
    
    initial_interval = int(current_interval_db) if current_interval_db else SCRAPE_INTERVAL_HOURS
    
    new_interval = st.number_input("Scrape Interval (Hours)", min_value=1, max_value=24, value=initial_interval)
    
    current_days_db = get_metadata(session, "scrape_days")
    from config import SCRAPE_DAYS
    initial_days = int(current_days_db) if current_days_db else SCRAPE_DAYS
    new_days = st.number_input("Days to Scrape", min_value=1, max_value=14, value=initial_days)

    if new_interval != initial_interval:
        session = get_session()
        set_metadata(session, "scrape_interval_hours", str(new_interval))
        session.close()
        st.success(f"Interval Updated! Restart scheduler or app.")

    if new_days != initial_days:
        session = get_session()
        set_metadata(session, "scrape_days", str(new_days))
        session.close()
        st.success(f"Days Updated! Reflected in next background scrape.")

    st.divider()
    st.caption(f"NOC Mobile Scraper {VERSION}")
