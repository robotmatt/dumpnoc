
import streamlit as st
from config import NOC_USERNAME, NOC_PASSWORD, VERSION, SCRAPE_INTERVAL_HOURS, ENABLE_CLOUD_SYNC
from database import get_session, get_metadata, set_metadata
from firestore_lib import set_cloud_sync_enabled
from tools.backup_db import create_db_backup

def render_settings_tab():
    st.header("⚙️ Settings")
    
    st.subheader("Authentication Settings")
    
    session = get_session()
    db_auth_mode = get_metadata(session, "auth_mode", "legacy")
    session.close()

    auth_mode = st.radio(
        "Authentication Mode",
        options=["legacy", "sso"],
        format_func=lambda x: "Legacy Form (Username/Password)" if x == "legacy" else "Microsoft SSO (Azure AD)",
        index=0 if db_auth_mode == "legacy" else 1,
        key="auth_mode_input"
    )
    
    # Save auth_mode to metadata if it changed
    if auth_mode != db_auth_mode:
        session = get_session()
        set_metadata(session, "auth_mode", auth_mode)
        session.close()
        st.toast(f"Auth mode updated to {auth_mode.upper()}!", icon="🔄")
        st.rerun()

    launch_sso = False
    
    if auth_mode == "legacy":
        username = st.text_input("Username", value=NOC_USERNAME if NOC_USERNAME else "", key="username_input")
        password = st.text_input("Password", value=NOC_PASSWORD if NOC_PASSWORD else "", type="password", key="password_input")
        
        # Store in session state for pages to access
        st.session_state["username"] = username
        st.session_state["password"] = password
        
        import os
        from config import SESSION_STATE_PATH
        if os.path.exists(SESSION_STATE_PATH):
            st.success("✅ Saved session cookies exist (Legacy mode). App will bypass login forms until cookies expire.")
            if st.button("Clear Saved Cookies / Logout"):
                try:
                    os.remove(SESSION_STATE_PATH)
                    st.toast("Cleared saved cookies!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error clearing cookies: {e}")
    else:
        st.info("ℹ️ **Microsoft SSO Mode:** Authentication is handled interactively via a Chromium window. The app will save your session tokens to automate background scraping.")
        
        import os
        from config import SESSION_STATE_PATH
        session_exists = os.path.exists(SESSION_STATE_PATH)
        
        if session_exists:
            st.success("✅ **Active Microsoft SSO Session Saved**")
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("🔄 Refresh SSO Session (Launch Browser)", use_container_width=True):
                    launch_sso = True
            with col2:
                if st.button("🗑️ Delete Saved Session (Log Out)", use_container_width=True):
                    try:
                        os.remove(SESSION_STATE_PATH)
                        session = get_session()
                        set_metadata(session, "last_scrape_error", "")
                        session.close()
                        st.toast("Saved session deleted.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting session file: {e}")
        else:
            st.warning("⚠️ **No Active SSO Session found.** Background scheduler cannot scrape until you log in once.")
            if st.button("🚀 Launch SSO Login Browser", type="primary", use_container_width=True):
                launch_sso = True
                
        if launch_sso:
            with st.spinner("Opening browser window... Please complete the SSO login and 2FA in the browser. The browser will close itself when done."):
                from scraper import run_interactive_sso_login
                success = run_interactive_sso_login(SESSION_STATE_PATH)
                if success:
                    session = get_session()
                    set_metadata(session, "last_scrape_error", "") # Clear error banner
                    session.close()
                    st.success("🎉 SSO login completed and session state saved! Scraper is now ready.")
                    st.rerun()
                else:
                    st.error("❌ Login failed or browser was closed before completing login. Please try again.")

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
    new_days = st.number_input("Days to Scrape", min_value=1, max_value=45, value=initial_days)

    next_scrape = get_metadata(session, "next_scheduled_scrape")
    if next_scrape:
        st.info(f"⏳ **Next Automatic Scrape:** {next_scrape}")
    
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
    st.subheader("🛠️ Database Management")
    
    col_b1, col_b2 = st.columns([1, 1.5])
    with col_b1:
        if st.button("🚀 Create Manual Backup", use_container_width=True):
            backup_file = create_db_backup()
            if backup_file:
                st.success(f"Backup created successfully!")
                st.code(backup_file)
            else:
                st.error("Backup failed. Check logs.")
    with col_b2:
        st.write("Backups are stored in the `/backups` folder. The system keeps the last 15 days of backups automatically.")

    st.divider()
    st.caption(f"NOC Mobile Scraper {VERSION}")
