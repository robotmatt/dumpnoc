
import streamlit as st
from datetime import datetime, timedelta
from database import get_session, get_metadata, DailySyncStatus
from scraper import NOCScraper
from config import NOC_USERNAME, NOC_PASSWORD
from firestore_lib import is_cloud_sync_enabled
from sqlalchemy import desc

def render_sync_tab():
    username = st.session_state.get("username")
    password = st.session_state.get("password")
    
    # --- Sync Status Summary ---
    session = get_session()
    global_last_sync = get_metadata(session, "last_successful_sync")
    last_sync_rec = session.query(DailySyncStatus).order_by(desc(DailySyncStatus.last_scraped_at)).first()
    session.close()
    
    st.header("Sync Settings")
    
    if global_last_sync:
        st.info(f"📊 **Data Freshness:** Last pull performed at {global_last_sync}")
    if last_sync_rec:
        st.caption(f"Last data point synced: {last_sync_rec.date.strftime('%Y-%m-%d')} ({last_sync_rec.flights_found} flights)")

    # Cloud Configuration Check (using dynamic setting)
    active_cloud_sync = is_cloud_sync_enabled()
    
    if active_cloud_sync:
        st.success("✅ Cloud Sync Active")
    else:
         st.warning("⚠️ Cloud Sync Inactive")

    st.divider()
    
    st.subheader("1. Scraper Sync (NOC -> Local DB)")
    sync_mode = st.radio("Sync Mode", ["Current Day", "Date Range"], horizontal=True)
    
    start_date = datetime.today()
    end_date = datetime.today()
    
    if sync_mode == "Date Range":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.today() - timedelta(days=1))
        with col2:
            end_date = st.date_input("End Date", datetime.today())
    else:
        st.info(f"Will sync data for: **{start_date.strftime('%Y-%m-%d')}**")
    

    if st.button(f"Start Scraper Sync ({sync_mode})", type="primary"):
        status_area = st.empty()
        
        if start_date > end_date:
            status_area.error("Start date must be before or equal to end date.")
        else:
            status_area.info("Initializing browser...")
            progress_bar = st.progress(0)
            
            scraper = NOCScraper(headless=True) # Ensure this is compatible with your environment
            
            try:
                scraper.start()
                if scraper.login(username, password):
                    status_area.success("Logged in! Scraping dates...")
                    
                    # Iterate
                    curr = start_date
                    total_days = (end_date - start_date).days + 1
                    days_done = 0
                    
                    while curr <= end_date:
                        status_area.write(f"Scraping {curr.strftime('%Y-%m-%d')}...")
                        
                        s_dt = datetime.combine(curr, datetime.min.time())
                        scraper.scrape_date(s_dt)
                        
                        days_done += 1
                        progress_bar.progress(days_done / total_days)
                        curr += timedelta(days=1)
                        
                    status_area.success("Sync Complete! Check the Historical Data tab.")
                else:
                    status_area.error("Login failed. Please check your credentials.")
            except Exception as e:
                import traceback
                status_area.error(f"An error occurred: {e}")
                st.exception(e) # Show full traceback
            finally:
                scraper.stop()
    
    st.divider()
    
    st.subheader("2. Cloud Sync (Local DB -> Google Firestore)")
    if not active_cloud_sync:
        st.caption("Enable cloud sync in the sidebar to use these features.")
    else:
        # Unified Sync Button
        from firestore_lib import get_cloud_metadata
        session = get_session()
        local_sync = get_metadata(session, "last_successful_sync")
        session.close()
        cloud_sync = get_cloud_metadata("last_successful_sync")
        
        can_proceed = True
        if local_sync and cloud_sync:
            try:
                l_dt = datetime.strptime(local_sync, '%Y-%m-%d %H:%M:%S')
                c_dt = datetime.strptime(cloud_sync, '%Y-%m-%d %H:%M:%S')
                
                if l_dt < c_dt:
                    st.warning(f"⚠️ **Cloud Data is Newer!**\n\nYour local data (**{local_sync}**) is older than the data in the cloud (**{cloud_sync}**). Mirroring will overwrite newer cloud data with your older local data.")
                    if not st.checkbox("I understand local data is older and I want to mirror it anyway.", key="confirm_mirror_older"):
                        can_proceed = False
                        st.info("Please confirm the checkbox above to enable the Full Sync button.")
            except:
                pass

        if st.button("☁️ Full Sync: Mirror ALL Local Data to Cloud", type="secondary", width='stretch', disabled=not can_proceed):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] User initiated Full Sync Mirroring...")
            with st.status("🚀 Initializing Full Sync Mirror...", expanded=True) as status:
                start_time = datetime.now()
                session = get_session()
                from ingest_data import upload_ioe_to_cloud, upload_pairings_to_cloud, upload_flights_to_cloud, upload_metadata_to_cloud
                
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] 📋 Gathering IOE assignments...")
                cnt_ioe = upload_ioe_to_cloud(session)
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Uploaded {cnt_ioe} IOE records.")
                
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] 📋 Gathering scheduled pairings...")
                cnt_pair = upload_pairings_to_cloud(session)
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Uploaded {cnt_pair} pairing bundles.")
                
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] 📋 Gathering historical flight data (this may take a moment)...")
                cnt_flt = upload_flights_to_cloud(session) # No date range = all
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Uploaded {cnt_flt} daily flight blocks.")
                
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] 📋 Syncing application metadata...")
                cnt_meta = upload_metadata_to_cloud(session)
                status.write(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Metadata sync complete.")
                
                session.close()
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                status.update(label=f"✅ Full Sync Complete in {duration:.1f}s!", state="complete", expanded=False)
                st.success(f"**Mirroring Summary:**\n- {cnt_ioe} IOE Assignments\n- {cnt_pair} Pairings\n- {cnt_flt} Daily Flight Bundles\n- {cnt_meta} Metadata Keys\n\nTotal Duration: {duration:.1f} seconds")

        st.divider()
        st.caption("Manual Individual Syncs:")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            if st.button("☁️ Upload/Update IOE Assignments"):
                with st.spinner("Uploading IOE data..."):
                    from ingest_data import upload_ioe_to_cloud
                    session = get_session()
                    cnt = upload_ioe_to_cloud(session)
                    session.close()
                    st.success(f"Uploaded {cnt} IOE records.")
                    
        with c2:
            if st.button("☁️ Upload/Update Scheduled Pairings"):
                with st.spinner("Uploading Scheduled Flights..."):
                    from ingest_data import upload_pairings_to_cloud
                    session = get_session()
                    cnt = upload_pairings_to_cloud(session)
                    session.close()
                    st.success(f"Uploaded {cnt} scheduled flights.")
        
        with c3:
            if st.button("☁️ Upload Scraped Flights History"):
                with st.spinner("Uploading Historical Flights..."):
                    from ingest_data import upload_flights_to_cloud
                    session = get_session()
                    
                    s_dt = datetime.combine(start_date, datetime.min.time())
                    e_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
                    
                    cnt = upload_flights_to_cloud(session, start_date=s_dt, end_date=e_dt)
                    session.close()
                    st.success(f"Uploaded {cnt} flights from {start_date} to {end_date}.")

    st.divider()
    
    st.subheader("3. Restore / Hydrate Local DB (Cloud -> Local)")
    st.markdown("⚠️ **Warning**: This will merge data from the Cloud into your local database.")
    
    if active_cloud_sync:
        if st.button("⬇️ Restore from Cloud", type="secondary"):
             with st.status("📥 Restoring data from Cloud...", expanded=True) as status:
                 from ingest_data import sync_down_from_cloud
                 session = get_session()
                 status.write("Downloading and merging data (this may take a minute)...")
                 stats = sync_down_from_cloud(session)
                 session.close()
                 
                 status.update(label="✅ Restore Complete!", state="complete", expanded=False)
                 st.success(f"**Restore Summary:**\n- {stats.get('flights', 0)} Flights restored/updated\n- {stats.get('pairings', 0)} Pairings restored\n- {stats.get('ioe', 0)} IOE Assignments restored\n- {stats.get('metadata', 0)} Metadata keys updated")
                 st.balloons()
    else:
        st.info("Cloud sync is disabled. Enable it in the sidebar to use this feature.")
