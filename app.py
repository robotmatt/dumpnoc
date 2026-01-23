import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
from config import NOC_USERNAME, NOC_PASSWORD
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
    st.info("NOC Mobile Scraper v1.2")
    
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
    # Header layout with Date Picker
    h_col1, h_col2 = st.columns([3, 1])
    with h_col1:
        st.header("Daily Flight Logs")
    with h_col2:
        d_val = st.session_state.get("history_date_default", datetime.today())
        view_date = st.date_input("Select Date", d_val, label_visibility="collapsed")
        
    session = get_db_session()
    view_dt = datetime.combine(view_date, datetime.min.time())
    status_rec = session.get(DailySyncStatus, view_dt)
    
    if status_rec:
        st.caption(f"Last Sync: {status_rec.last_scraped_at.strftime('%Y-%m-%d %H:%M') if status_rec.last_scraped_at else 'Unknown'}")
        
    # Main Content
    with st.container():
        # Load Data
        flights_query = session.query(Flight).filter(Flight.date >= view_dt, Flight.date < view_dt + timedelta(days=1))
        df_flights = pd.read_sql(flights_query.statement, session.bind)
        session.close()
        
        if not df_flights.empty:
            # --- Selection Logic (Up Front) ---
            def clean_fn(fn):
                s = str(fn).strip()
                if s.startswith("C5"): return s[2:]
                if s.startswith("C"): return s[1:]
                return s

            selected_flight_num = st.session_state.get("last_selected_flight")
            
            # Prioritize Query Param for Flight
            if "history_flight_default" in st.session_state:
                 selected_flight_num = st.session_state.pop("history_flight_default") # Consume it
            
            # 1. Detailed View (Now at the top)
            flight_opts = [clean_fn(f) for f in df_flights['flight_number'].tolist()]
            idx_sel = 0
            if selected_flight_num:
                path_target = clean_fn(selected_flight_num)
                if path_target in flight_opts:
                    idx_sel = flight_opts.index(path_target)
            
            selected_flight_val = st.selectbox("Select Flight to View Details", flight_opts, index=idx_sel)
            
            if selected_flight_val:
                st.session_state["last_selected_flight"] = selected_flight_val
                # Query full object
                session = get_db_session()
                candidates = [selected_flight_val, f"C5{selected_flight_val}", f"C{selected_flight_val}"]
                
                detailed_flight = session.query(Flight).filter(
                    Flight.flight_number.in_(candidates),
                    Flight.date >= view_dt, 
                    Flight.date < view_dt + timedelta(days=1)
                ).first()
                
                if detailed_flight:
                    st.subheader(f"✈️ Flight {detailed_flight.flight_number}")
                    
                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                    m_col1.metric("Tail", detailed_flight.tail_number or "N/A")
                    m_col2.metric("Type/Ver", f"{detailed_flight.aircraft_type or '--'} / {detailed_flight.version or '--'}")
                    m_col3.metric("Route", f"{detailed_flight.departure_airport} ➝ {detailed_flight.arrival_airport}")
                    
                    def fmt_pair(local_dt, utc_dt):
                        l = local_dt.strftime('%H:%M') if local_dt else "--"
                        u = utc_dt.strftime('%H:%M') if utc_dt else "--"
                        return f"{l} (L) | {u} (Z)"

                    m_col4.metric("Departure (STD)", fmt_pair(detailed_flight.scheduled_departure, detailed_flight.scheduled_departure_utc))
                    st.metric("Arrival (STA)", fmt_pair(detailed_flight.scheduled_arrival, detailed_flight.scheduled_arrival_utc))

                    st.markdown("### 👨‍✈️ Crew")
                    from database import flight_crew_association
                    stmt = flight_crew_association.select().where(flight_crew_association.c.flight_id == detailed_flight.id)
                    assoc_rows = session.execute(stmt).fetchall()
                    
                    crew_list = []
                    for row in assoc_rows:
                        cm = session.get(CrewMember, row.crew_id)
                        crew_list.append({
                            "Role": row.role,
                            "Name": cm.name,
                            "ID": cm.employee_id,
                            "Flags": row.flags or "" 
                        })
                    
                    if crew_list:
                        # Convert to HTML for consistent header centering
                        crew_df = pd.DataFrame(crew_list)
                        st.markdown(crew_df.to_html(index=False, classes='dataframe'), unsafe_allow_html=True)
                    else:
                        st.info("No crew parsed for this flight.")

                    # Flight History
                    history_records = session.query(FlightHistory).filter_by(flight_id=detailed_flight.id).order_by(desc(FlightHistory.timestamp)).all()
                    if history_records:
                        with st.expander("📜 Flight Change History"):
                            for h in history_records:
                                st.markdown(f"**Changed detected at:** {h.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                                
                                # Use description for quick view
                                st.caption(h.description)
                                
                                try:
                                    changes = json.loads(h.changes_json)
                                    
                                    # Handle specialized Crew display separately if present
                                    if "Crew" in changes:
                                        crew_change = changes.pop("Crew")
                                        c_old = crew_change.get("old", [])
                                        c_new = crew_change.get("new", [])
                                        
                                        st.write("---")
                                        st.write("**👨‍✈️ Crew Changed:**")
                                        h_col1, h_col2 = st.columns(2)
                                        with h_col1:
                                            st.caption("Former Crew")
                                            if c_old:
                                                st.dataframe(pd.DataFrame(c_old), use_container_width=True, hide_index=True)
                                            else:
                                                st.write("None / Initial Scrape")
                                        with h_col2:
                                            st.caption("New Crew")
                                            if c_new:
                                                st.dataframe(pd.DataFrame(c_new), use_container_width=True, hide_index=True)
                                            else:
                                                st.write("Crew Removed")
                                    
                                    # Display other scalar changes
                                    if changes:
                                        st.write("**📝 Other Field Changes:**")
                                        # Convert dict to list of dicts for table
                                        scalar_diffs = []
                                        for k, v in changes.items():
                                            scalar_diffs.append({
                                                "Field": k,
                                                "Old Value": v.get("old", "None"),
                                                "New Value": v.get("new", "None")
                                            })
                                        st.table(pd.DataFrame(scalar_diffs))
                                        
                                except Exception as e:
                                    st.error(f"Error parsing history: {e}")
                                st.divider()

                    st.markdown("### 📋 Operational Data")
                    c_op1, c_op2, c_op3 = st.columns(3)
                    with c_op1:
                        with st.expander("Passenger Data (Pax)"):
                             st.text(detailed_flight.pax_data or "No Pax Data")
                    with c_op2:
                        with st.expander("Load Sheet"):
                             st.text(detailed_flight.load_data or "No Load Data")
                    with c_op3:
                        with st.expander("Notes"):
                             st.text(detailed_flight.notes_data or "No Notes")
                session.close()

            # 2. Historical Schedule Table (HTML for same-window links)
            st.divider()
            st.subheader("Daily Flight Schedule")
            
            # Sorting for Schedule
            col_sort1, col_sort2 = st.columns([2, 1])
            with col_sort1:
                hist_sort_col = st.selectbox("Sort Schedule By", ["Departure", "Flight #", "Tail"], index=0)
            with col_sort2:
                hist_sort_order = st.radio("Hist Order", ["Ascending", "Descending"], horizontal=True, index=0, key="hist_sort_order")
            
            # Map display names to internal names
            sort_map = {
                "Departure": "scheduled_departure",
                "Flight #": "flight_num_clean",
                "Tail": "tail_number"
            }
            
            display_df = df_flights[['flight_number', 'scheduled_departure', 'departure_airport', 'arrival_airport', 'tail_number']].copy()
            display_df['flight_num_clean'] = display_df['flight_number'].apply(clean_fn).astype(int, errors='ignore')
            
            # Sort the data
            is_asc_hist = (hist_sort_order == "Ascending")
            display_df = display_df.sort_values(by=sort_map[hist_sort_col], ascending=is_asc_hist)
            
            # Format display columns
            display_df['formatted_departure'] = pd.to_datetime(display_df['scheduled_departure']).dt.strftime('%H:%M')
            
            # Create the link HTML
            display_df['Flight #'] = display_df.apply(
                lambda r: f"<a href='/?tab=historical&date={view_dt.strftime('%Y-%m-%d')}&flight_num={clean_fn(r['flight_number'])}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{clean_fn(r['flight_number'])}</a>", 
                axis=1
            )
            
            # Rename for final presentation
            render_df = display_df[['Flight #', 'formatted_departure', 'departure_airport', 'arrival_airport', 'tail_number']].rename(columns={
                'formatted_departure': 'Departure',
                'departure_airport': 'Dep',
                'arrival_airport': 'Arr',
                'tail_number': 'Tail'
            })
            
            html_table = render_df.to_html(escape=False, index=False, classes='dataframe')
            
            # Scrollable container
            st.markdown(f"""
            <div style="max-height: 500px; overflow-y: auto; border: 1px solid #444; border-radius: 5px;">
                {html_table}
            </div>
            """, unsafe_allow_html=True)
            st.caption("Click a Flight # to view details above in this window.")
        else:
            st.info("No flights recorded for this date.")
                


# --- TAB 2: Sync Data ---
elif selected_tab == NAV_SYNC:
    st.header("Sync Settings")
    
    # Cloud Configuration Check (using dynamic setting)
    from firestore_lib import is_cloud_sync_enabled
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
        local_sync = get_metadata(session, "last_successful_sync")
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
            with st.status("Performing Full Sync...", expanded=True) as status:
                session = get_db_session()
                from ingest_data import upload_ioe_to_cloud, upload_pairings_to_cloud, upload_flights_to_cloud, upload_metadata_to_cloud
                
                status.write("Uploading IOE assignments...")
                cnt_ioe = upload_ioe_to_cloud(session)
                
                status.write("Uploading scheduled pairings...")
                cnt_pair = upload_pairings_to_cloud(session)
                
                status.write("Uploading historical flight data...")
                cnt_flt = upload_flights_to_cloud(session) # No date range = all
                
                status.write("Uploading application metadata...")
                cnt_meta = upload_metadata_to_cloud(session)
                
                session.close()
                status.update(label="✅ Full Sync Complete!", state="complete", expanded=False)
                st.success(f"Successfully mirrored: {cnt_ioe} IOE, {cnt_pair} Pairings, {cnt_flt} Flights, {cnt_meta} Metadata.")

        st.divider()
        st.caption("Manual Individual Syncs:")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            if st.button("☁️ Upload/Update IOE Assignments"):
                with st.spinner("Uploading IOE data..."):
                    from ingest_data import upload_ioe_to_cloud
                    session = get_db_session()
                    cnt = upload_ioe_to_cloud(session)
                    session.close()
                    st.success(f"Uploaded {cnt} IOE records.")
                    
        with c2:
            if st.button("☁️ Upload/Update Scheduled Pairings"):
                with st.spinner("Uploading Scheduled Flights..."):
                    from ingest_data import upload_pairings_to_cloud
                    session = get_db_session()
                    cnt = upload_pairings_to_cloud(session)
                    session.close()
                    st.success(f"Uploaded {cnt} scheduled flights.")
        
        with c3:
            if st.button("☁️ Upload Scraped Flights History"):
                with st.spinner("Uploading Historical Flights..."):
                    from ingest_data import upload_flights_to_cloud
                    session = get_db_session()
                    
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
             with st.spinner("Restoring data from Cloud..."):
                 from ingest_data import sync_down_from_cloud
                 session = get_db_session()
                 stats = sync_down_from_cloud(session)
                 session.close()
                 st.success(f"Restore Complete! Stats: {stats}")
                 st.balloons()
    else:
        st.info("Cloud sync is disabled. Enable it in the sidebar to use this feature.")

# --- TAB 3: Pairings ---

elif selected_tab == NAV_PAIRINGS:
    st.header("Scheduled Pairings")
    from database import ScheduledFlight
    
    session = get_db_session()
    
    # Filter Controls
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    with col1:
        # Get unique pairing numbers
        pairing_nums = [r[0] for r in session.query(ScheduledFlight.pairing_number).distinct()]
        
        # Determine Default
        p_idx = 0
        p_arg = st.session_state.get("pairing_search_default", "All")
        if p_arg in pairing_nums:
            p_idx = (["All"] + sorted(pairing_nums)).index(p_arg)
        
        sel_pairing = st.selectbox("Filter by Pairing", ["All"] + sorted(pairing_nums), index=p_idx, key="pairing_search")
    
    with col2:
        # Get distinct months from ScheduledFlight
        pairing_dates_q = session.query(ScheduledFlight.pairing_start_date).distinct().all()
        months_set = set()
        for d in pairing_dates_q:
            if d[0]:
                bp_year, bp_month = get_bid_period_from_date(d[0])
                dt_obj = datetime(bp_year, bp_month, 1)
                months_set.add(dt_obj.strftime("%B %Y"))
        
        curr_bp_year, curr_bp_month = get_bid_period_from_date(datetime.now().date())
        current_month_str = datetime(curr_bp_year, curr_bp_month, 1).strftime("%B %Y")
        months_set.add(current_month_str)
            
        months = sorted(list(months_set), key=lambda x: datetime.strptime(x, "%B %Y"), reverse=True)
        default_idx = months.index(current_month_str) if current_month_str in months else 0
        sel_month_str = st.selectbox("Filter by Bid Period", months, index=default_idx, key="month_search")

    with col3:
        sel_date = st.date_input("Filter by Date", value=None, key="date_search")
    
    with col4:
        st.write("") # Spacer
        if st.button("🔄 Reset"):
            st.rerun()

    # Build Query
    query = session.query(ScheduledFlight)
    
    if sel_pairing != "All":
        query = query.filter(ScheduledFlight.pairing_number == sel_pairing)
    
    if sel_date:
        query = query.filter(ScheduledFlight.pairing_start_date == datetime.combine(sel_date, datetime.min.time()))
    elif sel_month_str:
        sel_month_dt = datetime.strptime(sel_month_str, "%B %Y")
        bp_start, bp_end = get_bid_period_date_range(sel_month_dt.year, sel_month_dt.month)
        
        start_dt = datetime.combine(bp_start, datetime.min.time())
        end_dt = datetime.combine(bp_end + timedelta(days=1), datetime.min.time())
        
        query = query.filter(
            ScheduledFlight.pairing_start_date >= start_dt,
            ScheduledFlight.pairing_start_date < end_dt
        )
        
    scheduled_rows = query.order_by(
        ScheduledFlight.pairing_start_date, 
        ScheduledFlight.date, 
        ScheduledFlight.scheduled_departure
    ).limit(1000).all()
    
    data = []
    for sf in scheduled_rows:
        candidates = [sf.flight_number, f"C5{sf.flight_number}", f"C{sf.flight_number}"]
        actual = session.query(Flight).filter(
            Flight.flight_number.in_(candidates), 
            Flight.date == sf.date
        ).first()
        
        crew_str = "N/A"
        status = "Scheduled"
        if actual:
            status = "Flown"
            crews = [c.name for c in actual.crew_members]
            crew_str = "; ".join(crews)
        
        data.append({
            "Trip Start": sf.pairing_start_date.strftime("%Y-%m-%d") if sf.pairing_start_date else "N/A",
            "Leg Date": sf.date.strftime("%Y-%m-%d"),
            "Pairing": sf.pairing_number,
            "Flight": sf.flight_number,
            "Route": f"{sf.departure_airport}-{sf.arrival_airport}",
            "Sch Dep": sf.scheduled_departure,
            "Sch Arr": sf.scheduled_arrival or "N/A",
            "Block": sf.block_time or "N/A",
            "Credit": sf.total_credit or "N/A",
            "Status": status,
            "Actual Crew": crew_str
        })
    
    if data:
        pairings_df = pd.DataFrame(data)
        html_pairings = pairings_df.to_html(index=False, classes='dataframe')
        st.markdown(f"""
        <div style="max-height: 800px; overflow-y: auto; border: 1px solid #444; border-radius: 5px;">
            {html_pairings}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("No pairings found matching filters.")
    session.close()

# --- TAB 4: IOE Audit ---
elif selected_tab == NAV_IOE:
    st.header("IOE Audit Report")
    from database import IOEAssignment, flight_crew_association, ScheduledFlight
    from sqlalchemy import extract
    
    session = get_db_session()
    
    # 1. Bid Period Selector
    # Get distinct months from assignments
    dates_q = session.query(IOEAssignment.start_date).all()
    months_set = set()
    for d in dates_q:
        if d[0]:
            bp_year, bp_month = get_bid_period_from_date(d[0])
            dt_obj = datetime(bp_year, bp_month, 1)
            months_set.add(dt_obj.strftime("%B %Y"))
            
    curr_bp_year, curr_bp_month = get_bid_period_from_date(datetime.now().date())
    current_month_str = datetime(curr_bp_year, curr_bp_month, 1).strftime("%B %Y")
    months_set.add(current_month_str)
            
    months = sorted(list(months_set), key=lambda x: datetime.strptime(x, "%B %Y"), reverse=True)
    default_idx = months.index(current_month_str) if current_month_str in months else 0
    
    selected_month_str = st.selectbox("Select Bid Period", months, index=default_idx) if months else None
    
    if selected_month_str:
        sel_month_dt = datetime.strptime(selected_month_str, "%B %Y")
        bp_start, bp_end = get_bid_period_date_range(sel_month_dt.year, sel_month_dt.month)
        
        start_dt = datetime.combine(bp_start, datetime.min.time())
        end_dt = datetime.combine(bp_end + timedelta(days=1), datetime.min.time())
        
        assignments = session.query(IOEAssignment).filter(
            IOEAssignment.start_date >= start_dt,
            IOEAssignment.start_date < end_dt
        ).order_by(IOEAssignment.start_date.asc()).all()
    else:
        assignments = []
    
    audit_results = []
    
    # Metrics
    total_legs_global = 0
    total_ioe_verified_global = 0
    total_future_legs_global = 0
    total_flown_not_ioe_global = 0
    
    now = datetime.now()
    
    for assign in assignments:
        start_dt = assign.start_date
        
        legs = session.query(ScheduledFlight).filter(
            ScheduledFlight.pairing_number == assign.pairing_number,
            ScheduledFlight.pairing_start_date == start_dt
        ).order_by(ScheduledFlight.date, ScheduledFlight.scheduled_departure).all()
        
        if not legs:
            end_dt = start_dt + timedelta(days=5)
            legs = session.query(ScheduledFlight).filter(
                ScheduledFlight.pairing_number == assign.pairing_number,
                ScheduledFlight.date >= start_dt,
                ScheduledFlight.date <= end_dt
            ).order_by(ScheduledFlight.date, ScheduledFlight.scheduled_departure).all()
        
        total_legs = len(legs)
        legs_flown_by_student = 0
        legs_marked_ioe = 0
        
        details_html = []
        
        for leg in legs:
            leg_date = leg.date
            
            # Link Generation for Flight
            flight_link = f"<a href='/?tab=historical&date={leg_date.strftime('%Y-%m-%d')}&flight_num={leg.flight_number}' target='_self' style='text-decoration:none; font-weight:bold;'>{leg.flight_number}</a>"
            
            candidates = [leg.flight_number, f"C5{leg.flight_number}", f"C{leg.flight_number}"]
            actual = session.query(Flight).filter(
                Flight.flight_number.in_(candidates), 
                Flight.date == leg.date
            ).first()
            
            crew_details = []
            has_ioe_fo = False
            has_ioe_any = False
            captain_name = "Unknown"
            fo_found = False
            ioe_crew_names = []

            if actual:
                for cm in actual.crew_members:
                    assoc = session.execute(
                        flight_crew_association.select().where(
                            (flight_crew_association.c.flight_id == actual.id) &
                            (flight_crew_association.c.crew_id == cm.id)
                        )
                    ).fetchone()
                    
                    role = assoc.role or ""
                    if role and "FA" in role.upper():
                        continue
                        
                    name_role = f"{cm.name} ({role})"
                    crew_details.append(name_role)
                    
                    r_up = role.upper()
                    if "CAPTAIN" in r_up or "CA" == r_up:
                         captain_name = cm.name
                    
                    if "FIRST OFFICER" in r_up or "FO" in r_up:
                         fo_found = True
                    
                    if assoc and "IOE" in (assoc.flags or ""):
                        ioe_crew_names.append(name_role)
                        has_ioe_any = True
                        if "FIRST OFFICER" in r_up or "FO" in r_up:
                            has_ioe_fo = True
            
            if leg_date.date() > now.date():
                if actual:
                    if has_ioe_fo:
                        legs_marked_ioe += 1
                        details_html.append(f"{flight_link}: Future Trip (IOE: {', '.join(ioe_crew_names)})")
                    else:
                        fo_status = "No FO" if not fo_found else "FO Present (No IOE)"
                        details_html.append(f"{flight_link}: Future Trip (CA: {captain_name}; {fo_status})")
                else:
                    details_html.append(f"{flight_link}: Future Trip (Not Scraped)")
                
                total_future_legs_global += 1
                continue
            
            leg_status = "Not Scraped"
            
            if not actual and leg_date.date() == now.date():
                 leg_status = "In Progress (Not Scraped)"
            elif actual:
                 student_present = any(c.employee_id == assign.employee_id for c in actual.crew_members)
                 
                 if student_present:
                     legs_flown_by_student += 1
                     leg_status = "Flown"
                     if has_ioe_any:
                         legs_marked_ioe += 1
                         leg_status += f" (IOE: {', '.join(ioe_crew_names)})"
                     else:
                         total_flown_not_ioe_global += 1
                         leg_status += f" (No IOE flags, Crew: {'; '.join(crew_details)})"
                 else:
                     leg_status = "Student Missing"
            
            details_html.append(f"{flight_link}: {leg_status}")

        total_legs_global += total_legs
        total_ioe_verified_global += legs_marked_ioe
        
        if total_legs == 0:
            details_html.append("⚠️ No schedule data found for this pairing")

        # Pairing Link
        p_link = f"<a href='/?tab=pairings&pairing={assign.pairing_number}' target='_self' style='text-decoration:none; color:#E694FF;'>{assign.pairing_number}</a>"
        
        audit_results.append({
            "Check Airman": assign.employee_id,
            "Pairing": p_link,
            "Start": start_dt.strftime("%Y-%m-%d"),
            "Legs Count": total_legs,
            "IOE Verified": legs_marked_ioe,
            "Details": f"<div style='line-height:1.6;'>{'<br>'.join(details_html)}</div>"
        })
        
    session.close()
    
    # -- Metrics Display --
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Assignments", len(assignments))
    m2.metric("Total Flight Legs", total_legs_global)
    
    rate = 0.0
    future_rate = 0.0
    if total_legs_global > 0:
        rate = (total_ioe_verified_global / total_legs_global) * 100
        future_rate = (total_future_legs_global / total_legs_global) * 100
           
    m3.metric("IOE Verified Rate", f"{rate:.1f}%")
    m4.metric("Future Trip Rate", f"{future_rate:.1f}%")
    
    # HTML Table Rendering with Sorting
    if audit_results:
        df_audit = pd.DataFrame(audit_results)
        
        # Sorting UI
        st.write("---")
        c1, c2 = st.columns([2, 1])
        with c1:
            sort_col = st.selectbox("Sort Table By", ["Start", "Legs Count", "Check Airman", "IOE Verified"], index=0)
        with c2:
            sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True, index=0)
        
        is_asc = (sort_order == "Ascending")
        df_audit = df_audit.sort_values(by=sort_col, ascending=is_asc)

        # Use simple pandas to html
        html = df_audit.to_html(escape=False, index=False, classes='dataframe')
        st.markdown(f"""
        <div style="max-height: 600px; overflow-y: auto; border: 1px solid #444; border-radius: 5px;">
            {html}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("No assignments found for this period.")
    
    st.caption("Auto-generated audit based on scraped flight logs vs assigned IOE pairings.")
    
    # --- Unscheduled IOE Flights ---
    st.divider()
    st.subheader("🔍 Unscheduled IOE Flights")
    st.caption("Flights marked with IOE flag but not part of official IOE assignments for this month")
    
    # Get all employee IDs from assignments for this month
    if selected_month_str:
        session = get_db_session()
        sel_month_dt = datetime.strptime(selected_month_str, "%B %Y")
        
        # Get assigned pairings for this month (not just employee IDs)
        assigned_pairings = set([a.pairing_number for a in assignments])
        
        # Query all flights in this bid period with IOE flags
        bp_start, bp_end = get_bid_period_date_range(sel_month_dt.year, sel_month_dt.month)
        month_start = datetime.combine(bp_start, datetime.min.time())
        month_end = datetime.combine(bp_end + timedelta(days=1), datetime.min.time())
        
        # Get all flights in this month
        flights_in_month = session.query(Flight).filter(
            Flight.date >= month_start,
            Flight.date < month_end
        ).all()
        
        unscheduled_ioe = []
        
        for flight in flights_in_month:
            # Check each crew member for IOE flag
            for crew in flight.crew_members:
                # Get association to check flags
                assoc = session.execute(
                    flight_crew_association.select().where(
                        (flight_crew_association.c.flight_id == flight.id) &
                        (flight_crew_association.c.crew_id == crew.id)
                    )
                ).fetchone()
                
                if assoc and "IOE" in (assoc.flags or ""):
                    # This crew member has IOE flag
                    # Skip FAs - we only care about pilot IOE
                    if assoc.role and "FA" in assoc.role.upper():
                        continue
                    
                    # Look up pairing number from ScheduledFlight
                    flight_num_clean = flight.flight_number
                    if flight_num_clean.startswith('C5'):
                        flight_num_clean = flight_num_clean[2:]
                    elif flight_num_clean.startswith('C'):
                        flight_num_clean = flight_num_clean[1:]
                    scheduled = session.query(ScheduledFlight).filter(
                        ScheduledFlight.flight_number == flight_num_clean,
                        ScheduledFlight.date == flight.date
                    ).first()
                    
                    pairing_num = scheduled.pairing_number if scheduled else "Unknown"
                    
                    # Check if this pairing is in the official IOE assignment list
                    if pairing_num not in assigned_pairings:
                        unscheduled_ioe.append({
                            "Date": flight.date.strftime("%Y-%m-%d"),
                            "Flight": flight.flight_number,
                            "Pairing": pairing_num,
                            "Employee ID": crew.employee_id,
                            "Name": crew.name,
                            "Role": assoc.role,
                            "Flags": assoc.flags or "",
                            "Route": f"{flight.departure_airport}-{flight.arrival_airport}",
                            "Tail": flight.tail_number or "N/A"
                        })
        
        session.close()
        
        if unscheduled_ioe:
            df_unscheduled = pd.DataFrame(unscheduled_ioe)
            st.warning(f"Found {len(unscheduled_ioe)} IOE flight(s) not in official assignments")
            st.markdown(df_unscheduled.to_html(index=False, classes='dataframe'), unsafe_allow_html=True)
        else:
            st.success("✓ All IOE-flagged flights match official assignments")
    else:
        st.info("Select a month to view unscheduled IOE flights")
    
    # --- Ad-Hoc IOE Pairings ---
    st.divider()
    st.subheader("📊 Ad-Hoc IOE Pairings")
    st.caption("Pairings used for IOE but not in the official withheld list - grouped by pairing")
    
    if selected_month_str:
        session = get_db_session()
        sel_month_dt = datetime.strptime(selected_month_str, "%B %Y")
        
        # Get assigned pairings for this month
        assigned_pairings = set([a.pairing_number for a in assignments])
        
        # Query all flights in this bid period
        bp_start, bp_end = get_bid_period_date_range(sel_month_dt.year, sel_month_dt.month)
        month_start = datetime.combine(bp_start, datetime.min.time())
        month_end = datetime.combine(bp_end + timedelta(days=1), datetime.min.time())
        
        flights_in_month = session.query(Flight).filter(
            Flight.date >= month_start,
            Flight.date < month_end
        ).all()
        
        # Track pairings with IOE flags
        pairing_stats = {}  # pairing_num -> {total_legs, ioe_legs, dates}
        
        for flight in flights_in_month:
            # Look up pairing
            flight_num_clean = flight.flight_number
            if flight_num_clean.startswith('C5'):
                flight_num_clean = flight_num_clean[2:]
            elif flight_num_clean.startswith('C'):
                flight_num_clean = flight_num_clean[1:]
            scheduled = session.query(ScheduledFlight).filter(
                ScheduledFlight.flight_number == flight_num_clean,
                ScheduledFlight.date == flight.date
            ).first()
            
            if not scheduled:
                continue
            
            pairing_num = scheduled.pairing_number
            
            # Skip if this is an official IOE pairing
            if pairing_num in assigned_pairings:
                continue
            
            # Initialize pairing stats if needed
            if pairing_num not in pairing_stats:
                pairing_stats[pairing_num] = {
                    'total_legs': 0,
                    'ioe_legs': 0,
                    'dates': set()
                }
            
            pairing_stats[pairing_num]['total_legs'] += 1
            pairing_stats[pairing_num]['dates'].add(flight.date.strftime('%Y-%m-%d'))
            
            # Check if this flight has IOE flags (non-FA)
            has_ioe = False
            for crew in flight.crew_members:
                assoc = session.execute(
                    flight_crew_association.select().where(
                        (flight_crew_association.c.flight_id == flight.id) &
                        (flight_crew_association.c.crew_id == crew.id)
                    )
                ).fetchone()
                
                if assoc and "IOE" in (assoc.flags or ""):
                    if not (assoc.role and "FA" in assoc.role.upper()):
                        has_ioe = True
                        break
            
            if has_ioe:
                pairing_stats[pairing_num]['ioe_legs'] += 1
        
        session.close()
        
        # Filter to only pairings that have at least one IOE leg
        adhoc_pairings = []
        for pairing, stats in pairing_stats.items():
            if stats['ioe_legs'] > 0:
                date_range = sorted(list(stats['dates']))
                adhoc_pairings.append({
                    'Pairing': pairing,
                    'Total Legs': stats['total_legs'],
                    'IOE Legs': stats['ioe_legs'],
                    'IOE %': f"{(stats['ioe_legs'] / stats['total_legs'] * 100):.0f}%",
                    'Date Range': f"{date_range[0]} to {date_range[-1]}" if len(date_range) > 1 else date_range[0]
                })
        
        if adhoc_pairings:
            df_adhoc = pd.DataFrame(adhoc_pairings)
            df_adhoc = df_adhoc.sort_values('IOE Legs', ascending=False)
            st.warning(f"Found {len(adhoc_pairings)} pairing(s) used for IOE but not in official list")
            st.markdown(df_adhoc.to_html(index=False, classes='dataframe'), unsafe_allow_html=True)
        else:
            st.success("✓ No ad-hoc IOE pairings found")
    else:
        st.info("Select a month to view ad-hoc IOE pairings")

