import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from config import NOC_USERNAME, NOC_PASSWORD
from scraper import NOCScraper
from database import get_session, Flight, CrewMember, DailySyncStatus, init_db
from sqlalchemy import desc

# Page Config
st.set_page_config(page_title="NOC Mobile Scraper", layout="wide", page_icon="✈️")

st.title("✈️ NOC Mobile Scraper & Archiver")

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

# Tabs
tab_explore, tab_sync, tab_pairings, tab_ioe = st.tabs(["📅 Historical Data", "🔄 Sync Data", "📋 Pairings", "🎓 IOE Audit"])

# --- TAB 1: Historical Data ---
with tab_explore:
    st.header("Daily Flight Logs")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Date Picker for History
        view_date = st.date_input("Select Date to View", datetime.today())
        
        # Check if we have data for this date
        session = get_db_session()
        view_dt = datetime.combine(view_date, datetime.min.time())
        status_rec = session.query(DailySyncStatus).get(view_dt)
        
        if status_rec:
            st.success(f"**Status:** {status_rec.status}")
            st.write(f"**Last Sync:** {status_rec.last_scraped_at.strftime('%Y-%m-%d %H:%M')}")
            st.write(f"**Flights:** {status_rec.flights_found}")
        else:
            st.warning("No data synced for this date yet.")
            
    with col2:
        # Load Data
        flights_query = session.query(Flight).filter(Flight.date >= view_dt, Flight.date < view_dt + timedelta(days=1))
        df_flights = pd.read_sql(flights_query.statement, session.bind)
        session.close()
        
        if not df_flights.empty:
            # 1. Master List
            display_df = df_flights[['flight_number', 'scheduled_departure', 'departure_airport', 'arrival_airport', 'tail_number', 'status']].copy()
            # Format times
            display_df['scheduled_departure'] = pd.to_datetime(display_df['scheduled_departure']).dt.strftime('%H:%M')
            
            st.dataframe(display_df, width='stretch', hide_index=True)
            
            st.divider()
            
            # 2. Drill Down Selection
            flight_opts = df_flights['flight_number'].tolist()
            selected_flight_num = st.selectbox("Select Flight to View Details", flight_opts)
            
            if selected_flight_num:
                # Query full object
                session = get_db_session()
                # Use date filter to be precise (duplicate flight numbers possible on same day?)
                # Assuming flight number unique per day or taking first
                detailed_flight = session.query(Flight).filter(
                    Flight.flight_number == selected_flight_num,
                    Flight.date >= view_dt, 
                    Flight.date < view_dt + timedelta(days=1)
                ).first()
                
                if detailed_flight:
                    # -- Header --
                    st.subheader(f"✈️ Flight {detailed_flight.flight_number}")
                    
                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                    m_col1.metric("Tail", detailed_flight.tail_number or "N/A")
                    m_col2.metric("Type/Ver", f"{detailed_flight.aircraft_type or '--'} / {detailed_flight.version or '--'}")
                    m_col3.metric("Route", f"{detailed_flight.departure_airport} ➝ {detailed_flight.arrival_airport}")
                    
                    # Times (STD / STA)
                    # Helper
                    def fmt_pair(local_dt, utc_dt):
                        l = local_dt.strftime('%H:%M') if local_dt else "--"
                        u = utc_dt.strftime('%H:%M') if utc_dt else "--"
                        return f"{l} (L) | {u} (Z)"

                    m_col4.metric("Departure (STD)", fmt_pair(detailed_flight.scheduled_departure_local, detailed_flight.scheduled_departure))
                    st.metric("Arrival (STA)", fmt_pair(detailed_flight.scheduled_arrival_local, detailed_flight.scheduled_arrival))

                    
                    # -- Crew Section --
                    st.markdown("### 👨‍✈️ Crew")
                    # We need to query the association to get flags
                    from database import flight_crew_association
                    stmt = flight_crew_association.select().where(flight_crew_association.c.flight_id == detailed_flight.id)
                    assoc_rows = session.execute(stmt).fetchall()
                    
                    crew_list = []
                    for row in assoc_rows:
                        # Get member details
                        cm = session.query(CrewMember).get(row.crew_id)
                        crew_list.append({
                            "Role": row.role,
                            "Name": cm.name,
                            "ID": cm.employee_id,
                            "Flags": row.flags or "" 
                        })
                    
                    if crew_list:
                        st.dataframe(pd.DataFrame(crew_list), width='stretch', hide_index=True)
                    else:
                        st.info("No crew parsed for this flight.")

                    # -- Operational Data --
                    st.markdown("### 📋 Operational Data")
                    with st.expander("Passenger Data (Pax)", expanded=False):
                        st.text(detailed_flight.pax_data or "No Pax Data")
                    
                    with st.expander("Load Sheet", expanded=False):
                        st.text(detailed_flight.load_data or "No Load Data")
                        
                    with st.expander("Notes", expanded=False):
                        st.text(detailed_flight.notes_data or "No Notes")
                        
                session.close()
                
        else:
            st.info("No flights recorded for this date.")

# --- TAB 2: Sync Data ---
with tab_sync:
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
                        
                        # Optional: Auto-push to cloud if enabled?
                        # For now, let's keep it manual or rely on scraper implementation if we changed it. 
                        # But we didn't change scraper.py to auto-push yet.
                        
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
        if st.button("☁️ Full Sync: Mirror ALL Local Data to Cloud", type="secondary", use_container_width=True):
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
                    # optionally filter by date range selected above?
                    # Let's use the date picker from scraper section if user wants, 
                    # but maybe default to all or a different picker.
                    # For simplicity, let's just upload ALL for now or reuse the picker?
                    # Reusing the picker seems smart.
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

with tab_pairings:
    st.header("Scheduled Pairings (Dec 2025)")
    from database import ScheduledFlight
    
    session = get_db_session()
    
    # Filter Controls
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        # Get unique pairing numbers
        pairing_nums = [r[0] for r in session.query(ScheduledFlight.pairing_number).distinct()]
        sel_pairing = st.selectbox("Filter by Pairing", ["All"] + sorted(pairing_nums), key="pairing_search")
    
    with col2:
        sel_date = st.date_input("Filter by Date", value=None, key="date_search")
    
    with col3:
        st.write("") # Spacer
        if st.button("🔄 Reset"):
            st.rerun()

    # Build Query
    query = session.query(ScheduledFlight)
    
    # Logic Change: Grouping by Pairing Instance
    if sel_pairing != "All":
        query = query.filter(ScheduledFlight.pairing_number == sel_pairing)
        if sel_date:
            # Show the specific instance starting on this date
            query = query.filter(ScheduledFlight.pairing_start_date == datetime.combine(sel_date, datetime.min.time()))
    elif sel_date:
        # Show all pairings starting on this date
        query = query.filter(ScheduledFlight.pairing_start_date == datetime.combine(sel_date, datetime.min.time()))
        
    scheduled_rows = query.order_by(
        ScheduledFlight.pairing_start_date, 
        ScheduledFlight.date, 
        ScheduledFlight.scheduled_departure
    ).limit(1000).all()
    
    # Correlate
    data = []
    for sf in scheduled_rows:
        # Find Actual (Handle 'C' or 'C5' prefix mismatch)
        # Scraped flights often have 'C5' prefix (e.g. C54945)
        candidates = [sf.flight_number, f"C5{sf.flight_number}", f"C{sf.flight_number}"]
        actual = session.query(Flight).filter(
            Flight.flight_number.in_(candidates), 
            Flight.date == sf.date
        ).first()
        
        crew_str = "N/A"
        status = "Scheduled"
        if actual:
            status = "Flown"
            # Get crew names
            crews = [c.name for c in actual.crew_members]
            crew_str = ", ".join(crews)
        
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
    
    st.dataframe(pd.DataFrame(data), width='stretch')
    session.close()

# --- TAB 4: IOE Audit ---
with tab_ioe:
    st.header("IOE Audit Report")
    from database import IOEAssignment, flight_crew_association
    from sqlalchemy import extract
    
    session = get_db_session()
    
    # 1. Month Selector
    # Get distinct months from assignments
    dates_q = session.query(IOEAssignment.start_date).all()
    # Unique months set
    months_set = set()
    for d in dates_q:
        if d[0]:
            months_set.add(d[0].strftime("%B %Y"))
            
    months = sorted(list(months_set), key=lambda x: datetime.strptime(x, "%B %Y"), reverse=True)
    
    selected_month_str = st.selectbox("Select Month", months) if months else None
    
    if selected_month_str:
        sel_month_dt = datetime.strptime(selected_month_str, "%B %Y")
        assignments = session.query(IOEAssignment).filter(
            extract('month', IOEAssignment.start_date) == sel_month_dt.month,
            extract('year', IOEAssignment.start_date) == sel_month_dt.year
        ).all()
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
        
        # Use pairing_start_date for precise leg matching
        legs = session.query(ScheduledFlight).filter(
            ScheduledFlight.pairing_number == assign.pairing_number,
            ScheduledFlight.pairing_start_date == start_dt
        ).order_by(ScheduledFlight.date, ScheduledFlight.scheduled_departure).all()
        
        # Fallback to date range if no legs found with specific start date (legacy data)
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
        
        details = []
        
        for leg in legs:
            leg_date = leg.date
            
            # Check Actual (Handle 'C' or 'C5' prefix) - do this for both past and future
            candidates = [leg.flight_number, f"C5{leg.flight_number}", f"C{leg.flight_number}"]
            actual = session.query(Flight).filter(
                Flight.flight_number.in_(candidates), 
                Flight.date == leg.date
            ).first()
            
            # Future Check - but still verify IOE if crew data exists
            if leg_date.date() > now.date():
                if actual:
                    # Check if there's an FO with IOE flag and collect crew names
                    has_ioe_fo = False
                    ioe_crew_names = []
                    
                    for cm in actual.crew_members:
                        assoc = session.execute(
                            flight_crew_association.select().where(
                                (flight_crew_association.c.flight_id == actual.id) &
                                (flight_crew_association.c.crew_id == cm.id)
                            )
                        ).fetchone()
                        
                        if assoc and "IOE" in (assoc.flags or ""):
                            ioe_crew_names.append(f"{cm.name} ({assoc.role})")
                            if "FO" in (assoc.role or "").upper():
                                has_ioe_fo = True
                    
                    if has_ioe_fo:
                        legs_marked_ioe += 1
                        details.append(f"{leg.flight_number}: Future Trip (IOE: {', '.join(ioe_crew_names)})")
                    else:
                        details.append(f"{leg.flight_number}: Future Trip (No IOE FO)")
                else:
                    details.append(f"{leg.flight_number}: Future Trip (Not Scraped)")
                
                total_future_legs_global += 1
                continue
            
            # Check Actual (Handle 'C' or 'C5' prefix)
            candidates = [leg.flight_number, f"C5{leg.flight_number}", f"C{leg.flight_number}"]
            actual = session.query(Flight).filter(
                Flight.flight_number.in_(candidates), 
                Flight.date == leg.date
            ).first()
            
            leg_status = "Not Scraped"
            
            # In Progress Check (Today)
            if not actual and leg_date.date() == now.date():
                 leg_status = "In Progress (Not Scraped)"
            elif actual:
                 # Check student presence
                 student_present = any(c.employee_id == assign.employee_id for c in actual.crew_members)
                 
                 if student_present:
                     legs_flown_by_student += 1
                     leg_status = "Flown"
                     
                     # Check IOE Flag on ANY crew member (not just the student)
                     # This catches cases where CA has IOE flag but FO doesn't, etc.
                     is_ioe = False
                     ioe_crew_names = []
                     
                     for cm in actual.crew_members:
                         # Get association details for this crew member
                         assoc = session.execute(
                             flight_crew_association.select().where(
                                 (flight_crew_association.c.flight_id == actual.id) &
                                 (flight_crew_association.c.crew_id == cm.id)
                             )
                         ).fetchone()
                         
                         if assoc and "IOE" in (assoc.flags or ""):
                             is_ioe = True
                             ioe_crew_names.append(f"{cm.name} ({assoc.role})")
                     
                     if is_ioe:
                         legs_marked_ioe += 1
                         leg_status += f" (IOE: {', '.join(ioe_crew_names)})"
                     else:
                         total_flown_not_ioe_global += 1
                         leg_status += " (No IOE flags)"
                 else:
                     leg_status = "Student Missing"
            
            details.append(f"{leg.flight_number}: {leg_status}")

        total_legs_global += total_legs
        total_ioe_verified_global += legs_marked_ioe
        
        audit_results.append({
            "Check Airman": assign.employee_id,
            "Pairing": assign.pairing_number,
            "Start": start_dt.strftime("%Y-%m-%d"),
            "Legs Count": total_legs,
            "IOE Verified": legs_marked_ioe,
            "Details": "; ".join(details)
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
    
    df_audit = pd.DataFrame(audit_results)
    st.dataframe(df_audit, width='stretch')
    
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
        
        # Query all flights in this month with IOE flags
        month_start = sel_month_dt.replace(day=1)
        if sel_month_dt.month == 12:
            month_end = sel_month_dt.replace(year=sel_month_dt.year + 1, month=1, day=1)
        else:
            month_end = sel_month_dt.replace(month=sel_month_dt.month + 1, day=1)
        
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
            st.dataframe(df_unscheduled, width='stretch')
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
        
        # Query all flights in this month
        month_start = sel_month_dt.replace(day=1)
        if sel_month_dt.month == 12:
            month_end = sel_month_dt.replace(year=sel_month_dt.year + 1, month=1, day=1)
        else:
            month_end = sel_month_dt.replace(month=sel_month_dt.month + 1, day=1)
        
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
            st.dataframe(df_adhoc, width='stretch')
        else:
            st.success("✓ No ad-hoc IOE pairings found")
    else:
        st.info("Select a month to view ad-hoc IOE pairings")

