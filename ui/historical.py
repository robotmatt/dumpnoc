
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database import get_session, Flight, CrewMember, DailySyncStatus, FlightHistory
from sqlalchemy import desc
import json

def render_historical_tab():
    # Header layout with Date Picker
    h_col1, h_col2 = st.columns([3, 1])
    with h_col1:
        st.header("Daily Flight Logs")
    with h_col2:
        d_val = st.session_state.get("history_date_default", datetime.today())
        view_date = st.date_input("Select Date", d_val, label_visibility="collapsed")
        
    session = get_session()
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
                session = get_session()
                candidates = [selected_flight_val, f"C5{selected_flight_val}", f"C{selected_flight_val}"]
                
                detailed_flight = session.query(Flight).filter(
                    Flight.flight_number.in_(candidates),
                    Flight.date >= view_dt, 
                    Flight.date < view_dt + timedelta(days=1)
                ).first()
                
                if detailed_flight:
                    st.subheader(f"✈️ Flight {detailed_flight.flight_number}")
                    
                    # Status Badge
                    if detailed_flight.status:
                        s_up = detailed_flight.status.upper()
                        if "CANCELED" in s_up:
                            st.error(f"⚠️ STATUS: {detailed_flight.status}")
                        elif "DELAYED" in s_up:
                            st.warning(f"🕒 STATUS: {detailed_flight.status}")
                        elif "FLOWN" in s_up:
                            st.success(f"✅ STATUS: {detailed_flight.status}")
                        else:
                            st.info(f"STATUS: {detailed_flight.status}")

                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                    m_col1.metric("Tail", detailed_flight.tail_number or "N/A")
                    m_col2.metric("Type/Ver", f"{detailed_flight.aircraft_type or '--'} / {detailed_flight.version or '--'}")
                    m_col3.metric("Departure Airport", detailed_flight.departure_airport or "N/A")
                    m_col4.metric("Arrival Airport", detailed_flight.arrival_airport or "N/A")
                    
                    def fmt_pair(local_dt, utc_dt):
                        l = local_dt.strftime('%H:%M') if local_dt else "--"
                        u = utc_dt.strftime('%H:%M') if utc_dt else "--"
                        return f"{l} (L) | {u} (Z)"
                    
                    def fmt_actual(local_dt):
                        return local_dt.strftime('%H:%M') if local_dt else "--"

                    # Row for Times
                    c_t1, c_t2, c_t3, c_t4 = st.columns(4)
                    c_t1.metric("Dep Scheduled", fmt_pair(detailed_flight.scheduled_departure, detailed_flight.scheduled_departure_utc))
                    c_t2.metric("Dep Actual", fmt_actual(detailed_flight.actual_departure))
                    
                    c_t3.metric("Arr Scheduled", fmt_pair(detailed_flight.scheduled_arrival, detailed_flight.scheduled_arrival_utc))
                    c_t4.metric("Arr Actual", fmt_actual(detailed_flight.actual_arrival))

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
                                                st.dataframe(pd.DataFrame(c_old), width='stretch', hide_index=True)
                                            else:
                                                st.write("None / Initial Scrape")
                                        with h_col2:
                                            st.caption("New Crew")
                                            if c_new:
                                                st.dataframe(pd.DataFrame(c_new), width='stretch', hide_index=True)
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
                lambda r: f"<a href='/?date={view_dt.strftime('%Y-%m-%d')}&flight_num={clean_fn(r['flight_number'])}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{clean_fn(r['flight_number'])}</a>", 
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
