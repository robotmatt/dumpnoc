
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database import get_session, Flight, CrewMember, DailySyncStatus, FlightHistory, flight_crew_association
from sqlalchemy import desc, and_
import json

@st.cache_data(ttl=3600)
def get_all_crew_cached():
    session = get_session()
    # Pull only necessary columns for performance
    crew = session.query(CrewMember.name, CrewMember.employee_id).order_by(CrewMember.name).all()
    session.close()
    return [{"name": c.name, "id": c.employee_id, "label": f"{c.name} ({c.employee_id})"} for c in crew]

@st.cache_data(ttl=3600)
def get_airports_cached():
    session = get_session()
    deps = session.query(Flight.departure_airport).filter(Flight.departure_airport != None).distinct().all()
    arrs = session.query(Flight.arrival_airport).filter(Flight.arrival_airport != None).distinct().all()
    session.close()
    
    all_apts = sorted(list(set([a[0] for a in deps] + [a[0] for a in arrs])))
    return all_apts

def render_historical_tab():
    # Header layout with Date Picker
    h_col1, h_col2 = st.columns([3, 1])
    with h_col1:
        st.header("Daily Flight Logs")
    with h_col2:
        d_val = st.session_state.get("history_date_default", datetime.today())
        view_date = st.date_input("Select Date", d_val, label_visibility="collapsed")
    
    # 2. URL Sync (Deep-linking)
    query_params = st.query_params
    url_flight = query_params.get("flight_num", "")
    if url_flight and st.session_state.get("last_synced_hist_flight") != url_flight:
        st.session_state["hist_flight_selector"] = url_flight
        st.session_state["last_synced_hist_flight"] = url_flight
    view_dt = datetime.combine(view_date, datetime.min.time())
    session = get_session()
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

            # 1. Detailed View (Now at the top)
            flight_opts = [clean_fn(f) for f in df_flights['flight_number'].tolist()]
            
            # If the current selection in session state isn't in available options (e.g. date changed)
            # or if it's the first run, initialize/validate the session state key.
            current_sel = st.session_state.get("hist_flight_selector")
            if current_sel not in flight_opts:
                 st.session_state["hist_flight_selector"] = flight_opts[0] if flight_opts else None
            
            selected_flight_val = st.selectbox(
                "Select Flight to View Details", 
                flight_opts, 
                key="hist_flight_selector"
            )
            
            if selected_flight_val:
                st.session_state["last_synced_hist_flight"] = selected_flight_val
                # Sync to URL
                if query_params.get("flight_num") != selected_flight_val:
                    st.query_params.update(date=view_date.strftime("%Y-%m-%d"), flight_num=selected_flight_val)
                
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

                    if detailed_flight.has_duplicate_warning:
                        st.error("⚠️ WARNING: Scraper flagged an ambiguous duplicate for this flight during OOOI parsing.")

                    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                    m_col1.metric("Tail", detailed_flight.tail_number or "N/A")
                    m_col2.metric("Type/Ver", f"{detailed_flight.aircraft_type or '--'} / {detailed_flight.version or '--'}")
                    m_col3.metric("Departure Airport", detailed_flight.departure_airport or "N/A")
                    m_col4.metric("Arrival Airport", detailed_flight.arrival_airport or "N/A")
                    
                    def fmt_pair(local_dt, utc_dt):
                        l = local_dt.strftime('%H:%M') if local_dt else "--"
                        u = utc_dt.strftime('%H:%M') if utc_dt else "--"
                        return f"{l} (L) | {u} (Z)"
                    
                    def fmt_actual(local_dt, utc_dt=None):
                        l = local_dt.strftime('%H:%M') if local_dt else "--"
                        if utc_dt:
                            u = utc_dt.strftime('%H:%M') if utc_dt else "--"
                            return f"{l} (L) | {u} (Z)"
                        return l

                    # Row for Times
                    c_t1, c_t2, c_t3, c_t4 = st.columns(4)
                    c_t1.metric("Sch Out", fmt_pair(detailed_flight.scheduled_departure, detailed_flight.scheduled_departure_utc))
                    c_t2.metric("Act Out", fmt_actual(detailed_flight.actual_out, detailed_flight.actual_out_utc))
                    
                    c_t3.metric("Sch In", fmt_pair(detailed_flight.scheduled_arrival, detailed_flight.scheduled_arrival_utc))
                    c_t4.metric("Act In", fmt_actual(detailed_flight.actual_in, detailed_flight.actual_in_utc))
                    
                    def fmt_block(total_minutes):
                        if total_minutes is None: return "--"
                        h = abs(total_minutes) // 60
                        m = abs(total_minutes) % 60
                        sign = "-" if total_minutes < 0 else ""
                        return f"{sign}{h}:{m:02d}"

                    # Row for Off / On and Block
                    c_o1, c_o2, c_o3, c_o4 = st.columns(4)
                    c_o1.metric("Act Off", fmt_actual(detailed_flight.actual_off, detailed_flight.actual_off_utc))
                    c_o2.metric("Act On", fmt_actual(detailed_flight.actual_on, detailed_flight.actual_on_utc))
                    c_o3.metric("Planned Block", fmt_block(detailed_flight.planned_block_minutes))
                    c_o4.metric("Actual Block", fmt_block(detailed_flight.actual_block_minutes))

                    st.markdown("### 👨‍✈️ Crew")
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
                        with st.expander("📜 Flight Change History", expanded=True):
                            history_rows = []
                            for h in history_records:
                                try:
                                    changes = json.loads(h.changes_json)
                                    ts_str = h.timestamp.strftime('%m/%d %H:%M')
                                    
                                    for field, vals in changes.items():
                                        old_val = vals.get("old")
                                        new_val = vals.get("new")
                                        
                                        if field == "Crew":
                                            if not isinstance(old_val, list): old_val = []
                                            if not isinstance(new_val, list): new_val = []
                                            
                                            old_map = {str(c.get("id")): c for c in old_val if c.get("id")}
                                            new_map = {str(c.get("id")): c for c in new_val if c.get("id")}
                                            
                                            added_ids = set(new_map.keys()) - set(old_map.keys())
                                            removed_ids = set(old_map.keys()) - set(new_map.keys())
                                            stayed_ids = set(old_map.keys()) & set(new_map.keys())
                                            
                                            diffs = []
                                            for i in added_ids:
                                                c = new_map[i]
                                                diffs.append(f"🟢 Added: {c.get('role','')} {c.get('name','')}")
                                            for i in removed_ids:
                                                c = old_map[i]
                                                diffs.append(f"🔴 Removed: {c.get('role','')} {c.get('name','')}")
                                            for i in stayed_ids:
                                                o, n = old_map[i], new_map[i]
                                                if o.get("role") != n.get("role") or o.get("flags") != n.get("flags"):
                                                    diffs.append(f"🟡 Updated {n.get('name','')}: {o.get('role','')}->{n.get('role','')} | Flags: '{o.get('flags','')}'->'{n.get('flags','')}'")
                                            
                                            if not old_val and new_val:
                                                to_val = f"Initial Scrape: {len(new_val)} members"
                                            else:
                                                to_val = "\n".join(diffs) if diffs else "No Change in List"
                                                
                                            history_rows.append({
                                                "Time": ts_str,
                                                "Field": "👨‍✈️ Crew Changed",
                                                "From": f"{len(old_val)} members",
                                                "To": to_val
                                            })
                                        else:
                                            history_rows.append({
                                                "Time": ts_str,
                                                "Field": field,
                                                "From": str(old_val) if old_val is not None else "None",
                                                "To": str(new_val) if new_val is not None else "None"
                                            })
                                except:
                                    continue
                            
                            if history_rows:
                                st.dataframe(pd.DataFrame(history_rows), width="stretch", hide_index=True)
                            else:
                                st.info("No detailed history available for this flight.")

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
            
            # --- Metrics ---
            m1, m2, m3 = st.columns(3)
            num_scheduled = len(df_flights)
            num_flown = df_flights['status'].str.upper().str.contains('FLOWN', na=False).sum()
            num_canceled = df_flights['status'].str.upper().str.contains('CANCELED', na=False).sum()
            
            m1.metric("Flights Scheduled", num_scheduled)
            m2.metric("Flights Flown", num_flown)
            m3.metric("Flights Canceled", num_canceled)

            st.markdown("#### 🔍 Filters")
            f_col1, f_col2, f_col3 = st.columns(3)
            
            all_crew = get_all_crew_cached()
            all_apts = get_airports_cached()
            
            with f_col1:
                filter_person = st.multiselect(
                    "Filter by Person", 
                    options=all_crew,
                    format_func=lambda x: x["label"],
                    key="hist_filter_person"
                )
            with f_col2:
                filter_dep = st.multiselect("Departure Airport", options=all_apts, key="hist_filter_dep")
            with f_col3:
                filter_arr = st.multiselect("Destination Airport", options=all_apts, key="hist_filter_arr")

            # FETCH CREW DATA IN BULK (Performance Boost)
            session = get_session()
            f_ids = df_flights['id'].tolist()
            
            # Efficiently fetch all crew for these flights in ONE query
            crew_stmt = session.query(
                flight_crew_association.c.flight_id,
                CrewMember.name,
                CrewMember.employee_id,
                flight_crew_association.c.role
            ).join(CrewMember, flight_crew_association.c.crew_id == CrewMember.id)\
             .filter(flight_crew_association.c.flight_id.in_(f_ids))
            
            all_assoc = crew_stmt.all()
            session.close()

            # Map crew to flights for filtering and display
            from collections import defaultdict
            flight_to_crew = defaultdict(list)
            flight_to_ca = {}
            flight_to_fo = {}
            
            for f_id, c_name, c_emp_id, c_role in all_assoc:
                flight_to_crew[f_id].append(str(c_emp_id))
                role = (c_role or "").upper()
                if "CAPTAIN" in role or "CA" == role:
                    flight_to_ca[f_id] = c_name
                elif "FIRST OFFICER" in role or "FO" in role:
                    flight_to_fo[f_id] = c_name

            # --- Apply Filters to DataFrame ---
            mask = pd.Series([True] * len(df_flights))
            
            if filter_person:
                target_ids = [str(p["id"]) for p in filter_person]
                mask &= df_flights['id'].apply(lambda x: any(tid in flight_to_crew[x] for tid in target_ids))
            
            if filter_dep:
                mask &= df_flights['departure_airport'].isin(filter_dep)
                
            if filter_arr:
                mask &= df_flights['arrival_airport'].isin(filter_arr)
                
            filtered_df = df_flights[mask].copy()

            if filtered_df.empty:
                st.warning("No flights match the selected filters.")
                # We still want to show the sorting UI if they want to change it? 
                # Actually, if empty, just stop here.
                return

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
            
            ca_list = [flight_to_ca.get(f_id, "N/A") for f_id in filtered_df['id']]
            fo_list = [flight_to_fo.get(f_id, "N/A") for f_id in filtered_df['id']]
            
            display_df = filtered_df[['flight_number', 'scheduled_departure', 'departure_airport', 'arrival_airport', 'tail_number', 'status', 'id']].copy()
            display_df['CA'] = ca_list
            display_df['FO'] = fo_list
            display_df['flight_num_clean'] = display_df['flight_number'].apply(clean_fn).astype(int, errors='ignore')
            
            # Sort the data
            is_asc_hist = (hist_sort_order == "Ascending")
            display_df = display_df.sort_values(by=sort_map[hist_sort_col], ascending=is_asc_hist)
            
            # Format display columns
            display_df['formatted_departure'] = pd.to_datetime(display_df['scheduled_departure']).dt.strftime('%H:%M')
            
            # Create the link HTML
            display_df['Flight #'] = display_df.apply(
                lambda r: f"<a href='/historical?date={view_dt.strftime('%Y-%m-%d')}&flight_num={clean_fn(r['flight_number'])}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{clean_fn(r['flight_number'])}</a>", 
                axis=1
            )
            
            # Rename for final presentation
            render_df = display_df[['Flight #', 'formatted_departure', 'departure_airport', 'arrival_airport', 'tail_number', 'CA', 'FO', 'status']].rename(columns={
                'formatted_departure': 'Departure',
                'departure_airport': 'Dep',
                'arrival_airport': 'Arr',
                'tail_number': 'Tail', 
                'status': 'Status'
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
