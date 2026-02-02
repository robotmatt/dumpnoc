
import streamlit as st
import pandas as pd
import json
from datetime import datetime, timedelta
from database import get_session, Flight, CrewMember, IOEAssignment, ScheduledFlight, flight_crew_association, FlightHistory
from sqlalchemy import desc, or_
from bid_periods import get_bid_period_from_date

def render_employee_tab():
    st.header("👤 Employee History Search")
    
    # --- Search Interface ---
    # Check for deep link or session persistence
    default_search = st.session_state.get("employee_search_id", "")
    default_month = st.session_state.get("employee_search_month", "All History")
    
    # If we have a default search, we might want to auto-run
    # But streamlit's text_input works best if we just set value
    
    col1, col2 = st.columns([1, 2])
    with col1:
        search_term = st.text_input("Search by Name or Employee ID", value=default_search, placeholder="e.g. 12345 or Smith")
    
    if not search_term:
        st.info("Please enter an employee ID or name to search.")
        return

    session = get_session()
    
    # Search logic: Exact match on ID or partial match on Name
    crew_member = session.query(CrewMember).filter(
        or_(
            CrewMember.employee_id == search_term,
            CrewMember.name.ilike(f"%{search_term}%")
        )
    ).first()
    
    employee_id = None
    employee_name = None
    crew_id_pk = None
    
    if crew_member:
        employee_id = crew_member.employee_id
        employee_name = crew_member.name
        crew_id_pk = crew_member.id
    else:
        # Fallback: Check if it's a raw ID in assignments
        assignment_check = session.query(IOEAssignment).filter(
            IOEAssignment.employee_id == search_term
        ).first()
        
        if assignment_check:
            employee_id = search_term
            employee_name = f"Unknown Name ({search_term})"
        else:
            st.warning(f"No employee or assignment history found matching '{search_term}'")
            session.close()
            return
        
    st.subheader(f"History for: {employee_name}")
    
    # --- Data Fetching (Pre-Filter) ---
    
    # 1. Fetch ALL Flown
    flown_all = []
    if crew_id_pk:
        stmt = session.query(Flight, flight_crew_association.c.role, flight_crew_association.c.flags)\
            .join(flight_crew_association, Flight.id == flight_crew_association.c.flight_id)\
            .filter(flight_crew_association.c.crew_id == crew_id_pk)\
            .order_by(desc(Flight.date))
        flown_all = stmt.all()
        
    # 2. Fetch ALL Assignments
    assignments_all = []
    if employee_id:
        assignments_all = session.query(IOEAssignment).filter(
            IOEAssignment.employee_id == employee_id
        ).order_by(desc(IOEAssignment.start_date)).all()
        
    # 3. Fetch ALL History (Removals/Changes)
    history_all = []
    # Search for any history record where the changes_json contains the employee_id
    if employee_id:
        # We search for the ID as a string in the JSON blob
        # This is a bit broad but we'll filter in Python
        hist_stmt = session.query(FlightHistory, Flight.flight_number, Flight.date, Flight.departure_airport, Flight.arrival_airport)\
            .join(Flight, Flight.id == FlightHistory.flight_id)\
            .filter(FlightHistory.changes_json.like(f"%{employee_id}%"))\
            .order_by(desc(FlightHistory.timestamp))
        
        raw_hist = hist_stmt.all()
        
        for h, f_num, f_date, f_dep, f_arr in raw_hist:
            try:
                changes = json.loads(h.changes_json)
                if "Crew" in changes:
                    c_old = changes["Crew"].get("old", [])
                    c_new = changes["Crew"].get("new", [])
                    
                    was_in_old = any(str(c.get("id")) == str(employee_id) for c in c_old)
                    was_in_new = any(str(c.get("id")) == str(employee_id) for c in c_new)
                    
                    if was_in_old or was_in_new:
                        event_type = "Modified"
                        if was_in_old and not was_in_new:
                            event_type = "Removed"
                        elif not was_in_old and was_in_new:
                            event_type = "Added"
                            
                        history_all.append({
                            "timestamp": h.timestamp,
                            "flight_id": h.flight_id,
                            "flight_number": f_num,
                            "date": f_date,
                            "route": f"{f_dep}-{f_arr}",
                            "event": event_type,
                            "description": h.description,
                            "detail": changes["Crew"]
                        })
            except:
                continue

    # --- Month Extraction ---
    months_set = set()
    
    # From Flown
    for f, _, _ in flown_all:
        if f.date:
            m_str = f.date.strftime("%B %Y")
            months_set.add(m_str)
            
    # From Assigned
    for a in assignments_all:
        if a.start_date:
            m_str = a.start_date.strftime("%B %Y")
            months_set.add(m_str)
    
    # From History
    for h in history_all:
        if h["date"]:
            m_str = h["date"].strftime("%B %Y")
            months_set.add(m_str)
            
    # Sort Months
    sorted_months = sorted(list(months_set), key=lambda x: datetime.strptime(x, "%B %Y"), reverse=True)
    
    # --- Filter UI ---
    filter_opts = ["All History"] + sorted_months
    
    # Resolve default index
    sel_idx = 0
    if default_month in filter_opts:
        sel_idx = filter_opts.index(default_month)

    # Show filter below header
    row_filt_1, row_filt_2 = st.columns([1, 3])
    with row_filt_1:
        selected_month = st.selectbox("📅 Filter by Month", filter_opts, index=sel_idx)

    # --- Filtering Logic ---
    flown_filtered = []
    if selected_month == "All History":
        flown_filtered = flown_all
    else:
        for item in flown_all:
            if item[0].date.strftime("%B %Y") == selected_month:
                flown_filtered.append(item)
                
    assignments_filtered = []
    if selected_month == "All History":
        assignments_filtered = assignments_all
    else:
        for a in assignments_all:
            if a.start_date.strftime("%B %Y") == selected_month:
                assignments_filtered.append(a)

    history_filtered = []
    if selected_month == "All History":
        history_filtered = history_all
    else:
        for h in history_all:
            if h["date"].strftime("%B %Y") == selected_month:
                history_filtered.append(h)

    # --- Tabs ---
    tab_flown, tab_assigned, tab_removals = st.tabs(["✅ Flights Flown", "📅 Assigned History", "🚫 Removals & Changes"])
    
    # ==========================
    # TAB 1: FLIGHTS FLOWN
    # ==========================
    with tab_flown:
        if not crew_id_pk:
             st.info("No flown flights recorded (Employee not found in Crew Database).")
        elif not flown_filtered:
            st.info(f"No flights flown in {selected_month}.")
        else:
            flown_data = []
            for flight, role, flags in flown_filtered:
                # Basic cleaning
                f_num_clean = flight.flight_number
                if f_num_clean.startswith('C5'): f_num_clean = f_num_clean[2:]
                elif f_num_clean.startswith('C'): f_num_clean = f_num_clean[1:]
                
                # HTML Link
                link = f"<a href='/?date={flight.date.strftime('%Y-%m-%d')}&flight_num={f_num_clean}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{f_num_clean}</a>"
                
                flown_data.append({
                    "Date": flight.date.strftime('%Y-%m-%d'),
                    "Flight": link,
                    "Dep": flight.departure_airport or "--",
                    "Arr": flight.arrival_airport or "--",
                    "Tail": flight.tail_number or "--",
                    "Role": role,
                    "Flags": flags or "",
                    "Status": flight.status or ""
                })
                
            df_flown = pd.DataFrame(flown_data)
            
            # KPI Cards
            k1, k2, k3 = st.columns(3)
            k1.metric("Legs Flown", len(df_flown))
            if not df_flown.empty:
                k2.metric("Most Recent", df_flown.iloc[0]['Date'])
                # Unique aircraft
                total_tails = df_flown['Tail'].nunique()
                k3.metric("Unique Aircraft", total_tails)
            
            # Render Table
            st.markdown(df_flown.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
            
    # ==========================
    # TAB 2: ASSIGNED HISTORY
    # ==========================
    with tab_assigned:
        if not employee_id:
            st.error("Cannot search assignments.")
        elif not assignments_filtered:
            st.info(f"No assignments found in {selected_month}.")
        else:
            assign_data = []
            all_legs_data = []
            
            for assign in assignments_filtered:
                # Get details of the pairing
                pairing_num = assign.pairing_number
                start_dt = assign.start_date
                
                # Link to pairings tab
                month_str = start_dt.strftime("%B %Y")
                p_link = f"<a href='/pairings?pairing={pairing_num}&month={month_str}' target='_self' style='text-decoration:none; font-weight:bold; color:#E694FF;'>{pairing_num}</a>"
                
                # Get legs
                legs = session.query(ScheduledFlight).filter(
                    ScheduledFlight.pairing_number == pairing_num,
                    ScheduledFlight.pairing_start_date == start_dt
                ).order_by(ScheduledFlight.date).all()
                
                if not legs:
                        legs = session.query(ScheduledFlight).filter(
                        ScheduledFlight.pairing_number == pairing_num,
                        ScheduledFlight.date >= start_dt,
                        ScheduledFlight.date <= start_dt + timedelta(days=5)
                    ).order_by(ScheduledFlight.date).all()

                first_leg = legs[0].date.strftime('%Y-%m-%d') if legs else "Unknown"
                last_leg = legs[-1].date.strftime('%Y-%m-%d') if legs else "Unknown"
                route = f"{legs[0].departure_airport}-{legs[-1].arrival_airport}" if legs else "Unknown"
                
                status_emoji = "🗓️"
                now = datetime.now()
                if legs:
                    if legs[-1].date < now:
                        status_emoji = "✅" # Past
                    elif legs[0].date > now:
                        status_emoji = "🔜" # Future
                    else:
                        status_emoji = "🔛" # Active
                
                assign_data.append({
                    "Start Date": start_dt.strftime('%Y-%m-%d'),
                    "Pairing": p_link,
                    "Range": f"{first_leg} to {last_leg}",
                    "Route": route,
                    "Status": status_emoji
                })
                
                # Collect legs for detailed view
                for leg in legs:
                    l_link = f"<a href='/?date={leg.date.strftime('%Y-%m-%d')}&flight_num={leg.flight_number}' target='_self' style='text-decoration:none; color: #BBB;'>{leg.flight_number}</a>"
                    all_legs_data.append({
                        "Date": leg.date.strftime('%Y-%m-%d'),
                        "Pairing": pairing_num,
                        "Flight": l_link,
                        "Dep": leg.departure_airport,
                        "Arr": leg.arrival_airport,
                        "Dep Time": leg.scheduled_departure
                    })
            
            # Summary Table
            df_assign = pd.DataFrame(assign_data)
            st.markdown(df_assign.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
            
            # Detailed Breakdown
            st.divider()
            st.subheader(f"Detailed Schedule ({len(all_legs_data)} legs)")
            
            if all_legs_data:
                df_all_legs = pd.DataFrame(all_legs_data)
                st.markdown(df_all_legs.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)

    # ==========================
    # TAB 3: REMOVALS & CHANGES
    # ==========================
    with tab_removals:
        if not employee_id:
            st.error("Cannot search history.")
        elif not history_filtered:
            st.info(f"No removals or crew changes detected for {selected_month}.")
        else:
            st.write(f"Showing {len(history_filtered)} change events.")
            
            hist_display_data = []
            for h in history_filtered:
                # Basic cleaning for link
                f_num = h["flight_number"]
                if f_num.startswith('C5'): f_num = f_num[2:]
                elif f_num.startswith('C'): f_num = f_num[1:]
                
                f_link = f"<a href='/?date={h['date'].strftime('%Y-%m-%d')}&flight_num={f_num}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{f_num}</a>"
                
                hist_display_data.append({
                    "Detected At": h["timestamp"].strftime('%Y-%m-%d %H:%M'),
                    "Flight Date": h["date"].strftime('%Y-%m-%d'),
                    "Flight": f_link,
                    "Route": h["route"],
                    "Event": h["event"],
                    "Description": h["description"]
                })
            
            df_hist = pd.DataFrame(hist_display_data)
            st.markdown(df_hist.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
            st.caption("Removals are detected when an employee appears in one scrape but is missing in the next.")
                
    session.close()
