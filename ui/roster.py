import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from database import get_session, Flight, CrewMember, flight_crew_association
from sqlalchemy import extract, and_, or_
import calendar

def render_roster_tab():
    # 1. State / URL Params
    query_params = st.query_params
    
    # Resolve initial defaults from URL or session state
    if "roster_year_default" in st.session_state:
        d_year = int(st.session_state["roster_year_default"])
    else:
        d_year = int(query_params.get("year", datetime.now().year))
        
    if "roster_month_default" in st.session_state:
        d_month = int(st.session_state["roster_month_default"])
    else:
        d_month = int(query_params.get("month", datetime.now().month))
        
    if "roster_hrid_default" in st.session_state:
        d_hrid = st.session_state["roster_hrid_default"]
    else:
        d_hrid = query_params.get("hrId", "")

    # 2. UI Layout
    col_s1, col_s2, col_s3 = st.columns([2, 1, 1])
    with col_s1:
        search_term = st.text_input("Search Employee (Name or ID)", value=d_hrid, placeholder="e.g. 9740 or Smith")
    with col_s2:
        curr_year = datetime.now().year
        years = [curr_year - 1, curr_year, curr_year + 1]
        selected_year = st.selectbox("Year", years, index=years.index(d_year) if d_year in years else 1)
    with col_s3:
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        selected_month_name = st.selectbox("Month", months, index=d_month - 1)
        selected_month = months.index(selected_month_name) + 1

    if not search_term:
        st.info("Please search for an employee to view their roster.")
        return

    session = get_session()
    
    # Search logic: Exact match on ID or partial match on Name
    crew_members = session.query(CrewMember).filter(
        or_(
            CrewMember.employee_id == search_term,
            CrewMember.name.ilike(f"%{search_term}%")
        )
    ).all()

    selected_crew = None
    if not crew_members:
        st.warning(f"No employee found matching '{search_term}'")
        session.close()
        return
    elif len(crew_members) > 1:
        st.info(f"Multiple matches found for '{search_term}'. Select one below:")
        choices = {f"{c.name} ({c.employee_id})": c for c in crew_members}
        sorted_labels = sorted(choices.keys())
        choice = st.radio("Select Member", sorted_labels, label_visibility="collapsed")
        selected_crew = choices[choice]
    else:
        selected_crew = crew_members[0]

    hrId = selected_crew.employee_id
    st.subheader(f"Schedule for: {selected_crew.name} ({hrId})")

    # Sync URL parameters for deep-linking
    st.query_params.update(year=selected_year, month=selected_month, hrId=hrId)

    # 3. Data Fetching
    # Fetch all flights CURRENTLY associated with this member
    current_flights = session.query(Flight).join(flight_crew_association).filter(
        and_(
            flight_crew_association.c.crew_id == selected_crew.id,
            extract('year', Flight.date) == selected_year,
            extract('month', Flight.date) == selected_month
        )
    ).all()

    # Identify Canceled vs Active
    active_flights = []
    canceled_flights = []
    for f in current_flights:
        if f.status == "Canceled":
            canceled_flights.append(f)
        else:
            active_flights.append(f)

    # 4. Fetch Removal History
    # Scan FlightHistory for removals of this employee_id in the target month
    from database import FlightHistory
    import json
    
    # Fetch all history for any flight in that month (broad search)
    # Join with Flight to get the date
    history_query = session.query(FlightHistory, Flight).join(Flight).filter(
        and_(
            extract('year', Flight.date) == selected_year,
            extract('month', Flight.date) == selected_month
        )
    ).all()

    removed_flights = []
    processed_history_flight_ids = set() # Avoid duplicates if multiple changes for same flight
    
    for hist, f in history_query:
        if f.id in [af.id for af in active_flights]: 
            continue # Don't show in "Removed" if they are currently on it
            
        try:
            changes = json.loads(hist.changes_json)
            if "Crew" in changes:
                old_list = changes["Crew"].get("old", [])
                new_list = changes["Crew"].get("new", [])
                
                # Was this employee in the OLD but not in the NEW?
                was_in_old = any(str(c.get("id")) == str(selected_crew.employee_id) for c in old_list)
                is_in_new = any(str(c.get("id")) == str(selected_crew.employee_id) for c in new_list)
                
                if was_in_old and not is_in_new:
                    if f.id not in processed_history_flight_ids:
                        removed_flights.append(f)
                        processed_history_flight_ids.add(f.id)
        except:
            continue

    # 5. Grouping by day (For Main View)
    from collections import defaultdict
    days_map = defaultdict(list)
    for f in active_flights:
        days_map[f.date.day].append(f)

    # 6. Formatting & Totals
    def fmt_block(mins):
        if mins is None: return "0:00"
        h = abs(mins) // 60
        m = abs(mins) % 60
        return f"{h}:{m:02d}"

    st.divider()
    
    # Monthly Navigation List
    _, num_days = calendar.monthrange(selected_year, selected_month)
    for day in range(1, num_days + 1):
        day_date = date(selected_year, selected_month, day)
        day_active = days_map.get(day, [])
        
        weekday = day_date.strftime("%a")
        total_block = sum((f.actual_block_minutes or 0) for f in day_active)
        
        label = f"{weekday} {day}"
        if day_active:
             label_styled = f"**{label}** — ✈️ {len(day_active)} legs — ⏱️ Block: {fmt_block(total_block)}"
             with st.expander(label_styled, expanded=False):
                 flight_rows = []
                 for f in day_active:
                     f_num = f.flight_number
                     if f_num.startswith("C5"): f_num = f_num[2:]
                     f_link = f"<a href='/?date={f.date.strftime('%Y-%m-%d')}&flight_num={f_num}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{f_num}</a>"
                     
                     flight_rows.append({
                         "Flight": f_link,
                         "Route": f"{f.departure_airport or '??'}-{f.arrival_airport or '??'}",
                         "Out (L)": f.actual_out.strftime("%H:%M") if f.actual_out else "--",
                         "In (L)": f.actual_in.strftime("%H:%M") if f.actual_in else "--",
                         "Schd Blk": fmt_block(f.planned_block_minutes),
                         "Actual Blk": fmt_block(f.actual_block_minutes),
                         "Tail": f.tail_number or "--"
                     })
                 
                 df_day = pd.DataFrame(flight_rows)
                 st.markdown(df_day.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
        else:
            st.markdown(f"<span style='color: #666;'>{label} — No active flights</span>", unsafe_allow_html=True)

    # 7. Cancelled or Removed Leg History
    if canceled_flights or removed_flights:
        st.write("")
        st.subheader("📋 Cancelled or Removed Legs")
        history_rows = []
        
        for f in canceled_flights:
            f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
            history_rows.append({
                "Date": f.date.strftime("%d%b%y").upper(),
                "Flight": f_num,
                "Route": f"{f.departure_airport or '??'}-{f.arrival_airport or '??'}",
                "Type": "🔴 CANCELLED",
                "Schd Blk": fmt_block(f.planned_block_minutes),
                "Actual Blk": "--"
            })
            
        for f in removed_flights:
             f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
             history_rows.append({
                "Date": f.date.strftime("%d%b%y").upper(),
                "Flight": f_num,
                "Route": f"{f.departure_airport or '??'}-{f.arrival_airport or '??'}",
                "Type": "🟡 REMOVED",
                "Schd Blk": fmt_block(f.planned_block_minutes),
                "Actual Blk": "--"
            })
            
        df_hist = pd.DataFrame(history_rows)
        st.table(df_hist)

    session.close()
