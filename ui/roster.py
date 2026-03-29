import streamlit as st
import pandas as pd
import json
import calendar
from datetime import datetime, date, timedelta
from database import get_session, Flight, CrewMember, flight_crew_association, IOEAssignment, ScheduledFlight, FlightHistory
from sqlalchemy import extract, and_, or_, desc

def render_roster_tab():
    # 1. State / URL Params
    query_params = st.query_params
    
    # Resolve initial defaults
    d_year = int(st.session_state.get("roster_year_default", query_params.get("year", datetime.now().year)))
    d_month = int(st.session_state.get("roster_month_default", query_params.get("month", datetime.now().month)))
    d_hrid = st.session_state.get("roster_hrid_default", query_params.get("hrId", ""))

    # 2. UI Layout - Search Header
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
        st.info("Please search for an employee to view their roster and history.")
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
    st.subheader(f"Schedule & History: {selected_crew.name} ({hrId})")

    # Sync URL parameters for deep-linking
    st.query_params.update(year=selected_year, month=selected_month, hrId=hrId)

    # --- MAIN TABS ---
    tab_schedule, tab_audit = st.tabs(["📅 Monthly Schedule", "🚫 Removal Audit"])

    def fmt_block(mins):
        if mins is None: return "0:00"
        h = abs(mins) // 60
        m = abs(mins) % 60
        return f"{h}:{m:02d}"

    # ==========================================
    # TAB 1: MONTHLY SCHEDULE (ACTIVE & RECENT)
    # ==========================================
    with tab_schedule:
        # Fetch flights CURRENTLY associated with this member in this month
        current_flights = session.query(Flight).join(flight_crew_association).filter(
            and_(
                flight_crew_association.c.crew_id == selected_crew.id,
                extract('year', Flight.date) == selected_year,
                extract('month', Flight.date) == selected_month
            )
        ).all()

        active_flights = [f for f in current_flights if f.status != "Canceled"]
        canceled_flights = [f for f in current_flights if f.status == "Canceled"]

        # Fetch All Month History for Removal Scanning
        history_query = session.query(FlightHistory, Flight).join(Flight).filter(
            and_(
                extract('year', Flight.date) == selected_year,
                extract('month', Flight.date) == selected_month
            )
        ).all()

        removed_flights = []
        processed_ids = set()
        for hist, f in history_query:
            if f.id in [af.id for af in active_flights]: continue
            try:
                changes = json.loads(hist.changes_json)
                if "Crew" in changes:
                    old_c = changes["Crew"].get("old", [])
                    new_c = changes["Crew"].get("new", [])
                    was_in = any(str(c.get("id")) == str(hrId) for c in old_c)
                    is_in = any(str(c.get("id")) == str(hrId) for c in new_c)
                    if was_in and not is_in:
                        if f.id not in processed_ids:
                            removed_flights.append(f)
                            processed_ids.add(f.id)
            except: continue

        # Group Active by day
        from collections import defaultdict
        days_map = defaultdict(list)
        for f in active_flights:
            days_map[f.date.day].append(f)

        st.divider()
        _, num_days = calendar.monthrange(selected_year, selected_month)
        for day in range(1, num_days + 1):
            day_date = date(selected_year, selected_month, day)
            day_active = days_map.get(day, [])
            weekday = day_date.strftime("%a")
            total_active_block = sum((f.actual_block_minutes or 0) for f in day_active)
            
            label = f"{weekday} {day}"
            if day_active:
                 label_styled = f"**{label}** — ✈️ {len(day_active)} legs — ⏱️ Block: {fmt_block(total_active_block)}"
                 with st.expander(label_styled, expanded=False):
                     flight_rows = []
                     for f in day_active:
                         f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
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
                     st.markdown(pd.DataFrame(flight_rows).to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
            else:
                st.markdown(f"<span style='color: #666;'>{label} — No active flights</span>", unsafe_allow_html=True)

        if canceled_flights or removed_flights:
            st.divider()
            st.subheader("📋 Cancelled or Removed Leg Summary")
            h_rows = []
            for f in canceled_flights:
                h_rows.append({"Date": f.date.strftime("%d%b%y").upper(), "Flight": f.flight_number, "Route": f"{f.departure_airport}-{f.arrival_airport}", "Type": "🔴 CANC", "Schd Blk": fmt_block(f.planned_block_minutes)})
            for f in removed_flights:
                h_rows.append({"Date": f.date.strftime("%d%b%y").upper(), "Flight": f.flight_number, "Route": f"{f.departure_airport}-{f.arrival_airport}", "Type": "🟡 REMV", "Schd Blk": fmt_block(f.planned_block_minutes)})
            st.table(pd.DataFrame(h_rows))

    # ==========================================
    # TAB 2: REMOVAL AUDIT (CHRONOLOGICAL)
    # ==========================================
    with tab_audit:
        # Detailed audit trail of all crew changes involving this person
        audit_query = session.query(FlightHistory, Flight.flight_number, Flight.date, Flight.departure_airport, Flight.arrival_airport)\
            .join(Flight, Flight.id == FlightHistory.flight_id)\
            .filter(FlightHistory.changes_json.like(f"%{hrId}%"))\
            .order_by(desc(FlightHistory.timestamp)).all()
        
        audit_rows = []
        for h, f_num, f_date, f_dep, f_arr in audit_query:
            try:
                ch = json.loads(h.changes_json)
                if "Crew" in ch:
                    c_old = ch["Crew"].get("old", [])
                    c_new = ch["Crew"].get("new", [])
                    in_old = any(str(c.get("id")) == str(hrId) for c in c_old)
                    in_new = any(str(c.get("id")) == str(hrId) for c in c_new)
                    if in_old or in_new:
                        event = "Modified"
                        if in_old and not in_new: event = "🚫 REMOVED"
                        elif not in_old and in_new: event = "🟢 ADDED"
                        audit_rows.append({
                            "Detected At": h.timestamp.strftime('%Y-%m-%d %H:%M'),
                            "Flight Date": f_date.strftime('%Y-%m-%d'),
                            "Flight": f_num,
                            "Route": f"{f_dep}-{f_arr}",
                            "Event": event,
                            "Summary": h.description
                        })
            except: continue
        
        if not audit_rows:
            st.info("No detailed crew change events detected in flight history.")
        else:
            st.dataframe(pd.DataFrame(audit_rows), hide_index=True, width="stretch")

    session.close()
