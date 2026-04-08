import streamlit as st
import pandas as pd
import json
import calendar
from datetime import datetime, date, timedelta
from database import get_session, Flight, CrewMember, flight_crew_association, IOEAssignment, ScheduledFlight, FlightHistory
from sqlalchemy import extract, and_, or_, desc
from fpdf import FPDF
import io

@st.cache_data(ttl=3600)
def get_all_crew_cached():
    session = get_session()
    # Pull only necessary columns for performance
    crew = session.query(CrewMember.name, CrewMember.employee_id).order_by(CrewMember.name).all()
    session.close()
    return [{"name": c.name, "id": c.employee_id, "label": f"{c.name} ({c.employee_id})"} for c in crew]

def generate_roster_pdf(crew_name, employee_id, month_name, year, days_map, num_days, fmt_block):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", 'B', 16)
    
    # Header
    pdf.cell(0, 10, f"Flight Roster: {crew_name} ({employee_id})", ln=True, align='C')
    pdf.set_font("helvetica", '', 12)
    pdf.cell(0, 10, f"Period: {month_name} {year}", ln=True, align='C')
    pdf.ln(5)
    
    # Table Header
    pdf.set_fill_color(200, 220, 255)
    pdf.set_font("helvetica", 'B', 8)
    # Adjusted widths to fit 190mm (A4 is 210mm - 20mm margins)
    cols = [("Day", 18), ("Flight", 18), ("Route", 28), ("Out", 18), ("In", 18), ("Schd Blk", 25), ("Act Blk", 25), ("Tail", 30)]
    for col_name, width in cols:
        pdf.cell(width, 10, col_name, 1, 0, 'C', True)
    pdf.ln()
    
    # Data Rows
    pdf.set_font("helvetica", '', 8)
    total_act = 0
    total_sch = 0
    
    fill = False
    for day in range(1, num_days + 1):
        # Calculate weekday
        day_date = date(year, list(calendar.month_name).index(month_name), day)
        weekday = day_date.strftime("%a")
        day_active = days_map.get(day, [])
        
        if not day_active:
            # Empty day row
            pdf.set_fill_color(245, 245, 245) if fill else pdf.set_fill_color(255, 255, 255)
            pdf.set_text_color(150, 150, 150)
            pdf.cell(18, 8, f"{weekday} {day}", 1, 0, 'C', True)
            pdf.cell(172, 8, "No active flights", 1, 1, 'L', True)
            pdf.set_text_color(0, 0, 0)
            fill = not fill
            continue
            
        def clean_apt(apt_str):
            if not apt_str: return "??"
            return str(apt_str).split(' - ')[0]

        for i, f in enumerate(day_active):
            pdf.set_fill_color(245, 245, 245) if fill else pdf.set_fill_color(255, 255, 255)
            
            day_str = f"{weekday} {day}" if i == 0 else ""
            f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
            
            pdf.cell(18, 8, day_str, 1, 0, 'C', True)
            pdf.cell(18, 8, f_num, 1, 0, 'C', True)
            route_str = f"{clean_apt(f.departure_airport)}-{clean_apt(f.arrival_airport)}"
            pdf.cell(28, 8, route_str, 1, 0, 'C', True)
            pdf.cell(18, 8, f.actual_out.strftime("%H:%M") if f.actual_out else "--", 1, 0, 'C', True)
            pdf.cell(18, 8, f.actual_in.strftime("%H:%M") if f.actual_in else "--", 1, 0, 'C', True)
            pdf.cell(25, 8, fmt_block(f.planned_block_minutes), 1, 0, 'C', True)
            pdf.cell(25, 8, fmt_block(f.actual_block_minutes), 1, 0, 'C', True)
            pdf.cell(30, 8, f.tail_number or "--", 1, 1, 'C', True)
            
            total_act += (f.actual_block_minutes or 0)
            total_sch += (f.planned_block_minutes or 0)
        
        fill = not fill

    # Totals Section
    pdf.ln(5)
    pdf.set_font("helvetica", 'B', 8)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(118, 5, "", 0, 0)
    pdf.cell(25, 5, "Total Sch", 0, 0, 'C')
    pdf.cell(25, 5, "Total Act", 0, 1, 'C')
    
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("helvetica", 'B', 10)
    pdf.cell(118, 10, "MONTHLY SUMMARY", 0, 0, 'R')
    pdf.cell(25, 10, fmt_block(total_sch), 1, 0, 'C')
    pdf.cell(25, 10, fmt_block(total_act), 1, 1, 'C')
    
    return bytes(pdf.output())

def render_roster_tab():
    def fmt_block(mins):
        if mins is None: return "0:00"
        h = abs(mins) // 60
        m = abs(mins) % 60
        return f"{h}:{m:02d}"
    query_params = st.query_params
    
    # Resolve initial date defaults
    d_year = int(st.session_state.get("roster_year_default", query_params.get("year", datetime.now().year)))
    d_month = int(st.session_state.get("roster_month_default", query_params.get("month", datetime.now().month)))
    
    # 2. Optimized Employee Selector Data
    crew_list = get_all_crew_cached()
    
    # Deep-linking: Initialize selector state from URL if not already set or if URL changed externally
    url_hrid = query_params.get("hrId", "")
    if "roster_crew_selector" not in st.session_state or (url_hrid and st.session_state.get("last_synced_hrid") != url_hrid):
        default_index = 0
        if url_hrid:
            for i, c in enumerate(crew_list):
                if str(c["id"]) == str(url_hrid):
                    default_index = i
                    break
        st.session_state["roster_crew_selector"] = crew_list[default_index] if crew_list else None
        st.session_state["last_synced_hrid"] = url_hrid

    # 3. UI Layout - Search Header
    col_s1, col_s2, col_s3 = st.columns([2, 1, 1])
    
    with col_s1:
        selected_crew_data = st.selectbox(
            "Select Employee", 
            crew_list, 
            key="roster_crew_selector",
            format_func=lambda x: x["label"]
        )
    
    with col_s2:
        curr_year = datetime.now().year
        years = [curr_year - 1, curr_year, curr_year + 1]
        selected_year = st.selectbox("Year", years, index=years.index(d_year) if d_year in years else 1)
    with col_s3:
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        selected_month_name = st.selectbox("Month", months, index=d_month - 1)
        selected_month = months.index(selected_month_name) + 1

    if not selected_crew_data:
        st.info("Please select an employee to view their roster and history.")
        return

    session = get_session()
    hrId = selected_crew_data["id"]
    
    # Sync URL parameters (one-way from state to URL)
    if query_params.get("hrId") != hrId or \
       query_params.get("year") != str(selected_year) or \
       query_params.get("month") != str(selected_month):
        st.query_params.update(year=selected_year, month=selected_month, hrId=hrId)
        st.session_state["last_synced_hrid"] = hrId

    # Fetch actual CrewMember object
    selected_crew = session.query(CrewMember).filter(CrewMember.employee_id == hrId).first()
    
    if not selected_crew:
        st.error(f"Could not load details for employee ID {hrId}")
        session.close()
        return

    # --- Data Preparation for Schedule & Export ---
    # Fetch flights CURRENTLY associated with this member in this month
    current_flights = session.query(Flight).join(flight_crew_association).filter(
        and_(
            flight_crew_association.c.crew_id == selected_crew.id,
            extract('year', Flight.date) == selected_year,
            extract('month', Flight.date) == selected_month
        )
    ).all()

    active_flights = [f for f in current_flights if f.status != "Canceled"]
    
    # Group Active by day for both UI and PDF
    from collections import defaultdict
    days_map = defaultdict(list)
    for f in active_flights:
        days_map[f.date.day].append(f)

    _, num_days = calendar.monthrange(selected_year, selected_month)

    # --- Header with Export Button ---
    h_col1, h_col2 = st.columns([2.5, 1])
    h_col1.subheader(f"Schedule & History: {selected_crew.name} ({hrId})")
    with h_col2:
        pdf_bytes = generate_roster_pdf(selected_crew.name, hrId, selected_month_name, selected_year, days_map, num_days, fmt_block)
        st.download_button(
            label="📄 Export Roster to PDF",
            data=pdf_bytes,
            file_name=f"Roster_{hrId}_{selected_month_name}_{selected_year}.pdf",
            mime="application/pdf",
            use_container_width=True
        )

    tab_schedule, tab_audit = st.tabs(["📅 Monthly Schedule", "🚫 Removal Audit"])

    # ==========================================
    # TAB 1: MONTHLY SCHEDULE (ACTIVE & RECENT)
    # ==========================================
    with tab_schedule:
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

        # (Logic moved to header for export support)

        st.divider()
        for day in range(1, num_days + 1):
            day_date = date(selected_year, selected_month, day)
            day_active = days_map.get(day, [])
            weekday = day_date.strftime("%a")
            total_active_block = sum((f.actual_block_minutes or 0) for f in day_active)
            total_scheduled_block = sum((f.planned_block_minutes or 0) for f in day_active)
            
            label = f"{weekday} {day}"
            if day_active:
                 label_styled = f"**{label}** — ✈️ {len(day_active)} legs — ⏱️ Actual: {fmt_block(total_active_block)} | Scheduled: {fmt_block(total_scheduled_block)}"
                 with st.expander(label_styled, expanded=False):
                     flight_rows = []
                     for f in day_active:
                         f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
                         f_link = f"<a href='/historical?date={f.date.strftime('%Y-%m-%d')}&flight_num={f_num}' target='_self' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{f_num}</a>"
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
