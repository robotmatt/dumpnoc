
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database import get_session, IOEAssignment, ScheduledFlight, Flight, flight_crew_association
from bid_periods import get_bid_period_date_range, get_bid_period_from_date

def render_ioe_tab():
    st.header("IOE Audit Report")
    
    session = get_session()
    
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
        session = get_session()
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
        session = get_session()
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
