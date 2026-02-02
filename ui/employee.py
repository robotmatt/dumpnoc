
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from database import get_session, Flight, CrewMember, IOEAssignment, ScheduledFlight, flight_crew_association
from sqlalchemy import desc, or_

def render_employee_tab():
    st.header("👤 Employee History Search")
    
    # --- Search Interface ---
    col1, col2 = st.columns([1, 2])
    with col1:
        search_term = st.text_input("Search by Name or Employee ID", placeholder="e.g. 12345 or Smith")
    
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
        # Use simple heuristic: if it looks like an ID (numeric-ish), check assignments
        # Only do this if we didn't find a name match
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
    
    # --- Tabs ---
    tab_flown, tab_assigned = st.tabs(["✅ Flights Flown", "📅 Assigned History"])
    
    # ==========================
    # TAB 1: FLIGHTS FLOWN
    # ==========================
    # ==========================
    # TAB 1: FLIGHTS FLOWN
    # ==========================
    with tab_flown:
        if not crew_id_pk:
             st.info("No flown flights recorded (Employee not found in Crew Database).")
        else:
             # We need to query the association table directly to get role/flags, but specific to this crew member
             stmt = session.query(Flight, flight_crew_association.c.role, flight_crew_association.c.flags)\
                .join(flight_crew_association, Flight.id == flight_crew_association.c.flight_id)\
                .filter(flight_crew_association.c.crew_id == crew_id_pk)\
                .order_by(desc(Flight.date))
            
             flown_results = stmt.all()
        
             if not flown_results:
                st.info("No flown flights found for this employee.")
             else:
                flown_data = []
                for flight, role, flags in flown_results:
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
                k1.metric("Total Legs Flown", len(df_flown))
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
            st.error("Cannot search assignments: No Employee ID associated with this profile.")
        else:
            # Query IOE Assignments
            assignments = session.query(IOEAssignment).filter(
                IOEAssignment.employee_id == employee_id
            ).order_by(desc(IOEAssignment.start_date)).all()
            
            if not assignments:
                st.info("No IOE assignments found for this ID.")
            else:
                st.write(f"Found {len(assignments)} assignment blocks.")
                
                assign_data = []
                
                for assign in assignments:
                    # Get details of the pairing
                    pairing_num = assign.pairing_number
                    start_dt = assign.start_date
                    
                    # Link to pairings tab
                    month_str = start_dt.strftime("%B %Y")
                    p_link = f"<a href='/pairings?pairing={pairing_num}&month={month_str}' target='_self' style='text-decoration:none; font-weight:bold; color:#E694FF;'>{pairing_num}</a>"
                    
                    # Get legs in this assignment
                    # Use a broader window to catch legs if exact start date match fails, although schema suggests pairing_start_date matches
                    legs = session.query(ScheduledFlight).filter(
                        ScheduledFlight.pairing_number == pairing_num,
                        ScheduledFlight.pairing_start_date == start_dt
                    ).order_by(ScheduledFlight.date).all()
                    
                    # If empty, try a heuristic
                    if not legs:
                         legs = session.query(ScheduledFlight).filter(
                            ScheduledFlight.pairing_number == pairing_num,
                            ScheduledFlight.date >= start_dt,
                            ScheduledFlight.date <= start_dt + timedelta(days=5)
                        ).order_by(ScheduledFlight.date).all()

                    first_leg = legs[0].date.strftime('%Y-%m-%d') if legs else "Unknown"
                    last_leg = legs[-1].date.strftime('%Y-%m-%d') if legs else "Unknown"
                    route = f"{legs[0].departure_airport}-{legs[-1].arrival_airport}" if legs else "Unknown"
                    
                    # Determine status (upcoming, past, current)
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
                    
                    # Create a mini-table for the legs of this assignment?
                    # Maybe too much detail. User asked for "history of flights they were assigned, but might not be on anymore"
                    # Robust linking: "all the flght numbers link back"
                
                df_assign = pd.DataFrame(assign_data)
                st.markdown(df_assign.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
                
                # Detailed Breakdown
                st.divider()
                st.subheader("Detailed Schedule from Assignments")
                
                # We want a flat list of all assigned legs
                all_legs_data = []
                for assign in assignments:
                    pairing_num = assign.pairing_number
                    start_dt = assign.start_date
                    
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
                        
                    for leg in legs:
                        # Link
                        l_link = f"<a href='/?date={leg.date.strftime('%Y-%m-%d')}&flight_num={leg.flight_number}' target='_self' style='text-decoration:none; color: #BBB;'>{leg.flight_number}</a>"
                        
                        all_legs_data.append({
                            "Date": leg.date.strftime('%Y-%m-%d'),
                            "Pairing": pairing_num,
                            "Flight": l_link,
                            "Dep": leg.departure_airport,
                            "Arr": leg.arrival_airport,
                            "Dep Time": leg.scheduled_departure
                        })
                
                if all_legs_data:
                    df_all_legs = pd.DataFrame(all_legs_data)
                    st.markdown(df_all_legs.to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
                
    session.close()
