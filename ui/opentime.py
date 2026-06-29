import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from database import get_session, Flight, CrewMember, flight_crew_association
from sqlalchemy import and_, or_

def render_opentime_tab():
    st.subheader("✈️ Open Time Dashboard")
    st.write("View active flights missing specific crew positions.")

    # UI Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        default_start = datetime.now().date()
        start_date = st.date_input("Start Date", default_start)
    
    with col2:
        end_date = st.date_input("End Date", default_start + timedelta(days=7))
        
    with col3:
        position = st.selectbox(
            "Select Missing Position",
            options=["CA", "FO", "FA"],
            index=0,
            format_func=lambda x: {"CA": "Captain (CA)", "FO": "First Officer (FO)", "FA": "Flight Attendant (FA)"}[x]
        )
        
    with col4:
        base_filter = st.selectbox(
            "Base Filter",
            options=["All Bases", "IAD", "IAH"],
            index=0
        )
    
    if start_date > end_date:
        st.error("Start Date must be before or equal to End Date.")
        return
        
    session = get_session()
    
    query_filters = [
        Flight.date >= start_date,
        Flight.date < end_date + timedelta(days=1),
        Flight.status != "Canceled",
        Flight.status != "Flown" # Assuming flown flights are not open time, but let's just stick to != Canceled as some systems just use active
    ]
    
    if base_filter != "All Bases":
        query_filters.append(
            or_(
                Flight.departure_airport == base_filter,
                Flight.arrival_airport == base_filter
            )
        )
        
    flights = session.query(Flight).filter(and_(*query_filters)).order_by(Flight.date, Flight.scheduled_departure).all()
    
    open_flights = []
    
    def fmt_block(mins):
        if mins is None: return "--"
        h = abs(mins) // 60
        m = abs(mins) % 60
        return f"{h}:{m:02d}"
    
    for f in flights:
        if f.status == "Canceled":
            continue
            
        crew_roles = session.query(flight_crew_association.c.role).filter(
            flight_crew_association.c.flight_id == f.id
        ).all()
        roles = [r[0] for r in crew_roles]
        
        if position not in roles:
            f_num = f.flight_number[2:] if f.flight_number.startswith("C5") else f.flight_number
            dep_code = f.departure_airport.split(" - ")[0].strip() if f.departure_airport else ""
            f_link = f"<a href='/historical?date={f.date.strftime('%Y-%m-%d')}&flight_num={f_num}&dep={dep_code}' target='_blank' style='text-decoration:none; font-weight:bold; color:#60B4FF;'>{f_num}</a>"
            open_flights.append({
                "Date": f.date.strftime('%Y-%m-%d'),
                "Flight": f_link,
                "Dep": f.departure_airport or "--",
                "Arr": f.arrival_airport or "--",
                "Sch Out": f.scheduled_departure.strftime("%H:%M") if f.scheduled_departure else "--",
                "Sch In": f.scheduled_arrival.strftime("%H:%M") if f.scheduled_arrival else "--",
                "Schd Blk": fmt_block(f.planned_block_minutes),
                "Tail": f.tail_number or "--",
                "Missing": position
            })
            
    session.close()
    
    st.divider()
    
    if not open_flights:
        st.success(f"No open time found missing a {position} between {start_date} and {end_date}.")
    else:
        st.warning(f"Found {len(open_flights)} flights missing a {position}.")
        st.markdown(pd.DataFrame(open_flights).to_html(escape=False, index=False, classes='dataframe'), unsafe_allow_html=True)
