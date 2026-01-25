
import streamlit as st
import pandas as pd
from datetime import datetime
from database import get_session, ScheduledFlight, Flight
from bid_periods import get_bid_period_date_range, get_bid_period_from_date
from datetime import timedelta

def render_pairings_tab():
    st.header("Scheduled Pairings")
    
    session = get_session()
    
    # Filter Controls
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    with col1:
        # Get unique pairing numbers
        pairing_nums = [r[0] for r in session.query(ScheduledFlight.pairing_number).distinct()]
        
        # Determine Default
        p_idx = 0
        p_arg = st.session_state.get("pairing_search_default", "All")
        if p_arg in pairing_nums:
            p_idx = (["All"] + sorted(pairing_nums)).index(p_arg)
        
        sel_pairing = st.selectbox("Filter by Pairing", ["All"] + sorted(pairing_nums), index=p_idx, key="pairing_search")
    
    with col2:
        # Get distinct months from ScheduledFlight
        pairing_dates_q = session.query(ScheduledFlight.pairing_start_date).distinct().all()
        months_set = set()
        for d in pairing_dates_q:
            if d[0]:
                bp_year, bp_month = get_bid_period_from_date(d[0])
                dt_obj = datetime(bp_year, bp_month, 1)
                months_set.add(dt_obj.strftime("%B %Y"))
        
        curr_bp_year, curr_bp_month = get_bid_period_from_date(datetime.now().date())
        current_month_str = datetime(curr_bp_year, curr_bp_month, 1).strftime("%B %Y")
        months_set.add(current_month_str)
            
        months = sorted(list(months_set), key=lambda x: datetime.strptime(x, "%B %Y"), reverse=True)
        default_idx = months.index(current_month_str) if current_month_str in months else 0
        
        # Use deep link default if present
        m_arg = st.session_state.get("pairing_month_default")
        if m_arg in months:
            default_idx = months.index(m_arg)
            
        sel_month_str = st.selectbox("Filter by Bid Period", months, index=default_idx, key="month_search")

    with col3:
        d_arg = st.session_state.get("pairing_date_default")
        sel_date = st.date_input("Filter by Date", value=d_arg, key="date_search")
    
    with col4:
        st.write("") # Spacer
        if st.button("🔄 Reset"):
            # Clear search defaults
            for k in ["pairing_search_default", "pairing_month_default", "pairing_date_default"]:
                if k in st.session_state: del st.session_state[k]
            st.rerun()

    # Build Query
    query = session.query(ScheduledFlight)
    
    if sel_pairing != "All":
        query = query.filter(ScheduledFlight.pairing_number == sel_pairing)
    
    if sel_date:
        query = query.filter(ScheduledFlight.pairing_start_date == datetime.combine(sel_date, datetime.min.time()))
    elif sel_month_str:
        sel_month_dt = datetime.strptime(sel_month_str, "%B %Y")
        bp_start, bp_end = get_bid_period_date_range(sel_month_dt.year, sel_month_dt.month)
        
        start_dt = datetime.combine(bp_start, datetime.min.time())
        end_dt = datetime.combine(bp_end + timedelta(days=1), datetime.min.time())
        
        query = query.filter(
            ScheduledFlight.pairing_start_date >= start_dt,
            ScheduledFlight.pairing_start_date < end_dt
        )
        
    scheduled_rows = query.order_by(
        ScheduledFlight.pairing_start_date, 
        ScheduledFlight.date, 
        ScheduledFlight.scheduled_departure
    ).limit(1000).all()
    
    data = []
    for sf in scheduled_rows:
        candidates = [sf.flight_number, f"C5{sf.flight_number}", f"C{sf.flight_number}"]
        actual = session.query(Flight).filter(
            Flight.flight_number.in_(candidates), 
            Flight.date == sf.date
        ).first()
        
        crew_str = "N/A"
        status = "Scheduled"
        if actual:
            status = actual.status or "Flown"
            crews = [c.name for c in actual.crew_members]
            crew_str = "; ".join(crews)
        
        # Determine month string for pairing link
        p_month_str = ""
        if sf.pairing_start_date:
            bp_y, bp_m = get_bid_period_from_date(sf.pairing_start_date)
            p_month_str = datetime(bp_y, bp_m, 1).strftime("%B %Y")

        data.append({
            "Trip Start": sf.pairing_start_date.strftime("%Y-%m-%d") if sf.pairing_start_date else "N/A",
            "Leg Date": sf.date.strftime("%Y-%m-%d"),
            "Pairing": f"<a href='/pairings?pairing={sf.pairing_number}&month={p_month_str}' target='_self' style='text-decoration:none; font-weight:bold; color:#E694FF;'>{sf.pairing_number}</a>",
            "Flight": f"<a href='/?date={sf.date.strftime('%Y-%m-%d')}&flight_num={sf.flight_number}' target='_self' style='text-decoration:none; font-weight:bold;'>{sf.flight_number}</a>",
            "Route": f"{sf.departure_airport}-{sf.arrival_airport}",
            "Sch Dep": sf.scheduled_departure,
            "Sch Arr": sf.scheduled_arrival or "N/A",
            "Block": sf.block_time or "N/A",
            "Credit": sf.total_credit or "N/A",
            "Status": status,
            "Actual Crew": crew_str
        })
    
    if data:
        pairings_df = pd.DataFrame(data)
        html_pairings = pairings_df.to_html(index=False, classes='dataframe', escape=False)
        st.markdown(f"""
        <div style="max-height: 800px; overflow-y: auto; border: 1px solid #444; border-radius: 5px;">
            {html_pairings}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("No pairings found matching filters.")
    session.close()
