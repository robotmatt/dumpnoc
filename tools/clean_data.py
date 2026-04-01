import sys
import os
from datetime import datetime
# Ensure we can import from parent dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_session, Flight, DailySyncStatus, flight_crew_association

def clean_date_data(date_str):
    """
    Deletes all flights for a specific date (YYYY-MM-DD) and resets sync status
    for that date so it can be re-scraped correctly.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    except Exception as e:
        print(f"Error: Invalid date format. Use YYYY-MM-DD. ({e})")
        return

    session = get_session()
    
    # 1. Find flights in range
    # SQLAlchemy Datetime columns usually have .000000 in SQLite
    target_dt_full = dt.strftime("%Y-%m-%d %H:%M:%S.000000")
    target_dt_short = dt.strftime("%Y-%m-%d %H:%M:%S")

    from sqlalchemy import or_
    flights = session.query(Flight).filter(or_(Flight.date == target_dt_full, Flight.date == target_dt_short, Flight.date == dt)).all()
    if not flights:
        print(f"No flights found for {date_str} in database.")
    else:
        f_ids = [f.id for f in flights]
        print(f"Found {len(flights)} flights for {date_str}. Cleaning up...")
        
        # Delete associations
        session.execute(flight_crew_association.delete().where(flight_crew_association.c.flight_id.in_(f_ids)))
        
        # Delete flights
        for f in flights:
            session.delete(f)
        
        print(f"Successfully deleted {len(flights)} flights and their crew associations.")
    
    # 2. Reset Sync Status (Check all possible formats)
    status = session.query(DailySyncStatus).filter(or_(DailySyncStatus.date == target_dt_full, DailySyncStatus.date == target_dt_short, DailySyncStatus.date == dt)).first()
    if status:
        session.delete(status)
        print(f"Reset Sync Status for {date_str}.")
    
    session.commit()
    session.close()
    print(f"Cleanup complete for {date_str}. You can now re-scrape this day.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/clean_data.py YYYY-MM-DD [YYYY-MM-DD...]")
    else:
        for ds in sys.argv[1:]:
            clean_date_data(ds)
