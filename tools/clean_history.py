
from database import get_session, FlightHistory
from sqlalchemy import desc
from datetime import timedelta

session = get_session()

# Get all history records sorted by timestamp desc
histories = session.query(FlightHistory).order_by(desc(FlightHistory.timestamp)).all()

if not histories:
    print("No history records found.")
    session.close()
    exit()

sessions = []
current_session = []

# Cluster threshold: 5 minutes
THRESHOLD = timedelta(minutes=5)

if histories:
    current_session.append(histories[0])
    for i in range(1, len(histories)):
        if abs(histories[i-1].timestamp - histories[i].timestamp) <= THRESHOLD:
            current_session.append(histories[i])
        else:
            sessions.append(current_session)
            current_session = [histories[i]]
    sessions.append(current_session)

print(f"Found {len(sessions)} total sync sessions.")

# Take the last 11 sessions (most recent)
to_delete_sessions = sessions[:11]

ids_to_delete = []
for s in to_delete_sessions:
    for h in s:
        ids_to_delete.append(h.id)

if ids_to_delete:
    print(f"Preparing to delete {len(ids_to_delete)} records from the last {len(to_delete_sessions)} sessions.")
    # session.query(FlightHistory).filter(FlightHistory.id.in_(ids_to_delete)).delete(synchronize_session=False)
    # Actually, let's just do it.
    count = session.query(FlightHistory).filter(FlightHistory.id.in_(ids_to_delete)).delete(synchronize_session=False)
    session.commit()
    print(f"Successfully deleted {count} history records.")
else:
    print("No records to delete.")

session.close()
