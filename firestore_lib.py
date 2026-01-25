import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
from config import FIRESTORE_CREDENTIALS, ENABLE_CLOUD_SYNC
from datetime import datetime

_db = None
_enabled_override = None

def set_cloud_sync_enabled(enabled):
    global _enabled_override
    _enabled_override = enabled

def is_cloud_sync_enabled():
    if _enabled_override is not None:
        return _enabled_override
    return ENABLE_CLOUD_SYNC

def init_firestore():
    global _db
    if not is_cloud_sync_enabled():
        print("Cloud sync disabled.")
        return None

    if _db:
        return _db

    try:
        if os.path.exists(FIRESTORE_CREDENTIALS):
            cred = credentials.Certificate(FIRESTORE_CREDENTIALS)
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app(cred)
            _db = firestore.client()
            print("Firestore initialized successfully.")
            return _db
        else:
            print(f"Firestore credentials file not found at: {FIRESTORE_CREDENTIALS}")
            return None
    except Exception as e:
        print(f"Failed to initialize Firestore: {e}")
        return None

def get_db():
    if not _db:
        return init_firestore()
    return _db

def upload_daily_flights(date_str: str, flights_map: dict):
    """
    date_str: YYYY-MM-DD
    flights_map: { flight_id: { ...details... } }
    
    Restructured to use subcollections to avoid the 1MB per document limit in Firestore.
    """
    db = get_db()
    if not db: return
    try:
        # Root document for the date
        doc_ref = db.collection('daily_flights').document(date_str)
        doc_ref.set({"last_updated": firestore.SERVER_TIMESTAMP}, merge=True)
        
        # Upload each flight to a subcollection
        for f_id, f_data in flights_map.items():
            # Sanitize document ID: Firestore IDs cannot contain '/'
            safe_f_id = str(f_id).replace("/", "-")
            f_doc_ref = doc_ref.collection('flights').document(safe_f_id)
            f_doc_ref.set(f_data, merge=True)
            
        print(f"Updated daily flights for {date_str} (in sub-collection 'flights')")
    except Exception as e:
        print(f"Error updating daily flights for {date_str}: {e}")

def upload_pairing_bundle(doc_id: str, pairing_data: dict):
    """
    doc_id: PairingNumber_YYYYMMDD
    pairing_data: { "pairing_number": ..., "legs": { "1": {...}, "2": {...} } }
    """
    db = get_db()
    if not db: return
    try:
        doc_ref = db.collection('pairings').document(doc_id)
        doc_ref.set(pairing_data, merge=True)
        print(f"Uploaded pairing bundle {doc_id}")
    except Exception as e:
        print(f"Error uploading pairing bundle {doc_id}: {e}")

def upload_flight(flight_data: dict, flight_id: str):
    # Backward compatibility - redirects to daily grouping
    # flight_id is usually YYYYMMDD_FlightNum
    # parse date
    try:
        if "_" in flight_id:
            d_str, f_num = flight_id.split("_")
            formatted_date = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
            upload_daily_flights(formatted_date, {f_num: flight_data})
    except:
        pass

def upload_ioe_assignment(ioe_data: dict, doc_id: str):
    db = get_db()
    if not db: return
    try:
        doc_ref = db.collection('ioe_assignments').document(str(doc_id))
        doc_ref.set(ioe_data, merge=True)
    except Exception as e:
        print(f"Error uploading IOE {doc_id}: {e}")

def upload_scheduled_flight(sched_data: dict, doc_id: str):
    # Deprecated - use upload_pairing_bundle
    pass

def get_cloud_count(collection_name):
    db = get_db()
    if not db: return 0
    # Note: count() aggregation query is cheaper/faster than fetching all
    try:
        coll = db.collection(collection_name)
        count_query = coll.count()
        return count_query.get()[0][0].value
    except Exception as e:
        print(f"Error getting count for {collection_name}: {e}")
        return -1

def upload_metadata(key, value):
    db = get_db()
    if not db: return
    try:
        doc_ref = db.collection('metadata').document(str(key))
        doc_ref.set({"value": value}, merge=True)
    except Exception as e:
        print(f"Error uploading metadata {key}: {e}")

def get_cloud_metadata(key):
    """Retrieves a single metadata value from Firestore."""
    db = get_db()
    if not db: return None
    try:
        doc = db.collection('metadata').document(str(key)).get()
        if doc.exists:
            return doc.to_dict().get("value")
    except Exception as e:
        print(f"Error getting cloud metadata {key}: {e}")
    return None

# --- Download Functions for Two-Way Sync ---

def download_daily_flights():
    """Yields (doc_id, data_dict) for all daily flight bundles."""
    db = get_db()
    if not db: return
    try:
        docs = db.collection('daily_flights').stream()
        for doc in docs:
            # Check for data in subcollection first (New Format)
            flights_map = {}
            f_docs = doc.reference.collection('flights').stream()
            for fd in f_docs:
                flights_map[fd.id] = fd.to_dict()
            
            # Fallback to legacy field if subcollection is empty (Old Format)
            if not flights_map:
                legacy_data = doc.to_dict()
                if legacy_data and "flights" in legacy_data:
                    flights_map = legacy_data["flights"]
            
            if flights_map:
                yield doc.id, {"flights": flights_map}
                
    except Exception as e:
        print(f"Error downloading daily_flights: {e}")

def download_pairings():
    """Yields (doc_id, data_dict) for all pairing bundles."""
    db = get_db()
    if not db: return
    try:
        docs = db.collection('pairings').stream()
        for doc in docs:
            yield doc.id, doc.to_dict()
    except Exception as e:
        print(f"Error downloading pairings: {e}")

def download_ioe():
    """Yields (doc_id, data_dict) for all IOE assignments."""
    db = get_db()
    if not db: return
    try:
        docs = db.collection('ioe_assignments').stream()
        for doc in docs:
            yield doc.id, doc.to_dict()
    except Exception as e:
        print(f"Error downloading ioe_assignments: {e}")

def download_metadata():
    """Yields (key, value_dict) for all metadata."""
    db = get_db()
    if not db: return
    try:
        docs = db.collection('metadata').stream()
        for doc in docs:
            yield doc.id, doc.to_dict()
    except Exception as e:
        print(f"Error downloading metadata: {e}")
