"""
Clear all flight data from local database and Firestore.
Use this to start fresh with corrected local-time-based flight dates.

CAUTION: This will delete ALL scraped flight data!
"""
from database import get_session, Flight, DailySyncStatus
from firestore_lib import is_cloud_sync_enabled
import sys

def main():
    print("=" * 80)
    print("CLEAR ALL FLIGHT DATA")
    print("=" * 80)
    print("\nThis script will delete:")
    print("  ✗ All scraped flights from local database")
    print("  ✗ All daily sync status records")
    print("  ✗ All flights from Firestore (if cloud sync is enabled)")
    print("\nThis will NOT delete:")
    print("  ✓ Scheduled pairings")
    print("  ✓ IOE assignments")
    print("  ✓ Crew member records")
    print("=" * 80)
    
    # Check local database
    session = get_session()
    flight_count = session.query(Flight).count()
    sync_status_count = session.query(DailySyncStatus).count()
    
    print(f"\nLocal Database:")
    print(f"  - {flight_count} flights")
    print(f"  - {sync_status_count} sync status records")
    
    # Check Firestore
    cloud_enabled = is_cloud_sync_enabled()
    if cloud_enabled:
        print(f"\nFirestore: Cloud sync is ENABLED")
        print(f"  - Will also delete flights from Firestore")
    else:
        print(f"\nFirestore: Cloud sync is DISABLED")
        print(f"  - Firestore will NOT be touched")
    
    print("\n" + "=" * 80)
    confirm1 = input("\nAre you SURE you want to delete all flight data? (yes/no): ")
    
    if confirm1.lower() != 'yes':
        print("Canceled - no changes made.")
        session.close()
        return
    
    # Double confirmation
    confirm2 = input("Type 'DELETE ALL FLIGHTS' to confirm: ")
    
    if confirm2 != 'DELETE ALL FLIGHTS':
        print("Canceled - no changes made.")
        session.close()
        return
    
    print("\n" + "=" * 80)
    print("DELETING DATA...")
    print("=" * 80)
    
    # Delete from local database
    print("\n1. Clearing local database...")
    print(f"   Deleting {flight_count} flights...")
    session.query(Flight).delete()
    session.commit()
    print("   ✓ Flights deleted")
    
    print(f"   Deleting {sync_status_count} sync status records...")
    session.query(DailySyncStatus).delete()
    session.commit()
    print("   ✓ Sync status deleted")
    
    # Clear metadata
    from database import set_metadata
    set_metadata(session, "last_successful_sync", "")
    print("   ✓ Cleared last sync metadata")
    
    session.close()
    
    # Delete from Firestore
    if cloud_enabled:
        print("\n2. Clearing Firestore...")
        try:
            from firestore_lib import get_db
            db = get_db()
            
            if not db:
                print("   ⚠ Could not connect to Firestore")
            else:
                # Delete all daily_flights documents
                daily_flights_ref = db.collection('daily_flights')
                docs = list(daily_flights_ref.stream())
                
                if len(docs) == 0:
                    print("   ℹ No daily_flights documents in Firestore")
                else:
                    deleted_count = 0
                    total_docs = len(docs)
                    
                    print(f"   Deleting {total_docs} daily_flights documents...")
                    
                    # Delete individually to avoid transaction size limits
                    # (each daily_flights doc can be very large - contains all flights for a day)
                    for i, doc in enumerate(docs, 1):
                        try:
                            doc.reference.delete()
                            deleted_count += 1
                            
                            # Progress update every 10 documents
                            if deleted_count % 10 == 0 or deleted_count == total_docs:
                                print(f"   Progress: {deleted_count}/{total_docs} documents deleted")
                        except Exception as e:
                            print(f"   ⚠ Error deleting document {doc.id}: {e}")
                    
                    print(f"   ✓ Deleted {deleted_count} total daily_flights documents from Firestore")
                
                # Update metadata
                from firestore_lib import upload_metadata
                upload_metadata('last_successful_sync', '')
                print("   ✓ Cleared Firestore metadata")
            
        except Exception as e:
            print(f"   ⚠ Error clearing Firestore: {e}")
            import traceback
            traceback.print_exc()
            print("   You may need to manually clear Firestore data")
    
    print("\n" + "=" * 80)
    print("✓ ALL FLIGHT DATA CLEARED!")
    print("=" * 80)
    print("\nYou can now re-scrape your dates with the corrected local-time logic.")
    print("The flights will be stored with dates based on LOCAL departure time.")

if __name__ == "__main__":
    main()
