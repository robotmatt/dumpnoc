import re
import os
from datetime import datetime, timedelta
from database import get_session, ScheduledFlight, IOEAssignment, init_db
from bid_periods import get_bid_period_date_range

PAIRINGS_DIR = "pairings"
IOE_DIR = "ioe"

def ingest_all(session):
    # IOE Files
    if os.path.exists(IOE_DIR):
        for f in os.listdir(IOE_DIR):
            if f.endswith(".txt") and not f.startswith("._"):
                parse_ioe_file(os.path.join(IOE_DIR, f), session)
    
    # Pairings Files
    if os.path.exists(PAIRINGS_DIR):
        for f in os.listdir(PAIRINGS_DIR):
            if f.endswith(".txt") and not f.startswith("._"):
                parse_pairings_file(os.path.join(PAIRINGS_DIR, f), session)

def parse_ioe_file(filepath, session):
    print(f"Parsing IOE file: {filepath}")
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    
    count = 0
    current_emp = None
    current_pairing = None
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith("-") or line.startswith("IOE") or line.startswith("Period") or line.startswith("Category") or line.startswith("Employee") or "End of" in line:
            continue
            
        # Exception: Only skip "Pairing Number" if NO date is present on that line
        if "Pairing Number" in line and not re.search(r'\d{4}-\d{2}-\d{2}', line):
            continue
            
        parts = line.split()
        
        # Look for Date (YYYY-MM-DD)
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', line)
        
        if date_match:
            date_str = date_match.group(0)
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                
                # If we have parts before the date, use them
                # Otherwise use the state from previous lines
                if len(parts) >= 3 and parts[2] == date_str:
                    emp_id = parts[0]
                    pairing = parts[1]
                elif current_emp and current_pairing:
                    emp_id = current_emp
                    pairing = current_pairing
                else:
                    continue
                
                rec = IOEAssignment(employee_id=emp_id, pairing_number=pairing, start_date=dt)
                session.add(rec)
                count += 1
            except ValueError:
                pass
        else:
            # Maybe this line just has EmpID and Pairing
            if len(parts) >= 2 and parts[0].isdigit() and (parts[1].startswith('I') or parts[1].startswith('H')):
                current_emp = parts[0]
                current_pairing = parts[1]
                
    session.commit()
    print(f"Imported {count} IOE assignments.")

def parse_pairings_file(filepath, session):
    print(f"Parsing Pairings file: {filepath}")
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    # Split by pairing ID blocks (Ixxxx or Hxxxx)
    # The separator is usually dashes
    # But regex might be safer. Look for "I\d{4}  Check-In"
    
    # We can perform a line-by-line state machine approach which is safer for this complex layout
    lines = content.split('\n')
    
    current_pairing = None
    month_year = None
    legs = [] # List of dicts {day_offset, flt, dep, arr}
    start_dates = []
    
    # Grid parsing state
    in_grid = False
    current_total_credit = None
    
    import_count = 0
    
    for line in lines:
        # 1. Header Detection
        # I0001  Check-In ... Category IAH-XMJ-CA,F ... November 2025
        header_regex = r'^([IH]\d{4})\s+Check-In.*?Category\s+([^\s]+).*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})'
        header_match = re.search(header_regex, line)
        if header_match:
            # Save previous if exists
            if current_pairing and start_dates and legs:
                save_pairing(session, current_pairing, month_year, start_dates, legs, current_total_credit)
                import_count += 1
            
            # Reset
            current_pairing = header_match.group(1)
            category = header_match.group(2)
            month_str = header_match.group(3)
            year_str = header_match.group(4)
            month_year = datetime.strptime(f"{month_str} {year_str}", "%B %Y")
            legs = []
            start_dates = []
            current_total_credit = None

            # User Filter: Only CA and F pairings. Ignore FA.
            # Example Good: IAH-XMJ-CA,F, IAH-XMJ-CA, IAH-XMJ-F
            # Example Bad: IAH-ALL-FA
            if "FA" in category and "CA" not in category and "CA,F" not in category:
                # This is likely a Flight Attendant pairing
                current_pairing = None # Skip this block
                continue

            continue

        if not current_pairing: continue
        
        # 2. Grid Parsing (Right side of the line)
        # The grid is consistently at specific columns? NO, depends on layout.
        # But it is framed by | ... |
        # Look for | ... |
        grid_match = re.search(r'\|(.*?)\|', line)
        if grid_match:
            grid_content = grid_match.group(1)
            # Find numbers in this grid line
            nums = re.findall(r'\b(\d{1,2})\b', grid_content)
            for n in nums:
                start_dates.append(int(n))
        
        # 3. Leg Parsing
        # Day     Flt    Dep         Arr
        #  1      4137   IAD 08:00   CMH 09:30   ...
        #         4119   CMH 11:00   ...
        
        # We need to be careful not to match the header line "Day Flt ..."
        if "Day" in line and "Flt" in line: continue
        
        # Regex for Flight Leg
        # Optional Day (digits), Flt (digits), Dep (AAA HH:MM), Arr (AAA HH:MM)
        # Note: Day is blank for subsequent legs on same day
        # Regex:
        # Start of line, maybe whitespace, maybe Day digits, whitespace, Flt digits, whitespace, AAA HH:MM...
        
        # Remove the grid part from line to avoid confusion
        line_clean = line.split('|')[0]
        
        # Flexible Leg Parsing
        # Matches core: [Day] Flt DepApt DepTime ArrApt ArrTime
        # Then captures remaining times in a list
        # Pattern: Whitespace, Day(opt), Flt, DepApt, DepTime, ArrApt, ArrTime...
        core_regex = r'^\s*(\d+)?\s+([A-Z]{2}\s+)?(\d{1,4})\s+([A-Z]{3,4})\s+(\d{1,2}:\d{2})\s+([A-Z]{3,4})\s+(\d{1,2}:\d{2})'
        core_match = re.search(core_regex, line_clean)
        
        if core_match:
            day_val = core_match.group(1)
            dh_marker = core_match.group(2)
            flt = core_match.group(3)
            dep_apt = core_match.group(4)
            dep_time = core_match.group(5)
            arr_apt = core_match.group(6)
            arr_time = core_match.group(7)
            
            # Find all remaining HHH:MM or HH:MM patterns in the rest of the line
            remainder = line_clean[core_match.end():]
            times = re.findall(r'(\d{1,3}:\d{2})', remainder)
            
            # Heuristic for Block/Credit:
            # If there's 1 time: It's likely Block.
            # If there's 2+: The last one is likely Credit or the one after Turn is Block.
            # In November: [Turn] [Block]
            # In December: [Block] [Credit]
            
            blk_time = None
            credit_val = None
            
            if len(times) >= 2:
                # If we have Turn and Block (November layout)
                # Or Block and Credit (December layout)
                # We'll treat the LAST time as the primary 'value' but we need to be smart.
                # Actually, in November, Block is the last time. 
                # In December, Credit is the last time and Block is before it.
                
                # Let's detect if "Credit" exists in the header for this file? 
                # Too complex. Let's just use the last two found times.
                blk_time = times[-2] if len(times) >= 2 else times[0]
                # If there's a 2nd time, it might be Credit.
                low_remainder = remainder.lower()
                if "credit" in low_remainder or len(times) > 1:
                    credit_val = times[-1]
                
                # Override: If only 2 times, the last one is probably the most interesting (Block or Credit)
                if len(times) == 1:
                    blk_time = times[0]
                elif len(times) == 2:
                     # If the header had 'Credit', the last is Credit. 
                     # For now, let's just assign 'blk_time' safely.
                     blk_time = times[0]
                     credit_val = times[1]
                else:
                    # 3+ times (Turn, Block, Duty, etc)
                    # Block is usually the one before the end if Duty is last?
                    # Let's just grab the last one for now as a fallback.
                    blk_time = times[-1]
            elif len(times) == 1:
                blk_time = times[0]

            # Maintain existing state logic
            if day_val:
                current_day_offset = int(day_val)
            else:
                # If day is missing, it's the same as previous leg
                # But we need to keep track of current_day_offset in the outer scope for this pairing
                # Actually, in this list format, `current_day_offset` should be persistent per pairing block
                pass 
                #Wait, if I reset `legs` per pairing, I need a variable `last_day_offset`
            
            # Wait, `current_day_offset` needs to be persistent across lines for one pairing
            # Let's initialize it in the header block? No, it changes.
            # If day_val is present: update last_day
            # If not: use last_day
            # But I need `last_day` to be defined.
            
            # Refined Loop Logic needed.
            legs.append({
                "day_raw": day_val,
                "flt": flt,
                "dep": dep_apt,
                "time": dep_time,
                "arr": arr_apt,
                "arr_time": arr_time,
                "blk": blk_time,
                "credit": credit_val, # Leg credit
                "is_dh": 1 if dh_marker and "DH" in dh_marker else 0
            })
            continue

        # 4. Total Credit Parsing
        # Total Credit 014:58
        credit_match = re.search(r'Total Credit\s+(\d{1,3}:\d{2})', line)
        if credit_match:
            current_total_credit = credit_match.group(1)

    # Save last one
    if current_pairing and start_dates and legs:
        save_pairing(session, current_pairing, month_year, start_dates, legs, current_total_credit)
        import_count += 1
        
    session.commit()
    print(f"Imported {import_count} pairings.")

def save_pairing(session, pairing_id, month_year, start_dates, legs, total_credit):
    # Process legs to handle Day inheritance
    processed_legs = []
    current_day = 1
    for leg in legs:
        if leg["day_raw"]:
            current_day = int(leg["day_raw"])
        
        processed_legs.append({
            "day": current_day,
            "flt": leg["flt"],
            "dep": leg["dep"],
            "time": leg["time"],
            "arr": leg["arr"],
            "arr_time": leg["arr_time"],
            "blk": leg["blk"],
            "credit": total_credit, # Use the Pairing Total Credit for the record
            "is_dh": leg["is_dh"]
        })
        
    # Instantiate for each start date
    # Get acceptable date range for this bid period
    bp_start, bp_end = get_bid_period_date_range(month_year.year, month_year.month)
    # Convert to datetime for comparison
    bp_start_dt = datetime.combine(bp_start, datetime.min.time())
    bp_end_dt = datetime.combine(bp_end, datetime.min.time())

    for start_day in start_dates:
        # Construct actual start date
        base_date = None
        
        # Try current month first
        try:
            candidate = month_year.replace(day=start_day)
            # Check if this date is within bid period (or reasonably close if we want flexibility, but strict is safer)
            # Actually, Feb 1 is in Feb bid period.
            base_date = candidate
        except ValueError:
            # If invalid (e.g. Feb 30), try previous month
            # This handles Jan 31 in Feb packet
            prev_month = month_year - timedelta(days=1)
            try:
                candidate = prev_month.replace(day=start_day)
                base_date = candidate
            except ValueError:
                pass
                
        # Special Case: Next Month overlap (e.g. Mar 1 in Feb packet)
        # If we found a base_date (e.g. Feb 1), it might actually be Mar 1 if Feb 1 isn't what was intended
        # But '1' usually means 1st of main month.
        # However, if 'base_date' is outside the bid period, strictly reject it?
        # Jan 31 is in Feb BP.
        # Feb 1 is in Feb BP.
        # Mar 1 is in Feb BP.
        
        # If we haven't found a valid date yet, check next month?
        # (Only really if current month day was valid but we want next month? Unlikely logic path)
        
        if not base_date:
            # Try next month? (e.g. input 1, but meant Mar 1 in Feb text?)
            # Usually input 1 works for Feb 1.
            # But just in case
            next_month = (month_year + timedelta(days=32)).replace(day=1)
            try:
                candidate = next_month.replace(day=start_day)
                base_date = candidate
            except ValueError:
                pass

        if not base_date:
            continue

        # Final Validation against Bid Period
        # Allow slight buffer? No, strict based on User Rules.
        # Jan 31 in Feb BP -> OK.
        # Mar 31 in Apr BP -> FAIL.
        if not (bp_start_dt <= base_date <= bp_end_dt):
            # Try to see if shifting months helps?
            # e.g. we parsed "31" as "Jan 31" for April BP (Apr 1-30). 
            # Jan 31 is not in Apr BP. Correctly rejected.
            
            # What if we parsed "1" as "Feb 1" for Feb BP (Jan 31-Mar 1). OK.
            # What if it was "Mar 1" in Feb BP?
            # We parsed "Feb 1". It is in BP. valid.
            # We effectively lose "Mar 1" capability if "Feb 1" is also valid.
            # But text file parsing is limited.
            
            # Main fix is for Jan 31 (Feb BP).
            # base_date = Jan 31. BP = Jan 31-Mar 1. Valid.
            pass
            
            # Double check invalid dates
            if base_date < bp_start_dt or base_date > bp_end_dt:
                # One last attempt: Check if it's Mar 1 in Feb BP
                # If we parsed Feb 1 (valid date) but maybe logic says check alternatives?
                # No, standard assumption applies.
                
                # If we parsed "30" in Feb -> Jan 30.
                # Jan 30 is NOT in Feb BP (Jan 31 start).
                # So Jan 30 rejected. Correct.
                continue

        for leg in processed_legs:
            # Flight Date = Base Date + (Leg Day - 1)
            flight_date = base_date + timedelta(days=(leg["day"] - 1))
            
            rec = ScheduledFlight(
                pairing_number=pairing_id,
                flight_number=leg["flt"],
                date=flight_date,
                departure_airport=leg["dep"],
                arrival_airport=leg["arr"],
                scheduled_departure=leg["time"],
                scheduled_arrival=leg["arr_time"],
                block_time=leg["blk"],
                total_credit=leg["credit"],
                pairing_start_date=base_date,
                is_deadhead=leg["is_dh"]
            )
            session.add(rec)



def upload_ioe_to_cloud(session):
    from firestore_lib import upload_ioe_assignment
    ioe_data = session.query(IOEAssignment).all()
    count = 0
    for rec in ioe_data:
        # Create a unique ID for the document, e.g., emp_pairing_date
        doc_id = f"{rec.employee_id}_{rec.pairing_number}_{rec.start_date.strftime('%Y%m%d')}"
        data = {
            "employee_id": rec.employee_id,
            "pairing_number": rec.pairing_number,
            "start_date": rec.start_date
        }
        upload_ioe_assignment(data, doc_id)
        count += 1
    return count

def upload_pairings_to_cloud(session):
    from firestore_lib import upload_pairing_bundle
    sched_data = session.query(ScheduledFlight).all()
    
    # Group by pairing_number and pairing_start_date
    bundles = {}
    for rec in sched_data:
        start_dt = rec.pairing_start_date or rec.date
        doc_id = f"{rec.pairing_number}_{start_dt.strftime('%Y%m%d')}"
        if doc_id not in bundles:
            bundles[doc_id] = {
                "pairing_number": rec.pairing_number,
                "date": start_dt,
                "legs": {}
            }
        
        # Unique leg key within trip
        leg_key = f"{rec.date.strftime('%Y%m%d')}_{rec.flight_number}_{rec.departure_airport}"
        bundles[doc_id]["legs"][leg_key] = {
            "date": rec.date,
            "flight_number": rec.flight_number,
            "departure_airport": rec.departure_airport,
            "arrival_airport": rec.arrival_airport,
            "scheduled_departure": rec.scheduled_departure,
            "scheduled_arrival": rec.scheduled_arrival,
            "block_time": rec.block_time,
            "total_credit": rec.total_credit
        }
        
    count = 0
    for doc_id, data in bundles.items():
        upload_pairing_bundle(doc_id, data)
        count += 1
    return count

def upload_flights_to_cloud(session, start_date=None, end_date=None):
    from firestore_lib import upload_daily_flights
    from database import Flight
    query = session.query(Flight)
    if start_date:
        query = query.filter(Flight.date >= start_date)
    if end_date:
        query = query.filter(Flight.date <= end_date)
        
    flights = query.all()
    
    # Group by date
    daily_bundles = {}
    for f in flights:
        date_str = f.date.strftime('%Y-%m-%d')
        if date_str not in daily_bundles:
            daily_bundles[date_str] = {}
        
        # Serialize flight details
        flight_data = {
            "flight_number": f.flight_number,
            "date": f.date,
            "tail_number": f.tail_number,
            "departure_airport": f.departure_airport,
            "arrival_airport": f.arrival_airport,
            "scheduled_departure": f.scheduled_departure,
            "scheduled_arrival": f.scheduled_arrival,
            "actual_departure": f.actual_departure,
            "actual_arrival": f.actual_arrival,
            "status": f.status,
            "aircraft_type": f.aircraft_type,
            "pax_data": f.pax_data,
            "load_data": f.load_data,
            "notes_data": f.notes_data or ""
        }
        # Add basic crew info
        crew_list = []
        for c in f.crew_members:
            # We need to query the association to get role and flags
            from database import flight_crew_association
            assoc = session.execute(
                flight_crew_association.select().where(
                    (flight_crew_association.c.flight_id == f.id) &
                    (flight_crew_association.c.crew_id == c.id)
                )
            ).fetchone()
            
            crew_list.append({
                "name": c.name, 
                "id": c.employee_id,
                "role": assoc.role if assoc else "Unknown",
                "flags": assoc.flags if assoc else ""
            })
        flight_data["crew"] = crew_list
        
        daily_bundles[date_str][f.flight_number] = flight_data
        
    count = 0
    for date_str, flights_map in daily_bundles.items():
        upload_daily_flights(date_str, flights_map)
        count += 1
    return count

def upload_metadata_to_cloud(session):
    from firestore_lib import upload_metadata
    from database import AppMetadata
    meta = session.query(AppMetadata).all()
    count = 0
    for m in meta:
        upload_metadata(m.key, m.value)
        count += 1
    return count

def sync_down_from_cloud(session):
    """
    Restores/Hydrates the local database from Firestore.
    """
    # Import inside function to avoid circular dependency
    from firestore_lib import download_daily_flights, download_pairings, download_ioe, download_metadata
    from database import Flight, ScheduledFlight, IOEAssignment, AppMetadata, CrewMember, flight_crew_association
    import datetime

    print("Starting Cloud -> Local Sync...")
    stats = {"flights": 0, "pairings": 0, "ioe": 0, "metadata": 0}

    # 1. METADATA
    for key, val_dict in download_metadata():
        if "value" in val_dict:
            # Upsert
            existing = session.query(AppMetadata).filter_by(key=key).first()
            if not existing:
                existing = AppMetadata(key=key)
                session.add(existing)
            existing.value = str(val_dict["value"])
            stats["metadata"] += 1
    session.commit()
    print("Metadata synced.")

    # 2. IOE ASSIGNMENTS
    for doc_id, data in download_ioe():
        try:
            emp_id = data.get("employee_id")
            pairing = data.get("pairing_number")
            s_date = data.get("start_date")
            
            exists = session.query(IOEAssignment).filter_by(
                employee_id=emp_id, pairing_number=pairing, start_date=s_date
            ).first()
            
            if not exists:
                rec = IOEAssignment(employee_id=emp_id, pairing_number=pairing, start_date=s_date)
                session.add(rec)
                stats["ioe"] += 1
        except Exception as e:
            print(f"Error restoring IOE {doc_id}: {e}")
    session.commit()
    print("IOE synced.")

    # 3. SCHEDULED PAIRINGS
    for doc_id, bundle in download_pairings():
        try:
            pairing_num = bundle.get("pairing_number")
            legs_map = bundle.get("legs", {})
            start_date = bundle.get("date")
            
            for leg_key, leg_data in legs_map.items():
                f_num = leg_data.get("flight_number")
                dep_apt = leg_data.get("departure_airport")
                f_date = leg_data.get("date") # Specific leg date

                exists = session.query(ScheduledFlight).filter_by(
                    pairing_number=pairing_num,
                    flight_number=f_num,
                    date=f_date,
                    departure_airport=dep_apt
                ).first()
                
                if not exists:
                    sf = ScheduledFlight(
                        pairing_number=pairing_num,
                        flight_number=f_num,
                        date=f_date,
                        departure_airport=dep_apt,
                        arrival_airport=leg_data.get("arrival_airport"),
                        scheduled_departure=leg_data.get("scheduled_departure"),
                        scheduled_arrival=leg_data.get("scheduled_arrival"),
                        block_time=leg_data.get("block_time"),
                        total_credit=leg_data.get("total_credit"),
                        pairing_start_date=start_date
                    )
                    session.add(sf)
                    stats["pairings"] += 1
        except Exception as e:
            print(f"Error restoring pairing {doc_id}: {e}")
    session.commit()
    print("Pairings synced.")

    # 4. FLIGHTS
    for doc_id, bundle in download_daily_flights():
        try:
            flights_map = bundle.get("flights", {})
            for f_num, f_data in flights_map.items():
                f_date = f_data.get("date")
                
                existing_f = session.query(Flight).filter_by(flight_number=f_num, date=f_date).first()
                crew_list = f_data.get("crew", [])
                
                if existing_f:
                    existing_f.tail_number = f_data.get("tail_number")
                    existing_f.status = f_data.get("status")
                    existing_f.actual_departure = f_data.get("actual_departure")
                    existing_f.actual_arrival = f_data.get("actual_arrival")
                    existing_f.departure_airport = f_data.get("departure_airport")
                    existing_f.arrival_airport = f_data.get("arrival_airport")
                else:
                    new_f = Flight(
                        flight_number=f_num,
                        date=f_date,
                        tail_number=f_data.get("tail_number"),
                        scheduled_departure=f_data.get("scheduled_departure"),
                        scheduled_arrival=f_data.get("scheduled_arrival"),
                        actual_departure=f_data.get("actual_departure"),
                        actual_arrival=f_data.get("actual_arrival"),
                        departure_airport=f_data.get("departure_airport"),
                        arrival_airport=f_data.get("arrival_airport"),
                        status=f_data.get("status"),
                        aircraft_type=f_data.get("aircraft_type"),
                        pax_data=f_data.get("pax_data"),
                        load_data=f_data.get("load_data"),
                        notes_data=f_data.get("notes_data")
                    )
                    session.add(new_f)
                    session.flush()
                    existing_f = new_f
                    stats["flights"] += 1

                # Sync Crew
                if existing_f.id:
                    session.execute(flight_crew_association.delete().where(
                        flight_crew_association.c.flight_id == existing_f.id
                    ))
                    
                    for c_dict in crew_list:
                        c_name = c_dict.get("name")
                        c_id = c_dict.get("id")
                        if not c_name: continue
                        
                        crew_rec = session.query(CrewMember).filter_by(employee_id=c_id).first()
                        if not crew_rec and c_name:
                             crew_rec = session.query(CrewMember).filter_by(name=c_name).first()
                        
                        if not crew_rec:
                            crew_rec = CrewMember(name=c_name, employee_id=c_id)
                            session.add(crew_rec)
                            session.flush()
                        
                        ins = flight_crew_association.insert().values(
                            flight_id=existing_f.id,
                            crew_id=crew_rec.id,
                            role=c_dict.get("role", "Unknown"),
                            flags=c_dict.get("flags", "")
                        )
                        session.execute(ins)

        except Exception as e:
            print(f"Error restoring flights for {doc_id}: {e}")
    
    session.commit()
    print("Flights synced.")
    return stats

if __name__ == "__main__":
    from database import get_metadata
    from firestore_lib import set_cloud_sync_enabled, is_cloud_sync_enabled
    from config import ENABLE_CLOUD_SYNC
    
    # 1. Clear old data to prevent duplicates
    session = get_session()
    print("Clearing existing ScheduledFlight and IOEAssignment data...")
    session.query(ScheduledFlight).delete()
    session.query(IOEAssignment).delete()
    session.commit()
    
    # 2. Ingest
    ingest_all(session)
    
    # 3. Check Cloud Sync
    # Check DB preference first (like app.py)
    db_cloud_sync = get_metadata(session, "ui_enable_cloud_sync")
    
    should_sync = False
    if db_cloud_sync is not None:
        should_sync = (db_cloud_sync.lower() == 'true')
    else:
        should_sync = ENABLE_CLOUD_SYNC
        
    if should_sync:
        print("\nCloud Sync is ENABLED. Uploading data to Firestore...")
        set_cloud_sync_enabled(True)
        
        try:
            print("  - Uploading Pairings...")
            p_count = upload_pairings_to_cloud(session)
            print(f"    Uploaded {p_count} pairing bundles.")
            
            print("  - Uploading IOE Assignments...")
            i_count = upload_ioe_to_cloud(session)
            print(f"    Uploaded {i_count} IOE assignments.")
            
            # Note: We probably don't need to upload ALL flight history every time ingest runs, 
            # as ingest focus on Pairings/IOE text files.
            
        except Exception as e:
            print(f"Error during cloud sync: {e}")
            import traceback
            traceback.print_exc()
            
    else:
        print("\nCloud Sync is DISABLED. Skipping upload.")

    session.close()
