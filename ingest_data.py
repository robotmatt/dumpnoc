import re
import os
from datetime import datetime, timedelta
from database import get_session, ScheduledFlight, IOEAssignment, init_db

PAIRINGS_DIR = "pairings"
IOE_DIR = "ioe"

def ingest_all(session):
    # IOE Files
    if os.path.exists(IOE_DIR):
        for f in os.listdir(IOE_DIR):
            if f.endswith(".txt"):
                parse_ioe_file(os.path.join(IOE_DIR, f), session)
    
    # Pairings Files
    if os.path.exists(PAIRINGS_DIR):
        for f in os.listdir(PAIRINGS_DIR):
            if f.endswith(".txt"):
                parse_pairings_file(os.path.join(PAIRINGS_DIR, f), session)

def parse_ioe_file(filepath, session):
    print(f"Parsing IOE file: {filepath}")
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    count = 0
    for line in lines:
        line = line.strip()
        if not line or line.startswith("-") or line.startswith("IOE") or line.startswith("Period") or line.startswith("Category") or line.startswith("Employee"):
            continue
            
        parts = line.split()
        if len(parts) >= 3:
            emp_id = parts[0]
            pairing = parts[1]
            date_str = parts[2]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                rec = IOEAssignment(employee_id=emp_id, pairing_number=pairing, start_date=dt)
                session.add(rec)
                count += 1
            except ValueError:
                pass
    session.commit()
    print(f"Imported {count} IOE assignments.")

def parse_pairings_file(filepath, session):
    print(f"Parsing Pairings file: {filepath}")
    with open(filepath, 'r') as f:
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
    
    import_count = 0
    
    for line in lines:
        # 1. Header Detection
        # I0001  Check-In ... December 2025
        header_match = re.search(r'^([IH]\d{4})\s+Check-In.*(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', line)
        if header_match:
            # Save previous if exists
            if current_pairing and start_dates and legs:
                save_pairing(session, current_pairing, month_year, start_dates, legs)
                import_count += 1
            
            # Reset
            current_pairing = header_match.group(1)
            month_str = header_match.group(2)
            year_str = header_match.group(3)
            month_year = datetime.strptime(f"{month_str} {year_str}", "%B %Y")
            legs = []
            start_dates = []
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
        
        # Match leg
        # Group 1: Day (optional)
        # Group 2: Flt
        # Group 3: Dep Apt
        # Group 4: Dep Time
        # Group 5: Arr Apt
        leg_match = re.search(r'^\s*(\d+)?\s+(\d{4})\s+([A-Z]{3})\s+(\d{2}:\d{2})\s+([A-Z]{3})', line_clean)
        if leg_match:
            day_val = leg_match.group(1)
            flt = leg_match.group(2)
            dep_apt = leg_match.group(3)
            dep_time = leg_match.group(4)
            arr_apt = leg_match.group(5)
            
            # State tracking for Day
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
                "arr": arr_apt
            })

    # Save last one
    if current_pairing and start_dates and legs:
        save_pairing(session, current_pairing, month_year, start_dates, legs)
        import_count += 1
        
    session.commit()
    print(f"Imported {import_count} pairings.")

def save_pairing(session, pairing_id, month_year, start_dates, legs):
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
            "time": leg["time"], # "HH:MM"
            "arr": leg["arr"]
        })
        
    # Instantiate for each start date
    for start_day in start_dates:
        # Construct actual start date
        try:
            base_date = month_year.replace(day=start_day)
        except ValueError:
            continue # Invalid date? e.g. Feb 30
            
        for leg in processed_legs:
            # Flight Date = Base Date + (Leg Day - 1)
            flight_date = base_date + timedelta(days=(leg["day"] - 1))
            
            rec = ScheduledFlight(
                pairing_number=pairing_id,
                flight_number=leg["flt"],
                date=flight_date,
                departure_airport=leg["dep"],
                arrival_airport=leg["arr"],
                scheduled_departure=leg["time"]
            )
            session.add(rec)

if __name__ == "__main__":
    # Clear old data to prevent duplicates
    session = get_session()
    print("Clearing existing ScheduledFlight and IOEAssignment data...")
    session.query(ScheduledFlight).delete()
    session.query(IOEAssignment).delete()
    session.commit()
    
    ingest_all(session)
    session.close()
