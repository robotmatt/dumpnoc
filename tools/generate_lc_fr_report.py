"""
generate_lc_fr_report.py

Usage:
  python tools/generate_lc_fr_report.py
  python tools/generate_lc_fr_report.py --csv
  python tools/generate_lc_fr_report.py --start-date 2025-07-01 --end-date 2025-07-31

This script gathers all flight legs since a specified start date (default: July 1, 2025)
that show 'LC' (Line Check) or 'FR' (Federal/Ferry) flags on a Line Check Pilot (LCP) and
ignores flight attendants.

It outputs:
  - A summary breakdown of unique LCPs and legs flown.
  - A detailed text report to 'tools/lc_fr_report.txt'.
  - (Optional) Detailed CSV of flight legs to 'tools/lc_fr_report.csv'.
  - (Optional) Detailed CSV of crew breakdown to 'tools/lc_fr_crew_breakdown.csv'.
"""
import os
import sys
import csv
import argparse
from datetime import datetime

# Change working directory to project root to ensure relative paths work correctly
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(project_root)
sys.path.append(project_root)

from sqlalchemy import create_engine, select, or_
from sqlalchemy.orm import sessionmaker

from database import Flight, CrewMember, flight_crew_association, LCP, DB_URL

# Setup Database
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def parse_flags(flags_str):
    if not flags_str:
        return []
    return [f.strip().upper() for f in flags_str.split(',') if f.strip()]

def generate_report(export_csv=False, start_date=None, end_date=None):
    # Set default start date to July 1, 2025 if not provided
    if not start_date:
        start_date = datetime(2025, 7, 1)
        
    session = SessionLocal()
    try:
        # Load all LCP employee IDs from database
        lcp_list = session.query(LCP.employee_id).filter(LCP.employee_id.isnot(None)).all()
        lcp_employee_ids = {item.employee_id.strip() for item in lcp_list}
        
        if not lcp_employee_ids:
            print("Warning: No LCPs found in the database. Cannot run report.")
            return

        # 1. Fetch potential flight IDs that have 'LC' or 'FR' anywhere in the flags string
        # Filtered to only flight crew members who are in the LCP employee ID list and not FAs
        candidate_flights_query = session.query(Flight.id).join(
            flight_crew_association, Flight.id == flight_crew_association.c.flight_id
        ).join(
            CrewMember, flight_crew_association.c.crew_id == CrewMember.id
        ).filter(
            or_(
                flight_crew_association.c.flags.like('%LC%'),
                flight_crew_association.c.flags.like('%FR%')
            ),
            CrewMember.employee_id.in_(list(lcp_employee_ids)),
            ~flight_crew_association.c.role.like('%FA%')
        )
        
        candidate_flights_query = candidate_flights_query.filter(Flight.date >= start_date)
        if end_date:
            candidate_flights_query = candidate_flights_query.filter(Flight.date <= end_date)
            
        candidate_flight_ids = [r[0] for r in candidate_flights_query.distinct().all()]
        
        if not candidate_flight_ids:
            print(f"No flights found with LC or FR flags on an LCP since {start_date.strftime('%Y-%m-%d')}.")
            return

        # 2. Query all flights and their crew members for these candidate flight IDs
        flights_data = session.query(Flight).filter(Flight.id.in_(candidate_flight_ids)).order_by(
            Flight.date.asc(), Flight.scheduled_departure.asc()
        ).all()
        
        matching_flights = []
        crew_stats = {} # crew_id -> dict of stats
        
        for f in flights_data:
            # Query all crew for this flight specifically to check their flags precisely
            crew_data = session.execute(
                select(
                    CrewMember.id,
                    CrewMember.name,
                    CrewMember.employee_id,
                    flight_crew_association.c.role,
                    flight_crew_association.c.flags
                ).join(
                    flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
                ).where(
                    flight_crew_association.c.flight_id == f.id
                )
            ).fetchall()
            
            # Check if this flight actually has LC or FR (precise token match) on an LCP
            has_lc_or_fr_on_lcp = False
            flight_crew_info = []
            
            for c_id, name, emp_id, role, flags_str in crew_data:
                role_upper = (role or "").upper()
                is_fa = 'FA' in role_upper or 'FLIGHT ATTENDANT' in role_upper
                is_lcp = emp_id in lcp_employee_ids
                
                tokens = parse_flags(flags_str)
                is_lc = 'LC' in tokens
                is_fr = 'FR' in tokens
                
                # Must be an LCP carrying the flag, and not a Flight Attendant
                is_valid_flagged = (is_lc or is_fr) and is_lcp and not is_fa
                
                if is_valid_flagged:
                    has_lc_or_fr_on_lcp = True
                
                flight_crew_info.append({
                    'id': c_id,
                    'name': name,
                    'employee_id': emp_id,
                    'role': role,
                    'flags': flags_str,
                    'tokens': tokens,
                    'is_lc': is_lc and is_lcp and not is_fa,
                    'is_fr': is_fr and is_lcp and not is_fa,
                    'is_lcp': is_lcp,
                    'is_fa': is_fa
                })
                
            if not has_lc_or_fr_on_lcp:
                # Substring matched in SQL, or it wasn't on an LCP, or was on an FA
                continue
                
            matching_flights.append((f, flight_crew_info))
            
            # Update crew statistics (only for LCPs who are not FAs)
            for c_info in flight_crew_info:
                if c_info['is_fa'] or not c_info['is_lcp']:
                    continue
                    
                c_id = c_info['id']
                if c_id not in crew_stats:
                    crew_stats[c_id] = {
                        'name': c_info['name'],
                        'employee_id': c_info['employee_id'] or 'N/A',
                        'lc_legs_flown': 0,      # Number of legs flown with LC flag
                        'fr_legs_flown': 0,      # Number of legs flown with FR flag
                        'any_flagged_legs': 0,   # Number of legs flown with LC or FR flag
                        'total_legs_assisted': 0  # Number of legs flown as crew on any LC/FR flight
                    }
                
                crew_stats[c_id]['total_legs_assisted'] += 1
                if c_info['is_lc']:
                    crew_stats[c_id]['lc_legs_flown'] += 1
                if c_info['is_fr']:
                    crew_stats[c_id]['fr_legs_flown'] += 1
                if c_info['is_lc'] or c_info['is_fr']:
                    crew_stats[c_id]['any_flagged_legs'] += 1

        if not matching_flights:
            print(f"No flights found with exact LC or FR flags on an LCP since {start_date.strftime('%Y-%m-%d')}.")
            return

        # Filter crew stats to only include LCPs who actually flew flagged legs
        flagged_crew_stats = {c_id: stats for c_id, stats in crew_stats.items() if stats['any_flagged_legs'] > 0}
        
        # Sort flagged crew stats by total flagged legs desc, then by name
        sorted_flagged_crew = sorted(
            flagged_crew_stats.values(),
            key=lambda x: (-x['any_flagged_legs'], x['name'])
        )
        
        # Count totals
        lc_people_count = sum(1 for c in flagged_crew_stats.values() if c['lc_legs_flown'] > 0)
        fr_people_count = sum(1 for c in flagged_crew_stats.values() if c['fr_legs_flown'] > 0)
        total_unique_people = len(flagged_crew_stats)

        print(f"Found {len(matching_flights)} flights and {total_unique_people} unique LCPs with LC/FR flags. Generating report...")

        # 3. Format the text report
        report_content = []
        report_content.append("=" * 80)
        report_content.append(f"LINE CHECK (LC) / FEDERAL (FR) LCP FLIGHT REPORT")
        report_content.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d') if end_date else 'Present'
        report_content.append(f"Date Range: {start_str} to {end_str}")
        report_content.append(f"Total Matching Legs: {len(matching_flights)}")
        report_content.append(f"Total Unique LCPs Flagged: {total_unique_people} (LC: {lc_people_count}, FR: {fr_people_count})")
        report_content.append("=" * 80)
        report_content.append("\n")

        # Crew breakdown summary section
        report_content.append("LCP MEMBERS BREAKDOWN (Sorted by Flagged Legs Flown):")
        report_content.append(f"{'Line Check Pilot (LCP) Name':<30} | {'ID':<10} | {'LC Legs':<8} | {'FR Legs':<8} | {'Total Flagged':<13} | {'Total Assisted':<14}")
        report_content.append("-" * 95)
        for c in sorted_flagged_crew:
            report_content.append(
                f"{c['name']:<30} | {c['employee_id']:<10} | {c['lc_legs_flown']:<8} | {c['fr_legs_flown']:<8} | {c['any_flagged_legs']:<13} | {c['total_legs_assisted']:<14}"
            )
        report_content.append("\n" + "=" * 80 + "\n")

        # Detailed flights section
        report_content.append("DETAILED FLIGHT LOG:")
        report_content.append("\n")
        
        for f, crew_list in matching_flights:
            report_content.append(f"FLIGHT: {f.flight_number} | DATE: {f.date.strftime('%Y-%m-%d')}")
            report_content.append(f"ROUTE: {f.departure_airport} -> {f.arrival_airport}")
            report_content.append(f"STATUS: {f.status or 'Unknown'}")
            report_content.append(f"AIRCRAFT: {f.tail_number or 'N/A'} ({f.aircraft_type or 'N/A'})")
            if f.version:
                report_content.append(f"VERSION: {f.version}")
            
            report_content.append("-" * 40)
            report_content.append("SCHEDULED TIMES:")
            report_content.append(f"  Departure: {f.scheduled_departure.strftime('%H:%M') if f.scheduled_departure else 'N/A'}")
            report_content.append(f"  Arrival:   {f.scheduled_arrival.strftime('%H:%M') if f.scheduled_arrival else 'N/A'}")
            
            # Check OOOI fallback
            act_dep = f.actual_departure or f.actual_off
            act_arr = f.actual_arrival or f.actual_on
            if act_dep or act_arr:
                report_content.append("ACTUAL TIMES:")
                report_content.append(f"  Departure: {act_dep.strftime('%H:%M') if act_dep else 'N/A'}")
                report_content.append(f"  Arrival:   {act_arr.strftime('%H:%M') if act_arr else 'N/A'}")
            
            report_content.append("BLOCK TIMES:")
            report_content.append(f"  Planned Block: {f.planned_block_minutes} mins" if f.planned_block_minutes is not None else "  Planned Block: N/A")
            report_content.append(f"  Actual Block:  {f.actual_block_minutes} mins" if f.actual_block_minutes is not None else "  Actual Block:  N/A")
            
            report_content.append("-" * 40)
            report_content.append("CREW ON BOARD:")
            for c in crew_list:
                flag_str = f" [{c['flags']}]" if c['flags'] else ""
                lcp_tag = " (LCP)" if c['is_lcp'] else ""
                report_content.append(f"  {c['role'] or 'Unknown'}: {c['name']} (ID: {c['employee_id'] or 'N/A'}){lcp_tag}{flag_str}")
            
            report_content.append("\n" + "=" * 80 + "\n")

        # Save text report to file
        output_txt = os.path.join("tools", "lc_fr_report.txt")
        os.makedirs("tools", exist_ok=True)
        with open(output_txt, "w", encoding="utf-8") as out:
            out.write("\n".join(report_content))
        print(f"Text report saved to {output_txt}")

        # 4. Save to CSV files if requested
        if export_csv:
            # Flight logs CSV
            output_csv = os.path.join("tools", "lc_fr_report.csv")
            with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    "Flight Number", "Date", "Departure", "Arrival", "Status",
                    "Tail Number", "Aircraft Type", "Version", "Scheduled Departure",
                    "Scheduled Arrival", "Actual Departure", "Actual Arrival",
                    "Planned Block (mins)", "Actual Block (mins)", "Crew Breakdown"
                ])
                for f, crew_list in matching_flights:
                    crew_strs = []
                    for c in crew_list:
                        flag_str = f" [{c['flags']}]" if c['flags'] else ""
                        lcp_tag = " (LCP)" if c['is_lcp'] else ""
                        crew_strs.append(f"{c['role'] or 'Unknown'}: {c['name']} (ID: {c['employee_id'] or 'N/A'}){lcp_tag}{flag_str}")
                    crew_joined = " | ".join(crew_strs)
                    
                    act_dep = f.actual_departure or f.actual_off
                    act_arr = f.actual_arrival or f.actual_on
                    
                    writer.writerow([
                        f.flight_number,
                        f.date.strftime('%Y-%m-%d') if f.date else "N/A",
                        f.departure_airport,
                        f.arrival_airport,
                        f.status,
                        f.tail_number,
                        f.aircraft_type,
                        f.version,
                        f.scheduled_departure.strftime('%Y-%m-%d %H:%M') if f.scheduled_departure else "N/A",
                        f.scheduled_arrival.strftime('%Y-%m-%d %H:%M') if f.scheduled_arrival else "N/A",
                        act_dep.strftime('%Y-%m-%d %H:%M') if act_dep else "N/A",
                        act_arr.strftime('%Y-%m-%d %H:%M') if act_arr else "N/A",
                        f.planned_block_minutes,
                        f.actual_block_minutes,
                        crew_joined
                    ])
            print(f"Flight legs CSV saved to {output_csv}")

            # Crew breakdown CSV
            output_crew_csv = os.path.join("tools", "lc_fr_crew_breakdown.csv")
            with open(output_crew_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    "Crew Member Name", "Employee ID", "LC Legs Flown", "FR Legs Flown", "Total Flagged Legs Flown", "Total LC/FR Flights Assisted"
                ])
                for c in sorted_flagged_crew:
                    writer.writerow([
                        c['name'],
                        c['employee_id'],
                        c['lc_legs_flown'],
                        c['fr_legs_flown'],
                        c['any_flagged_legs'],
                        c['total_legs_assisted']
                    ])
            print(f"Crew breakdown CSV saved to {output_crew_csv}")

    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a report of flight legs since July 2025 with LC or FR crew flags on LCPs.")
    parser.add_argument("--csv", action="store_true", help="Export the data to CSV files in addition to the text report.")
    parser.add_argument("--start-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="Start date in YYYY-MM-DD format (default: 2025-07-01).")
    parser.add_argument("--end-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    
    generate_report(export_csv=args.csv, start_date=args.start_date, end_date=args.end_date)
