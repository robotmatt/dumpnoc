"""
export_crew_flights.py

Usage:
  python tools/export_crew_flights.py --employee-id 123456
  python tools/export_crew_flights.py --employee-id 123456 --csv
  python tools/export_crew_flights.py --employee-id 123456 --start-date 2025-07-01 --end-date 2025-07-31

This script exports every flight a specific crew member did to a text file.
If the '--csv' flag is provided, it also exports the data to a CSV file.
Filters by crew employee ID number and optionally by date.
"""
import os
import sys
import csv
import argparse
from datetime import datetime

# Change working directory to project root to ensure relative paths (like db/noc_data.db) work correctly
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(project_root)
sys.path.append(project_root)

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from database import Flight, CrewMember, flight_crew_association, DB_URL

# Setup Database
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def export_flights(employee_id, export_csv=False, start_date=None, end_date=None):
    session = SessionLocal()
    try:
        # 1. Find the crew member
        crew_member = session.query(CrewMember).filter(CrewMember.employee_id == employee_id).first()
        
        if not crew_member:
            print(f"Error: No crew member found with Employee ID: {employee_id}")
            return

        print(f"Found crew member: {crew_member.name} (ID: {employee_id})")

        # 2. Gather all flights for this crew member
        flights_query = session.query(Flight).join(
            flight_crew_association, Flight.id == flight_crew_association.c.flight_id
        ).filter(
            flight_crew_association.c.crew_id == crew_member.id
        )
        
        if start_date:
            flights_query = flights_query.filter(Flight.date >= start_date)
        if end_date:
            flights_query = flights_query.filter(Flight.date <= end_date)
            
        flights_query = flights_query.order_by(Flight.date.asc(), Flight.scheduled_departure.asc())

        flights = flights_query.all()
        
        if not flights:
            print(f"No flights found for {crew_member.name} in the specified date range.")
            return

        print(f"Found {len(flights)} flights. Generating export...")

        # 3. Format the report
        report_content = []
        report_content.append("=" * 80)
        report_content.append(f"FLIGHT HISTORY REPORT - {crew_member.name} ({employee_id})")
        report_content.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if start_date or end_date:
            start_str = start_date.strftime('%Y-%m-%d') if start_date else 'Beginning'
            end_str = end_date.strftime('%Y-%m-%d') if end_date else 'Present'
            report_content.append(f"Date Range: {start_str} to {end_str}")
        report_content.append(f"Total Flights Found: {len(flights)}")
        report_content.append("=" * 80)
        report_content.append("\n")

        for f in flights:
            # Get role and flags for this crew member on this flight
            assignment = session.query(flight_crew_association).filter(
                flight_crew_association.c.flight_id == f.id,
                flight_crew_association.c.crew_id == crew_member.id
            ).first()
            
            role = assignment.role if assignment else "Unknown"
            flags = assignment.flags if assignment else ""
            flag_str = f" [{flags}]" if flags else ""

            report_content.append(f"FLIGHT: {f.flight_number} | DATE: {f.date.strftime('%Y-%m-%d')} | ROLE: {role}{flag_str}")
            report_content.append(f"ROUTE: {f.departure_airport} -> {f.arrival_airport}")
            report_content.append(f"STATUS: {f.status or 'Unknown'}")
            report_content.append(f"AIRCRAFT: {f.tail_number or 'N/A'} ({f.aircraft_type or 'N/A'})")
            
            report_content.append("-" * 40)
            report_content.append("TIMES:")
            report_content.append(f"  Sched Out: {f.scheduled_departure.strftime('%H:%M') if f.scheduled_departure else 'N/A'}")
            report_content.append(f"  Actual Out: {f.actual_departure.strftime('%H:%M') if f.actual_departure else 'N/A'}")
            report_content.append(f"  Actual In:  {f.actual_arrival.strftime('%H:%M') if f.actual_arrival else 'N/A'}")
            
            report_content.append(f"  Block Time: {f.actual_block_minutes} mins" if f.actual_block_minutes is not None else "  Block Time: N/A")
            
            # Show other crew members
            other_crew = session.execute(
                select(
                    CrewMember.name, 
                    flight_crew_association.c.role, 
                    flight_crew_association.c.flags
                ).join(
                    flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
                ).where(
                    flight_crew_association.c.flight_id == f.id,
                    CrewMember.id != crew_member.id
                )
            ).fetchall()
            
            if other_crew:
                report_content.append("-" * 40)
                report_content.append("OTHER CREW:")
                for name, o_role, o_flags in other_crew:
                    o_flag_str = f" [{o_flags}]" if o_flags else ""
                    report_content.append(f"  {o_role or 'Unknown'}: {name}{o_flag_str}")

            report_content.append("\n" + "=" * 80 + "\n")

        # 4. Save to file
        safe_name = crew_member.name.replace(" ", "_").replace(",", "").lower()
        output_file = os.path.join("tools", f"flights_{safe_name}_{employee_id}.txt")
        os.makedirs("tools", exist_ok=True)
        with open(output_file, "w") as out:
            out.write("\n".join(report_content))
            
        print(f"Report successfully saved to {output_file}")

        # 5. Export to CSV if requested
        if export_csv:
            csv_file = os.path.join("tools", f"flights_{safe_name}_{employee_id}.csv")
            with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow([
                    "Flight Number", "Date", "Role", "Flags", "Departure", "Arrival", "Status",
                    "Tail Number", "Aircraft Type", "Scheduled Departure",
                    "Actual Departure", "Actual Arrival",
                    "Planned Block (mins)", "Actual Block (mins)", "Other Crew"
                ])
                for f in flights:
                    assignment = session.query(flight_crew_association).filter(
                        flight_crew_association.c.flight_id == f.id,
                        flight_crew_association.c.crew_id == crew_member.id
                    ).first()
                    
                    role = assignment.role if assignment else "Unknown"
                    flags = assignment.flags if assignment else ""

                    # Collect other crew strings
                    other_crew = session.execute(
                        select(
                            CrewMember.name, 
                            flight_crew_association.c.role, 
                            flight_crew_association.c.flags
                        ).join(
                            flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
                        ).where(
                            flight_crew_association.c.flight_id == f.id,
                            CrewMember.id != crew_member.id
                        )
                    ).fetchall()
                    
                    crew_strs = []
                    for name, o_role, o_flags in other_crew:
                        o_flag_str = f" [{o_flags}]" if o_flags else ""
                        crew_strs.append(f"{o_role or 'Unknown'}: {name}{o_flag_str}")
                    crew_joined = " | ".join(crew_strs)

                    writer.writerow([
                        f.flight_number,
                        f.date.strftime('%Y-%m-%d') if f.date else "N/A",
                        role,
                        flags,
                        f.departure_airport,
                        f.arrival_airport,
                        f.status,
                        f.tail_number,
                        f.aircraft_type,
                        f.scheduled_departure.strftime('%Y-%m-%d %H:%M') if f.scheduled_departure else "N/A",
                        f.actual_departure.strftime('%Y-%m-%d %H:%M') if f.actual_departure else "N/A",
                        f.actual_arrival.strftime('%Y-%m-%d %H:%M') if f.actual_arrival else "N/A",
                        f.planned_block_minutes,
                        f.actual_block_minutes,
                        crew_joined
                    ])
            print(f"CSV Report successfully saved to {csv_file}")

    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export flight history for a specific crew member.")
    parser.add_argument("--employee-id", required=True, help="Employee ID of the crew member.")
    parser.add_argument("--csv", action="store_true", help="Export the data to a CSV file in addition to the text report.")
    parser.add_argument("--start-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    
    export_flights(employee_id=args.employee_id, export_csv=args.csv, start_date=args.start_date, end_date=args.end_date)
