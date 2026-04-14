"""
generate_jsp_report.py

Usage:
  python tools/generate_jsp_report.py
  python tools/generate_jsp_report.py --csv
  python tools/generate_jsp_report.py --start-date 2025-07-01 --end-date 2025-07-31

This script gathers all flights involving crew members with a 'JSP' tag.
It outputs a text report to 'tools/jsp_flights_report.txt'.
If the '--csv' flag is provided, it also exports the data to 'tools/jsp_flights_report.csv'.

Requirements:
  - Must be run from the project root or the tools directory.
  - Requires the database to be initialized and populated.
"""
import os
import sys
import json
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

def generate_report(export_csv=False, start_date=None, end_date=None):
    session = SessionLocal()
    try:
        # 1. Gather all flights that have a crew member with a 'JSP' tag
        jsp_flights_query = session.query(Flight).join(
            flight_crew_association, Flight.id == flight_crew_association.c.flight_id
        ).filter(
            flight_crew_association.c.flags.like('%JSP%')
        )
        
        if start_date:
            jsp_flights_query = jsp_flights_query.filter(Flight.date >= start_date)
        if end_date:
            jsp_flights_query = jsp_flights_query.filter(Flight.date <= end_date)
            
        jsp_flights_query = jsp_flights_query.distinct()

        flights = jsp_flights_query.all()
        
        if not flights:
            print("No flights found with JSP tag.")
            return

        # 2. Find unique people with JSP tag (excluding trainees with 'T' tag)
        jsp_people_query = session.query(CrewMember.name).join(
            flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
        ).filter(
            flight_crew_association.c.flags.like('%JSP%'),
            ~flight_crew_association.c.flags.like('%T%')
        ).distinct().order_by(CrewMember.name)
        
        jsp_people = [p.name for p in jsp_people_query.all()]

        print(f"Found {len(flights)} flights and {len(jsp_people)} unique people with JSP tag. Generating report...")

        # 3. Format the report
        report_content = []
        report_content.append("=" * 80)
        report_content.append(f"JSP FLIGHT REPORT")
        report_content.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if start_date or end_date:
            start_str = start_date.strftime('%Y-%m-%d') if start_date else 'Beginning'
            end_str = end_date.strftime('%Y-%m-%d') if end_date else 'Present'
            report_content.append(f"Date Range: {start_str} to {end_str}")
        report_content.append(f"Total Flights Found: {len(flights)}")
        report_content.append("=" * 80)
        report_content.append("\n")

        report_content.append("UNIQUE PEOPLE WITH JSP TAG:")
        for person in jsp_people:
            report_content.append(f"  - {person}")
        report_content.append("\n" + "=" * 80 + "\n")

        for f in flights:
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
            
            if f.actual_departure or f.actual_arrival:
                report_content.append("ACTUAL TIMES:")
                report_content.append(f"  Departure: {f.actual_departure.strftime('%H:%M') if f.actual_departure else 'N/A'}")
                report_content.append(f"  Arrival:   {f.actual_arrival.strftime('%H:%M') if f.actual_arrival else 'N/A'}")
            
            report_content.append("BLOCK TIMES:")
            report_content.append(f"  Planned Block: {f.planned_block_minutes} mins" if f.planned_block_minutes is not None else "  Planned Block: N/A")
            report_content.append(f"  Actual Block:  {f.actual_block_minutes} mins" if f.actual_block_minutes is not None else "  Actual Block:  N/A")
            
            report_content.append("-" * 40)
            report_content.append("CREW:")
            # Get crew for this flight specifically to show their roles/flags
            crew_data = session.execute(
                select(
                    CrewMember.name, 
                    flight_crew_association.c.role, 
                    flight_crew_association.c.flags
                ).join(
                    flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
                ).where(
                    flight_crew_association.c.flight_id == f.id
                )
            ).fetchall()
            
            for name, role, flags in crew_data:
                flag_str = f" [{flags}]" if flags else ""
                report_content.append(f"  {role or 'Unknown'}: {name}{flag_str}")

            report_content.append("\n" + "=" * 80 + "\n")

        # 4. Save to file
        output_file = os.path.join("tools", "jsp_flights_report.txt")
        os.makedirs("tools", exist_ok=True)
        with open(output_file, "w") as out:
            out.write("\n".join(report_content))
            
        print(f"Report successfully saved to {output_file}")

        # 5. Export to CSV if requested
        if export_csv:
            csv_file = os.path.join("tools", "jsp_flights_report.csv")
            with open(csv_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow([
                    "Flight Number", "Date", "Departure", "Arrival", "Status",
                    "Tail Number", "Aircraft Type", "Version", "Scheduled Departure",
                    "Scheduled Arrival", "Actual Departure", "Actual Arrival",
                    "Planned Block (mins)", "Actual Block (mins)", "Crew Breakdown"
                ])
                for f in flights:
                    # Collect crew strings for CSV
                    crew_data = session.execute(
                        select(
                            CrewMember.name, 
                            flight_crew_association.c.role, 
                            flight_crew_association.c.flags
                        ).join(
                            flight_crew_association, CrewMember.id == flight_crew_association.c.crew_id
                        ).where(
                            flight_crew_association.c.flight_id == f.id
                        )
                    ).fetchall()
                    
                    crew_strs = []
                    for name, role, flags in crew_data:
                        flag_str = f" [{flags}]" if flags else ""
                        crew_strs.append(f"{role or 'Unknown'}: {name}{flag_str}")
                    crew_joined = " | ".join(crew_strs)

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
    parser = argparse.ArgumentParser(description="Generate a report of flights involving crew with a JSP tag.")
    parser.add_argument("--csv", action="store_true", help="Export the data to a CSV file in addition to the text report.")
    parser.add_argument("--start-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    
    generate_report(export_csv=args.csv, start_date=args.start_date, end_date=args.end_date)
