import os
import sys
import json
from datetime import datetime
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import Flight, CrewMember, flight_crew_association, DB_URL

# Setup Database
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def generate_report():
    session = SessionLocal()
    try:
        # 1. Gather all flights that have a crew member with a 'JSP' tag
        jsp_flights_query = session.query(Flight).join(
            flight_crew_association, Flight.id == flight_crew_association.c.flight_id
        ).filter(
            flight_crew_association.c.flags.like('%JSP%')
        ).distinct()

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

        # 3. Save to file
        output_file = os.path.join("tools", "jsp_flights_report.txt")
        os.makedirs("tools", exist_ok=True)
        with open(output_file, "w") as out:
            out.write("\n".join(report_content))
            
        print(f"Report successfully saved to {output_file}")

    finally:
        session.close()

if __name__ == "__main__":
    generate_report()
