from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Table
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from config import DB_URL

Base = declarative_base()

# Association table for Flight <-> Crew
flight_crew_association = Table(
    'flight_crew', Base.metadata,
    Column('flight_id', Integer, ForeignKey('flights.id'), primary_key=True),
    Column('crew_id', Integer, ForeignKey('crew.id'), primary_key=True),
    Column('role', String), # e.g., Captain, First Officer, Cabin Crew
    Column('flags', String) # e.g., "IOE, L"
)

class FlightHistory(Base):
    __tablename__ = 'flight_history'
    id = Column(Integer, primary_key=True)
    flight_id = Column(Integer, ForeignKey('flights.id'))
    timestamp = Column(DateTime, default=datetime.now)
    changes_json = Column(String) # JSON containing dict of changes: {'field': {'old': val, 'new': val}, ...}
    description = Column(String) # Human readable summary

class Flight(Base):
    __tablename__ = 'flights'

    id = Column(Integer, primary_key=True)
    flight_number = Column(String, index=True) # e.g. "FL123"
    date = Column(DateTime) # Date of the flight
    
    # Times (Local -Default)
    scheduled_departure = Column(DateTime)
    scheduled_arrival = Column(DateTime)
    actual_departure = Column(DateTime, nullable=True)
    actual_arrival = Column(DateTime, nullable=True)
    
    # Times (UTC)
    scheduled_departure_utc = Column(DateTime, nullable=True)
    scheduled_arrival_utc = Column(DateTime, nullable=True)
    actual_departure_utc = Column(DateTime, nullable=True)
    actual_arrival_utc = Column(DateTime, nullable=True)
    
    # New Fields
    sta_raw = Column(String) # Raw STA string e.g. "0042 : 16DEC25"
    tail_number = Column(String, nullable=True)
    departure_airport = Column(String, nullable=True)
    arrival_airport = Column(String, nullable=True)
    aircraft_type = Column(String, nullable=True)
    version = Column(String, nullable=True)
    status = Column(String, nullable=True)
    
    # Storage for large text blocks
    pax_data = Column(String, nullable=True)
    load_data = Column(String, nullable=True)
    notes_data = Column(String, nullable=True)
    
    # Relationships
    crew_members = relationship("CrewMember", secondary=flight_crew_association, back_populates="flights")

    def __repr__(self):
        return f"<Flight(flight_number='{self.flight_number}', date='{self.date}', tail='{self.tail_number}')>"

class CrewMember(Base):
    __tablename__ = 'crew'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    employee_id = Column(String, unique=True, nullable=True)
    
    # Relationships
    flights = relationship("Flight", secondary=flight_crew_association, back_populates="crew_members")

    def __repr__(self):
        return f"<CrewMember(name='{self.name}')>"

class ScheduledFlight(Base):
    __tablename__ = 'scheduled_flights'
    id = Column(Integer, primary_key=True)
    pairing_number = Column(String, index=True) # e.g. "I0001"
    flight_number = Column(String)
    date = Column(DateTime) # Date of this specific flight leg
    departure_airport = Column(String)
    arrival_airport = Column(String)
    scheduled_departure = Column(String) # "HH:MM"
    scheduled_arrival = Column(String, nullable=True) # "HH:MM"
    block_time = Column(String, nullable=True) # "H:MM" or "HH:MM"
    total_credit = Column(String, nullable=True) # "HH:MM"
    pairing_start_date = Column(DateTime, nullable=True)
    is_deadhead = Column(Integer, default=0) # 1 if DH, 0 if flown
    
class IOEAssignment(Base):
    __tablename__ = 'ioe_assignments'
    id = Column(Integer, primary_key=True)
    employee_id = Column(String, index=True)
    pairing_number = Column(String)
    start_date = Column(DateTime)

class LCP(Base):
    __tablename__ = 'lcp'
    id = Column(Integer, primary_key=True)
    employee_id = Column(String, unique=True, index=True)
    name = Column(String, nullable=True) # Optional, captured if available in import

class DailySyncStatus(Base):
    __tablename__ = 'daily_sync_status'

    date = Column(DateTime, primary_key=True) # The date being scraped (midnight)
    last_scraped_at = Column(DateTime) # When the scrape happened
    flights_found = Column(Integer, default=0)
    status = Column(String) # 'Success', 'Failed', 'In Progress'

    def __repr__(self):
        return f"<DailySyncStatus(date='{self.date}', status='{self.status}')>"

class AppMetadata(Base):
    __tablename__ = 'app_metadata'
    key = Column(String, primary_key=True)
    value = Column(String)

def set_metadata(session, key, value):
    rec = session.query(AppMetadata).get(key)
    if not rec:
        rec = AppMetadata(key=key)
        session.add(rec)
    rec.value = str(value)
    session.commit()

def get_metadata(session, key, default=None):
    rec = session.query(AppMetadata).get(key)
    return rec.value if rec else default

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(engine)
    return engine

def get_session():
    return SessionLocal()
