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
    date = Column(DateTime, index=True) # Date of the flight
    
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
    
    actual_out_utc = Column(DateTime, nullable=True)
    actual_off_utc = Column(DateTime, nullable=True)
    actual_on_utc = Column(DateTime, nullable=True)
    actual_in_utc = Column(DateTime, nullable=True)
    
    # OOOI and Block Times
    actual_out = Column(DateTime, nullable=True)
    actual_off = Column(DateTime, nullable=True)
    actual_on = Column(DateTime, nullable=True)
    actual_in = Column(DateTime, nullable=True)
    planned_block_minutes = Column(Integer, nullable=True)
    actual_block_minutes = Column(Integer, nullable=True)
    has_duplicate_warning = Column(Integer, default=0)

    
    # New Fields
    sta_raw = Column(String) # Raw STA string e.g. "0042 : 16DEC25"
    tail_number = Column(String, nullable=True, index=True)
    departure_airport = Column(String, nullable=True, index=True)
    arrival_airport = Column(String, nullable=True, index=True)
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
    name = Column(String, index=True)
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

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Table, inspect, text, event

engine = create_engine(DB_URL, connect_args={'timeout': 15})

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(engine)
    
    # Auto-migration: Check for missing columns in 'flights' table
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns('flights')]
    
    # Define columns that might be missing in older versions
    # Map column name to its SQLAlchemy type string for ALTER TABLE
    required_columns = {
        'scheduled_departure_utc': 'DATETIME',
        'scheduled_arrival_utc': 'DATETIME',
        'actual_departure_utc': 'DATETIME',
        'actual_arrival_utc': 'DATETIME',
        'actual_out_utc': 'DATETIME',
        'actual_off_utc': 'DATETIME',
        'actual_on_utc': 'DATETIME',
        'actual_in_utc': 'DATETIME',
        'actual_out': 'DATETIME',
        'actual_off': 'DATETIME',
        'actual_on': 'DATETIME',
        'actual_in': 'DATETIME',
        'planned_block_minutes': 'INTEGER',
        'actual_block_minutes': 'INTEGER',
        'has_duplicate_warning': 'INTEGER DEFAULT 0',
        'sta_raw': 'VARCHAR',
        'tail_number': 'VARCHAR',
        'departure_airport': 'VARCHAR',
        'arrival_airport': 'VARCHAR',
        'aircraft_type': 'VARCHAR',
        'version': 'VARCHAR',
        'status': 'VARCHAR',
        'pax_data': 'VARCHAR',
        'load_data': 'VARCHAR',
        'notes_data': 'VARCHAR'
    }
    
    with engine.connect() as conn:
        for col_name, col_type in required_columns.items():
            if col_name not in columns:
                try:
                    conn.execute(text(f"ALTER TABLE flights ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                    print(f"Migration: Added missing column '{col_name}' to 'flights' table.")
                except Exception as e:
                    print(f"Migration Error on '{col_name}': {e}")
                    
        # Migration: Ensure 'crew.name' is not unique (it might have been in older versions)
        try:
            res = conn.execute(text("PRAGMA index_list('crew')"))
            for row in res:
                # row[1] is name, row[2] is unique
                if row[1] == 'ix_crew_name' and row[2] == 1:
                    print("Migration: Non-uniquifying crew.name index...")
                    # SQLAlchemy might have created it as unique index
                    conn.execute(text("DROP INDEX ix_crew_name"))
                    conn.execute(text("CREATE INDEX ix_crew_name ON crew(name)"))
                    conn.commit()
                    print("Migration: crew.name is no longer unique.")
        except Exception as e:
            print(f"Migration Error on crew name constraint: {e}")
            
        # Add performance indexes
        try:
            # We wrap in try to avoid errors if they already exist
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_flights_date ON flights(date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_flights_dep ON flights(departure_airport)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_flights_arr ON flights(arrival_airport)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_flights_tail ON flights(tail_number)"))
            conn.commit()
        except Exception as e:
            pass
            
    return engine

def get_session():
    return SessionLocal()
