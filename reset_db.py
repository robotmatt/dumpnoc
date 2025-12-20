import os
from database import init_db, Base
from config import DB_NAME
from sqlalchemy import create_engine
from config import DB_URL

# Force reset
engine = init_db()
print(f"Connecting to {DB_URL}...")

# Drop all tables to reset schema
Base.metadata.drop_all(engine)
print("Dropped all tables.")

# Recreate all tables
Base.metadata.create_all(engine)
print("Database initialized with new schema.")
