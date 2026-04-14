import sqlite3
import pandas as pd

db_path = 'noc_data.db'
conn = sqlite3.connect(db_path)

# Query for duplicates of flight 4823 on 2026-03-31
query = """
SELECT id, flight_number, date, departure_airport, arrival_airport, tail_number
FROM flights 
WHERE (flight_number LIKE '%4823%' OR flight_number = '4823') 
  AND date LIKE '2026-03-31%'
"""
df = pd.read_sql_query(query, conn)
print(df)

# Also check for ALL duplicates on that date
print("\n--- ALL DUPLICATES on 2026-03-31 ---")
dup_query = """
SELECT flight_number, date, departure_airport, arrival_airport, COUNT(*) as count
FROM flights
WHERE date LIKE '2026-03-31%'
GROUP BY flight_number, date, departure_airport, arrival_airport
HAVING count > 1
"""
df_dups = pd.read_sql_query(dup_query, conn)
print(df_dups)

conn.close()
