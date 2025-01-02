import duckdb

# Path to your DuckDB database file
db_path = "C:/Users/Eco/Desktop/dialogue/Dialogue/data-replication-1-tocktq/data/destination.duckdb"

# Connect to the database
conn = duckdb.connect(db_path)

# Query the data
print("Listing first 10 rows from the 'patients' table:")
result = conn.execute("SELECT * FROM patients LIMIT 10").fetchdf()
print(result)

# Count total rows
total_rows = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
print(f"Total rows in the patients table: {total_rows}")