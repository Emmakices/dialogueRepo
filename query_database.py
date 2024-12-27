import duckdb

# Connect to the DuckDB database
conn = duckdb.connect("data/replicated_patients.duckdb")

# Query the patients table
result = conn.execute("SELECT * FROM patients").fetchdf()

# Print the result
print(result)