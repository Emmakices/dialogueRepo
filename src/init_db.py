import duckdb
import os

# Connect to the DuckDB database (this will create the file if it doesn't exist)
with duckdb.connect(os.environ["DESTINATION_DUCKDB_PATH"]) as conn:

    # Replace me:
    conn.sql("""
        CREATE TABLE IF NOT EXISTS example (
            id INTEGER,
            name VARCHAR,
            value FLOAT
        );
    """)

    print("Successfully connected to the destination DB.")
    print("SHOW ALL TABLES")
    conn.sql("SHOW ALL TABLES").show();