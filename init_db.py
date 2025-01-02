import duckdb
import logging

# Configure logging
logging.basicConfig(filename='init_db.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# DuckDB Database Path
DB_PATH = "data/destination.duckdb"

def initialize_database():
    """
    Initialize the database schema for the 'patients' and 'history' tables.
    """
    try:
        conn = duckdb.connect(DB_PATH)

        # Drop and recreate the patients table
        conn.execute("DROP TABLE IF EXISTS patients")
        conn.execute("""
            CREATE TABLE patients (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                date_of_birth VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR,
                total_visits INTEGER,
                timestamp TIMESTAMP
            )
        """)
        print("Patients table initialized successfully.")

        # Drop and recreate the history table
        conn.execute("DROP TABLE IF EXISTS history")
        conn.execute("""
            CREATE TABLE history (
                id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                date_of_birth VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR,
                total_visits INTEGER,
                timestamp TIMESTAMP,
                change_timestamp TIMESTAMP
            )
        """)
        print("History table initialized successfully.")

        conn.close()
    except Exception as e:
        logging.error("Error initializing the database schema", exc_info=True)
        print("Failed to initialize the database. Check init_db.log for details.")

if __name__ == "__main__":
    initialize_database()