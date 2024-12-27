import requests
import duckdb
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Base API URL
API_URL = "http://localhost:8000/v1/patients"

# DuckDB Database Path
DB_PATH = "data/destination.duckdb"

# API Pagination Limit
LIMIT = 100  


def fetch_page(offset):
    """
    Fetch a single page of data from the API.
    """
    try:
        response = requests.get(
            API_URL,
            params={
                "offset": offset,
                "limit": LIMIT,
                "sort_field": "last_name",
                "sort_dir": "asc",
            }
        )
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data for offset={offset}", exc_info=True)
        return None


def reset_table(conn):
    """
    Drop and recreate the `patients` table to ensure schema alignment.
    """
    try:
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
        print("Patients table reset successfully.")
        print(conn.execute("DESCRIBE patients").fetchdf())
    except Exception as e:
        logging.error("Error resetting table schema", exc_info=True)


def store_data(batch_data, conn, initialized):
    """
    Store data in the DuckDB database in batches.
    """
    if not batch_data:
        logging.error("No data to store.")
        return

    try:
        # Convert to DataFrame
        df = pd.DataFrame(batch_data)
        df['timestamp'] = datetime.now()

        # Debugging: Print batch data
        print(f"store_data called. Batch DataFrame:\n{df.head()}")

        # Reset table if not initialized
        if not initialized:
            reset_table(conn)

        # Register the DataFrame
        conn.register("patients_temp", df)

        # Perform upsert (Update existing records and insert new ones)
        sql = """
            INSERT INTO patients
            SELECT * FROM patients_temp
            ON CONFLICT (id) DO UPDATE
            SET first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                updated_at = excluded.updated_at,
                total_visits = excluded.total_visits,
                timestamp = excluded.timestamp
        """
        print(f"Executing SQL: {sql}")
        conn.execute(sql)
        print("Upsert completed successfully.")

    except Exception as e:
        logging.error("Error storing batch data", exc_info=True)


def replicate_data():
    """
    Fetch and store patient data with parallel processing and batch inserts.
    """
    initialized = False
    conn = duckdb.connect(DB_PATH)

    # Debugging
    print(f"replicate_data called with connection: {conn}")

    # Fetch the first page to get total records
    first_page = fetch_page(0)
    if not first_page or "data" not in first_page:
        print("No data available to fetch.")
        return

    total_records = first_page.get("meta", {}).get("total_items", 0)
    print(f"Total records to fetch: {total_records}")

    # Fetch pages in parallel
    offsets = [i for i in range(0, total_records, LIMIT)]
    all_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch_page, offsets)

    # Collect all fetched data
    for page_data in results:
        if page_data and "data" in page_data:
            all_data.extend(page_data["data"])

    # Insert data in batches
    batch_size = 5000
    for i in range(0, len(all_data), batch_size):
        batch = all_data[i:i + batch_size]
        print(f"Inserting batch: {batch[:5]}")  # Debugging
        store_data(batch, conn, initialized)
        initialized = True  # Reset table only during the first batch

    print("Data replication completed successfully.")


if __name__ == "__main__":
    print("Starting data replication...")
    replicate_data()