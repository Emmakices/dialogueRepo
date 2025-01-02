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
    Fetch a single page of patient data from the API.

    Parameters:
        offset (int): The starting offset for pagination.

    Returns:
        dict: JSON response from the API containing the data or None if the request fails.
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


def store_data(batch_data, conn):
    """
    Insert or update patient data in the DuckDB database in batches.

    Parameters:
        batch_data (list[dict]): A list of dictionaries containing patient data.
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.

    Returns:
        None
    """
    if not batch_data:
        logging.error("No data to store.")
        return

    try:
        # Convert to DataFrame
        df = pd.DataFrame(batch_data)
        df['timestamp'] = datetime.now()

        print(f"store_data called. Batch DataFrame:\n{df.head()}")

        # Register the DataFrame
        conn.register("patients_temp", df)

        # Insert into patients table
        sql_patients = """
            INSERT INTO patients
            SELECT id, first_name, last_name, email, date_of_birth,
                   created_at, updated_at, total_visits, timestamp
            FROM patients_temp
            ON CONFLICT (id) DO UPDATE
            SET first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                updated_at = excluded.updated_at,
                total_visits = excluded.total_visits,
                timestamp = excluded.timestamp;
        """
        print(f"Executing SQL for patients: {sql_patients}")
        conn.execute(sql_patients)
        print("Upsert completed for patients table.")

        # Insert into history table
        sql_history = """
            INSERT INTO history
            SELECT id, first_name, last_name, email, date_of_birth,
                   created_at, updated_at, total_visits, timestamp, CURRENT_TIMESTAMP
            FROM patients_temp;
        """
        print(f"Executing SQL for history: {sql_history}")
        conn.execute(sql_history)
        print("Insert completed for history table.")

    except Exception as e:
        logging.error("Error storing batch data", exc_info=True)
        print(f"Error: {e}")


def replicate_data():
    """
    Fetch and store patient data using parallel processing and batch inserts.

    This function orchestrates the fetching of data from the API and storing it into
    the DuckDB database.

    Returns:
        None
    """
    conn = duckdb.connect(DB_PATH)

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
        store_data(batch, conn)

    print("Data replication completed successfully.")


if __name__ == "__main__":
    print("Starting one-time data replication...")
    replicate_data()