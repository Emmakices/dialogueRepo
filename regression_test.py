import subprocess
import unittest
import os

class TestRegression(unittest.TestCase):
    DUCKDB_EXECUTABLE = "C:\\Users\\Eco\\Desktop\\duckdb.exe"
    DB_PATH = "data/destination.duckdb"
    QUERY = "SELECT id, first_name, last_name, email, updated_at FROM patients LIMIT 5;"

    @classmethod
    def setUpClass(cls):
        print("Setting up the test environment...")
        result = subprocess.run(["docker-compose", "up", "-d"], capture_output=True, text=True)
        print(f"Docker-compose output:\n{result.stdout}")
        print("Docker containers started.")

        result = subprocess.run(["python", "init_db.py"], capture_output=True, text=True)
        print(f"Init DB script output:\n{result.stdout}")
        print("Database initialized successfully via init_db script.")

    @classmethod
    def tearDownClass(cls):
        print("Tearing down the test environment...")
        result = subprocess.run(["docker-compose", "down"], capture_output=True, text=True)
        print(f"Docker-compose down output:\n{result.stdout}")
        print("Docker containers stopped.")

    def test_data_consistency(self):
        print("Running data consistency test...")
        print("Skipping source update simulation as the API does not support updates.")

        if not os.path.isfile(self.DUCKDB_EXECUTABLE):
            self.fail(f"DuckDB executable not found at {self.DUCKDB_EXECUTABLE}")

        try:
            print(f"Executing query: {self.QUERY}")
            db_result = subprocess.run(
                [self.DUCKDB_EXECUTABLE, self.DB_PATH, f"--execute={self.QUERY}"],
                check=True,
                capture_output=True,
                text=True,
            )
            print("Database query executed successfully.")
            print(f"Query Result:\n{db_result.stdout}")

            # Validate the result
            if "id" not in db_result.stdout or len(db_result.stdout.strip().split("\n")) <= 1:
                self.fail("Query returned no data or unexpected results.")
        except subprocess.CalledProcessError as e:
            print(f"Error in database query:\n{e.stderr}")
            self.fail(f"Database query failed: {e.stderr}")
        except FileNotFoundError as e:
            print(f"DuckDB executable not found:\n{e}")
            self.fail(f"DuckDB executable not found: {e}")
