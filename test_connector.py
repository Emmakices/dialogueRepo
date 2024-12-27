import unittest
from unittest.mock import MagicMock, patch
from connector import replicate_data, fetch_page, store_data

class TestConnector(unittest.TestCase):

    @patch('connector.requests.get')
    @patch('connector.duckdb.connect')
    def test_replicate_patients_parallel(self, mock_connect, mock_get):
        """
        Test full replication with parallel fetching and batch insertion.
        """
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": i, "first_name": "Emmanuel", "last_name": "Ihetu", "email": f"user{i}@example.com"} for i in range(100)],
            "meta": {"total_items": 100}
        }
        mock_get.return_value = mock_response

        # Mock DuckDB connection
        mock_conn = MagicMock()
        mock_conn.register = MagicMock()
        mock_conn.execute = MagicMock()
        mock_connect.return_value = mock_conn

        # Call replicate_data
        replicate_data()

        # Assertions
        self.assertTrue(mock_get.called, "API was not called.")
        self.assertTrue(mock_conn.register.called, "DataFrame was not registered to DuckDB.")
        self.assertTrue(mock_conn.execute.called, "SQL execution was not called in DuckDB.")

        # Check SQL executed for upsert
        executed_queries = [call[0][0].strip() for call in mock_conn.execute.call_args_list]
        expected_query = """
            INSERT INTO patients
            SELECT * FROM patients_temp
            ON CONFLICT (id) DO UPDATE
            SET first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                updated_at = excluded.updated_at,
                total_visits = excluded.total_visits,
                timestamp = excluded.timestamp
        """.strip()

        # Check if the expected query exists in the list of executed queries
        self.assertIn(expected_query, executed_queries, f"Expected SQL query not found in executed queries: {executed_queries}")

        # Check calls to register and execute
        print(f"register() calls: {mock_conn.register.call_args_list}")
        print(f"execute() calls: {mock_conn.execute.call_args_list}")

    @patch('connector.requests.get')
    def test_fetch_page(self, mock_get):
        """
        Test the fetch_page function for handling API responses.
        """
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": 1, "first_name": "Test", "last_name": "User", "email": "testuser@example.com"}],
            "meta": {"total_items": 1}
        }
        mock_get.return_value = mock_response

        # Call fetch_page
        result = fetch_page(0)

        # Assertions
        self.assertIsNotNone(result, "fetch_page returned None.")
        self.assertIn("data", result, "'data' key missing in response.")
        self.assertEqual(len(result["data"]), 1, "Incorrect number of records fetched.")
        self.assertEqual(result["data"][0]["first_name"], "Test", "Incorrect data fetched.")

    @patch('connector.duckdb.connect')
    def test_store_data(self, mock_connect):
        """
        Test the store_data function to ensure data is inserted into DuckDB.
        """
        # Mock DuckDB connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Mock data
        batch_data = [
            {
                "id": 1,
                "first_name": "Test",
                "last_name": "User",
                "email": "testuser@example.com",
                "date_of_birth": "2000-01-01",
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-02T00:00:00",
                "total_visits": 5,
            }
        ]

        # Call store_data
        store_data(batch_data, mock_conn, initialized=False)

        # Assertions
        self.assertTrue(mock_conn.register.called, "DataFrame was not registered to DuckDB.")
        self.assertTrue(mock_conn.execute.called, "SQL execution was not called.")
        mock_conn.register.assert_called_once_with("patients_temp", unittest.mock.ANY)

if __name__ == "__main__":
    unittest.main()
