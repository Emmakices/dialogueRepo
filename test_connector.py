import unittest
from unittest.mock import MagicMock, patch
from connector import replicate_data, fetch_page, store_data
import pandas as pd
from datetime import datetime


class TestConnector(unittest.TestCase):

    @patch('connector.requests.get')
    @patch('connector.duckdb.connect')
    def test_replicate_data(self, mock_connect, mock_get):
        """
        Test full data replication process with mocked API and database.
        """
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "id": i,
                "first_name": "First",
                "last_name": "Last",
                "email": f"user{i}@example.com",
                "date_of_birth": "2000-01-01",
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-02T00:00:00",
                "total_visits": i
            } for i in range(100)],
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
        self.assertTrue(mock_conn.register.called, "DataFrame was not registered in DuckDB.")
        self.assertTrue(mock_conn.execute.called, "SQL execution was not called in DuckDB.")

    @patch('connector.requests.get')
    def test_fetch_page(self, mock_get):
        """
        Test the fetch_page function for API response handling.
        """
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "id": 1,
                "first_name": "Test",
                "last_name": "User",
                "email": "testuser@example.com",
                "date_of_birth": "2000-01-01",
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-02T00:00:00",
                "total_visits": 1
            }],
            "meta": {"total_items": 1}
        }
        mock_get.return_value = mock_response

        # Call fetch_page
        result = fetch_page(0)

        # Assertions
        self.assertIsNotNone(result, "fetch_page returned None.")
        self.assertIn("data", result, "'data' key missing in response.")
        self.assertEqual(len(result["data"]), 1, "Incorrect number of records fetched.")


if __name__ == "__main__":
    unittest.main()