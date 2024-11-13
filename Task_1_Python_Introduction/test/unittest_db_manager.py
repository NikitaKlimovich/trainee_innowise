import unittest
from unittest.mock import patch

from src.db_manager import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    @patch("mysql.connector.connect")
    def test_connection(self, mock_connect):
        config = {
            "host": "localhost",
            "user": "root",
            "password": "Nekito2001",
            "sql_folder": "sql//",
        }
        DatabaseManager(config)
        mock_connect.assert_called_with(
            host=config["host"],
            user=config["user"],
            password=config["password"],
        )

    def test_read_sql(self):
        config = {
            "host": "localhost",
            "user": "root",
            "password": "Nekito2001",
            "sql_folder": "sql//",
        }
        db_manager = DatabaseManager(config)
        sql_queries = db_manager.read_sql("create.sql")
        self.assertIsInstance(sql_queries, list)

    @patch("mysql.connector.connect")
    def test_execute_query(self, mock_connect):
        config = {
            "host": "localhost",
            "user": "root",
            "password": "Nekito2001",
            "sql_folder": "sql//",
        }
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value

        db_manager = DatabaseManager(config)
        db_manager.execute_query("SELECT * FROM student")

        mock_cursor.execute.assert_called_with("SELECT * FROM student")
        mock_connection.commit.assert_called_once()

    @patch("mysql.connector.connect")
    def test_close_connection(self, mock_connect):
        config = {
            "host": "localhost",
            "user": "root",
            "password": "Nekito2001",
            "sql_folder": "sql//",
        }
        mock_connection = mock_connect.return_value

        db_manager = DatabaseManager(config)
        db_manager.close_conn()

        mock_connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
