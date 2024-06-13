import unittest
import os
import sqlite3
import pandas as pd


class TestPipeline(unittest.TestCase):
    def setUp(self):
        # Setting up the data directory to execute the test cases
        self.data_dir = "data"


    def test_directory_creation(self):
        # Test if the data directory is created
        self.assertTrue(os.path.exists(self.data_dir))


    def test_database_storage(self):
        # Test if the SQLite databases are created
        self.database1 = os.path.join(self.data_dir, "database1.db")
        self.database2 = os.path.join(self.data_dir, "database2.db")
        self.database3 = os.path.join(self.data_dir, "database3.db")
        
        # Verify that the tables exist and contain data
        def check_table_exists(db_path, table_name):
            conn = sqlite3.connect(db_path)
            query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            result = conn.execute(query).fetchone()
            conn.close()
            return result is not None
        

        self.assertTrue(check_table_exists(self.database1, "dataset1"))
        self.assertTrue(check_table_exists(self.database2, "dataset2"))
        self.assertTrue(check_table_exists(self.database3, "dataset3"))

if __name__ == "__main__":
    unittest.main()
