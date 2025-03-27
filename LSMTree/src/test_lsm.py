import os
import shutil
import unittest
from time import sleep
from threading import Thread
from lsm import LSMTree


class TestLSMTree(unittest.TestCase):
    def setUp(self):
        self.db_path = "./test_db"

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

    def test_basic_operations(self):
        db = LSMTree(self.db_path)

        # Test single key-value
        db.set("test_key", "test_value")
        self.assertEqual(db.get("test_key"), "test_value")

        # Test overwrite
        db.set("test_key", "new_value")
        self.assertEqual(db.get("test_key"), "new_value")

        # Test non-existent key
        self.assertIsNone(db.get("missing_key"))

    def test_delete_operations(self):
        db = LSMTree(self.db_path)

        # Test delete existing key
        db.set("key1", "value1")
        self.assertEqual(db.get("key1"), "value1")
        db.delete("key1")
        self.assertIsNone(db.get("key1"))

        # Test delete non-existent key
        db.delete("nonexistent_key")  # Should not raise error

        # Test set after delete
        db.set("key1", "new_value")
        self.assertEqual(db.get("key1"), "new_value")

    def test_range_query(self):
        db = LSMTree(self.db_path)

        # Insert test data
        test_data = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
        for k, v in test_data.items():
            db.set(k, v)

        # Test range query
        results = list(db.range_query("b", "d"))
        self.assertEqual(len(results), 3)
        self.assertEqual(results, [("b", 2), ("c", 3), ("d", 4)])

        # Test empty range
        results = list(db.range_query("x", "z"))
        assert len(results) == 0

    def test_concurrent_operations(self):
        db = LSMTree(self.db_path)

        def writer_thread():
            for i in range(100):
                db.set(f"thread_key_{i}", f"thread_value_{i}")
                sleep(0.001)  # Small delay to increase chance of concurrency issues

        # Create multiple writer threads
        threads = [Thread(target=writer_thread) for _ in range(5)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all data was written correctly
        for i in range(100):
            self.assertEqual(db.get(f"thread_key_{i}"), f"thread_value_{i}")

    def test_recovery(self):
        """Test database recovery with optimized operations"""
        # Create a database instance
        db1 = LSMTree(self.db_path)

        # Add some data to the database
        db1.set("key1", "value1")
        db1.set("key2", "value2")
        self.assertEqual(db1.get("key1"), "value1")
        self.assertEqual(db1.get("key2"), "value2")

        # Close the database to force a flush
        db1.close()

        # Create a new instance of the database
        db2 = LSMTree(self.db_path)

        # Verify data is still present after recovery
        value1 = db2.get("key1")
        value2 = db2.get("key2")

        self.assertEqual(value1, "value1")
        self.assertEqual(value2, "value2")

        db2.close()


if __name__ == "__main__":
    unittest.main()
