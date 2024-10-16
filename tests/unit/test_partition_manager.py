
import unittest
from unittest.mock import MagicMock, patch
from src.partition_manager import PartitionManager
from datetime import datetime
import os

class TestPartitionManager(unittest.TestCase):

    def setUp(self):
        self.db_client = MagicMock()
        self.metric_publisher = MagicMock()
        self.current_date = datetime(2024, 10, 1)
        self.partition_manager = PartitionManager(self.db_client, self.metric_publisher, self.current_date)

    @patch.dict('os.environ', {'DROP_MONTHS_BACK': '4'})
    def test_drop_partition_valid(self):
        self.partition_manager.drop_old_partition('test_table')
        self.db_client.drop_partition_by_month.assert_called_with('test_table', 'p202406')

    @patch.dict('os.environ', {'DROP_MONTHS_BACK': 'invalid'})
    def test_drop_partition_invalid_value(self):
        with self.assertRaises(ValueError):
            self.partition_manager.drop_old_partition('test_table')

    @patch.dict('os.environ', {})
    def test_drop_partition_missing_env(self):
        with self.assertRaises(OSError):
            self.partition_manager.drop_old_partition('test_table')

if __name__ == '__main__':
    unittest.main()
