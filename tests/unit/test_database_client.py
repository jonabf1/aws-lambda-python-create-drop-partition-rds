import unittest
from unittest.mock import MagicMock
from src.config.database_connection import DatabaseClient

class TestDatabaseClient(unittest.TestCase):

    def setUp(self):
        # Simulando a conexão e o cursor com suporte ao context manager
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()

        # Configurando o __enter__ e __exit__ para simular o uso de context manager
        self.mock_connection.cursor.return_value.__enter__.return_value = self.mock_cursor
        self.mock_connection.cursor.return_value.__exit__.return_value = None

        # Injetando a conexão mock no DatabaseClient
        self.db_client = DatabaseClient(connection=self.mock_connection)

    def test_drop_partition_by_month(self):
        self.db_client.drop_partition_by_month('test_table', 'p202406')
        query = "ALTER TABLE test_table DROP PARTITION p202406;"
        self.mock_cursor.execute.assert_called_with(query)

    def test_create_partition_for_month(self):
        self.db_client.create_partition_for_month('test_table', 'p202406')
        query = '''
        ALTER TABLE test_table ADD PARTITION (
            PARTITION p202406 VALUES LESS THAN (TO_DAYS('p202406-01'))
        );
        '''
        self.mock_cursor.execute.assert_called_with(query)

if __name__ == '__main__':
    unittest.main()
