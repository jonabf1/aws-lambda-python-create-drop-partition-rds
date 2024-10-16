import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.tz import tz
from src.config.database_connection import DatabaseConnection
from src.adapter.partition_operations import PartitionOperations
from src.enviroment.enviroment_util import EnvironmentUtil
from src.adapter.publisher.metric.metric_publisher import MetricPublisher
from src.adapter.partition_manager import PartitionManager

class TestSuite(unittest.TestCase):

    @patch('pymysql.connect')
    def test_database_connection(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        db_conn = DatabaseConnection('localhost', 'user', 'password', 'dbname')
        connection = db_conn.get_connection()

        mock_connect.assert_called_once_with(
            host='localhost',
            user='user',
            password='password',
            db='dbname'
        )
        self.assertEqual(connection, mock_conn)

    @patch('pymysql.connect')
    def setUp(self, mock_connect):
        self.mock_conn = mock_connect.return_value
        self.mock_cursor = self.mock_conn.cursor.return_value
        self.partition_ops = PartitionOperations(self.mock_conn)

        self.db_client = MagicMock()
        self.metric_publisher = MagicMock()
        self.env_util = MagicMock()
        self.current_date = datetime(2023, 12, 15, tzinfo=tz.gettz('America/Sao_Paulo'))
        self.partition_manager = PartitionManager(self.db_client, self.metric_publisher, self.env_util, self.current_date)

    def test_partition_operations_drop_partition(self):
        self.partition_ops.drop_partition_by_month('test_table', 'p202310')
        self.mock_cursor.execute.assert_called_with("ALTER TABLE test_table DROP PARTITION p202310;")

    def test_partition_operations_create_partition(self):
        reference_date = datetime(2023, 10, 1)
        current_date = datetime(2023, 11, 1)
        next_month_first_day = current_date + relativedelta(months=1, day=1)
        end_date_previous_period = next_month_first_day.strftime('%Y-%m-%d')
        query = f"""
        ALTER TABLE test_table 
        PARTITION BY RANGE (TO_DAYS('{reference_date}')) (
            PARTITION p202311 VALUES LESS THAN (TO_DAYS('{end_date_previous_period}'))
        );
        """
        self.partition_ops.create_partition_for_month('test_table', 'p202311', reference_date, current_date)
        self.mock_cursor.execute.assert_called_with(query)

    @patch('os.getenv')
    def test_get_enviroment(self, mock_getenv):
        mock_getenv.return_value = 'test_value'
        env_util = EnvironmentUtil()
        result = env_util.get_enviroment('test_var')
        self.assertEqual(result, 'test_value')

    @patch('os.getenv')
    def test_get_enviroment_missing(self, mock_getenv):
        mock_getenv.return_value = None
        env_util = EnvironmentUtil()
        with self.assertRaises(OSError):
            env_util.get_enviroment('test_var')

    @patch('boto3.client')
    def test_publish_metric(self, mock_boto_client):
        metric_publisher = MetricPublisher("applicationname-namespace")
        mock_cloudwatch = mock_boto_client.return_value
        metric_publisher.publish("TestMetric", 1)
        mock_cloudwatch.put_metric_data.assert_called_with(
            Namespace="applicationname-namespace",
            MetricData=[
                {
                    'MetricName': "TestMetric",
                    'Timestamp': unittest.mock.ANY,
                    'Value': 1,
                    'Unit': 'Count'
                },
            ]
        )

    def test_partition_manager_drop_month_partition(self):
        self.env_util.get_enviroment.return_value = '2'
        self.partition_manager.drop_month_partition('test_table')
        self.db_client.drop_partition_by_month.assert_called_with('test_table', 'p202310')

    def test_partition_manager_create_month_partition(self):
        self.env_util.get_enviroment.return_value = '1'
        self.partition_manager.create_month_partition('test_table')
        self.db_client.create_partition_for_month.assert_called_with(
            'test_table', 'p202311', self.current_date - relativedelta(months=1), self.current_date
        )

if __name__ == '__main__':
    unittest.main()
