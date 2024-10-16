
import unittest
from unittest.mock import patch, MagicMock
from src.adapter.publisher.metric.metric_publisher import MetricPublisher

class TestMetricPublisher(unittest.TestCase):

    def setUp(self):
        self.metric_publisher = MetricPublisher('Custom/PartitionManagement')
        self.metric_publisher.cloudwatch = MagicMock()

    def test_publish_metric(self):
        self.metric_publisher.publish('TestMetric', 1)
        self.metric_publisher.cloudwatch.put_metric_data.assert_called_once_with(
            Namespace='Custom/PartitionManagement',
            MetricData=[
                {
                    'MetricName': 'TestMetric',
                    'Timestamp': unittest.mock.ANY,
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
