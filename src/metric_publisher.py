import boto3
from datetime import datetime
from src.interfaces import IMetricPublisher

class MetricPublisher(IMetricPublisher):
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.cloudwatch = boto3.client('cloudwatch', region_name='sa-east-1')

    def publish(self, metric_name: str, value: int):
        self.cloudwatch.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Timestamp': datetime.utcnow(),
                    'Value': value,
                    'Unit': 'Count'
                },
            ]
        )
