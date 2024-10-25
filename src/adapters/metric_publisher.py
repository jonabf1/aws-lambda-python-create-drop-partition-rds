import boto3
from datetime import datetime
from dateutil import tz

from src.interfaces.interfaces import IMetricPublisher


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
                    'Timestamp': datetime.now(tz.gettz('America/Sao_Paulo')),
                    'Value': value,
                    'Unit': 'Count'
                },
            ]
        )
