from src.adapters.metric_publisher import MetricPublisher
from src.adapters.partition_manager import PartitionManager


class PartitionService():
    def __init__(self, metric_publisher: MetricPublisher):
        self.partition_manager = PartitionManager(metric_publisher)

    def drop_partition(self):
        self.partition_manager.drop_older_partitions()

    def create_partition(self):
        self.partition_manager.create_partitions_for_future_months()
