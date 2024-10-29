from src.adapters.metric_publisher import MetricPublisher
from src.adapters.partition_manager import PartitionManager


class PartitionService():
    def __init__(self, current_date, metric_publisher: MetricPublisher):
        self.partition_manager = PartitionManager(metric_publisher)
        self.current_date = current_date

    def drop_month_partition(self):
        self.partition_manager.drop_partition_by_month(self.current_date)

    def create_month_partition(self):
        self.partition_manager.create_partitions_for_future_months(self.current_date)
