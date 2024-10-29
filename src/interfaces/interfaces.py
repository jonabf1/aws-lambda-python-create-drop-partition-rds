from abc import ABC, abstractmethod
from datetime import datetime


class IPartitionManager(ABC):
    @abstractmethod
    def drop_partition_by_month(self, current_date: datetime):
        pass

    @abstractmethod
    def _check_partition_exist(self, new_partition_name: str):
        pass

    @abstractmethod
    def create_partitions_for_future_months(self, current_date: datetime):
        pass

    @abstractmethod
    def _add_maxvalue_partition(self):
        pass

    @abstractmethod
    def _drop_maxvalue_partition(self):
        pass

class IPartitionQueryExecutor(ABC):
    @abstractmethod
    def execute(self, query: str, exception_class, error_message: str):
        pass

    @abstractmethod
    def partition_exist(self, query: str):
        pass

    @abstractmethod
    def execute_count(self, query: str, exception_class) -> int:
        pass

class IMetricPublisher(ABC):
    @abstractmethod
    def publish(self, metric_name: str, value: int):
        pass

class IEnvironmentUtil(ABC):
    @abstractmethod
    def get_environment(self, env_name: str) -> str:
        pass
