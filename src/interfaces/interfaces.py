from abc import ABC, abstractmethod
from datetime import datetime
from typing import List


class IPartitionManager(ABC):
    @abstractmethod
    def drop_partition_by_month(self, table_name: str, months_back: int, current_date: datetime):
        pass

    @abstractmethod
    def _get_existing_partitions(self, table_name: str) -> List[str]:
        pass

    @abstractmethod
    def create_partitions_for_future_months(self, table_name: str, months_in_future: int, current_date: datetime, maxvalue_partition_name: str):
        pass

    @abstractmethod
    def _add_maxvalue_partition(self, table_name: str, max_partition_name: str):
        pass

    @abstractmethod
    def _drop_maxvalue_partition(self, table_name: str, max_partition_name: str):
        pass

class IPartitionQueryExecutor(ABC):
    @abstractmethod
    def execute(self, query: str, exception_class, error_message: str):
        pass

    @abstractmethod
    def fetch_partition_names(self, query: str) -> List[str]:
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
