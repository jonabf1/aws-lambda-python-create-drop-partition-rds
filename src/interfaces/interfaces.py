from abc import ABC, abstractmethod
from typing import List


class IPartitionManager(ABC):
    @abstractmethod
    def drop_older_partitions(self):
        pass

    @abstractmethod
    def _get_existing_partitions(self) -> List[str]:
        pass

    @abstractmethod
    def create_partitions_for_future_months(self):
        pass

    @abstractmethod
    def _add_maxvalue_partition(self):
        pass

    @abstractmethod
    def _drop_maxvalue_partition(self):
        pass

    @abstractmethod
    def _get_previous_months(self, month_list) -> List[str]:
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
