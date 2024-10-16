
from abc import ABC, abstractmethod
from datetime import datetime


class IDatabaseClient(ABC):
    @abstractmethod
    def drop_partition_by_month(self, table_name: str, partition_name: str):
        """Dropar a partição de um determinado mês."""
        pass

    @abstractmethod
    def create_partition_for_month(self, table_name: str, partition_name: str, reference_date: datetime, current_date: datetime):
        """Criar uma nova partição para o mês anterior."""
        pass


class IMetricPublisher(ABC):
    @abstractmethod
    def publish(self, metric_name: str, value: int):
        """Publicar uma métrica personalizada."""
        pass

class IEnviromentUtil(ABC):
    @abstractmethod
    def get_enviroment(self, env_name: str):
        """Recupera uma variavel ambiente"""
        pass