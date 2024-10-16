from datetime import datetime
from dateutil import tz
from src.config.database_connection import DatabaseConnection
from src.config.enviroment_config import DatabaseEnvConfig, ServiceEnvConfig
from src.enviroment.enviroment_util import EnvironmentUtil
from src.adapter.partition_operations import PartitionOperations
from src.adapter.publisher.metric.metric_publisher import MetricPublisher
from src.adapter.partition_manager import PartitionManager

class DependencyProvider:
    def __init__(self):
        sao_paulo_tz = tz.gettz('America/Sao_Paulo')

        self.current_date = datetime.now(sao_paulo_tz)
        self.env_util = EnvironmentUtil()
        self.db_env_config = DatabaseEnvConfig(self.env_util)
        self.service_env_config = ServiceEnvConfig(self.env_util)
        self.db_connection = DatabaseConnection(self.db_env_config).get_connection()
        self.db_client = PartitionOperations(self.db_connection)

    def create_partition_manager(self) -> PartitionManager:
        return PartitionManager(
            self.db_client,
            self.env_util,
            self.current_date
        )

    def create_metric_publisher(self) -> MetricPublisher:
        return MetricPublisher(self.service_env_config.application_name)
