from src.database_connection import DatabaseConnection
from src.partition_operations import PartitionOperations
from src.enviroment_util import EnviromentUtil
from src.metric_publisher import MetricPublisher
from src.partition_manager import PartitionManager

class DependencyInjector:
    def __init__(self, current_date):
        self.env_util = EnviromentUtil()
        db_connection = DatabaseConnection(
            self.env_util.get_enviroment("host"),
            self.env_util.get_enviroment("user"),
            self.env_util.get_enviroment("password"),
            self.env_util.get_enviroment("db_name")
        ).get_connection()
        self.db_client = PartitionOperations(db_connection)
        self.metric_publisher = MetricPublisher(self.env_util.get_enviroment("service_name"))
        self.current_date = current_date

    def create_partition_manager(self) -> PartitionManager:
        return PartitionManager(self.db_client, self.metric_publisher, self.env_util, self.current_date)
