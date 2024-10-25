from datetime import datetime

from src.adapters.partition_manager import PartitionManager
from src.configs.enviroment_config import PartitionEnvConfig, TableEnvConfig
from src.utils.enviroment_util import IEnvironmentUtil


class PartitionService():
    def __init__(self, partition_manager: PartitionManager, env_util: IEnvironmentUtil, current_date: datetime):
        self.env_util = env_util
        self.partition_manager = partition_manager
        self.current_date = current_date
        self.partition_env_config = PartitionEnvConfig(self.env_util)
        self.table_env_config = TableEnvConfig(self.env_util)

    def drop_month_partition(self):
        months_back = self.partition_env_config.convert_value_to_int(self.partition_env_config.drop_months_back)
        self.partition_manager.drop_partition_by_month(self.table_env_config.table_name, months_back, self.current_date)

    def create_month_partition(self):
        future_months = self.partition_env_config.convert_value_to_int(self.partition_env_config.future_partition_months)
        self.partition_manager.create_partitions_for_future_months(
            self.table_env_config.table_name,
            future_months,
            self.current_date,
            self.table_env_config.maxvalue_partition_name
        )
