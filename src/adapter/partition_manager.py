
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.config.enviroment_config import PartitionEnvConfig, TableEnvConfig
from src.interface.interfaces import IDatabaseClient, IEnviromentUtil


class PartitionManager:
    def __init__(self, db_client: IDatabaseClient, env_util: IEnviromentUtil, current_date: datetime):
        self.enviroment_util = env_util
        self.db_client = db_client
        self.current_date = current_date
        self.partition_env_config = PartitionEnvConfig(self.enviroment_util)
        self.table_env_config = TableEnvConfig(self.enviroment_util)

    def drop_month_partition(self):
        months_back = self.__try_convert_value_to_int(self.partition_env_config.drop_months_back)
        drop_date = self.current_date - relativedelta(months=months_back)
        partition_name = f"p{drop_date.year}{drop_date.month:02d}"
        self.db_client.drop_partition_by_month(self.table_env_config.table_name, partition_name)

    def create_month_partition(self):
        months_back = self.__try_convert_value_to_int(self.partition_env_config.create_months_back)
        reference_date  = self.current_date - relativedelta(months=months_back)
        partition_name = f"p{reference_date.year}{reference_date.month:02d}"
        self.db_client.create_partition_for_month(self.table_env_config.table_name, partition_name, reference_date, self.current_date)

    @staticmethod
    def __try_convert_value_to_int(value: str):
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Invalid value for {value}")
