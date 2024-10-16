
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.interfaces import IDatabaseClient, IMetricPublisher, IEnviromentUtil


class PartitionManager:
    def __init__(self, db_client: IDatabaseClient, metric_publisher: IMetricPublisher, env_util: IEnviromentUtil, current_date: datetime):
        self.enviroment_util = env_util
        self.db_client = db_client
        self.metric_publisher = metric_publisher
        self.current_date = current_date
        self.drop_month_env_name = "DROP_MONTH_BACK"
        self.create_partition_month_env_name = "CREATE_MONTH_BACK"

    def drop_month_partition(self, table_name: str):
        months_back = self.try_convert_env_to_int(self.drop_month_env_name)
        drop_date = self.current_date - relativedelta(months=months_back)
        partition_name = f"p{drop_date.year}{drop_date.month:02d}"
        self.db_client.drop_partition_by_month(table_name, partition_name)

    def create_month_partition(self, table_name: str):
        months_back = self.try_convert_env_to_int(self.create_partition_month_env_name)
        reference_date  = self.current_date - relativedelta(months=months_back)
        partition_name = f"p{reference_date.year}{reference_date.month:02d}"
        self.db_client.create_partition_for_month(table_name, partition_name, reference_date, self.current_date)

    def try_convert_env_to_int(self, env_name: str):
        value = self.enviroment_util.get_enviroment(env_name)
        try:
            value_to_int = int(value)
            return value_to_int
        except ValueError:
            raise ValueError(f"Invalid value for {env_name}: {value}")
