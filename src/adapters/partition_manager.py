from datetime import datetime

from dateutil.relativedelta import relativedelta

from src.adapters.database_connection import Database
from src.adapters.partition_query_executor import PartitionQueryExecutor
from src.configs.config_env_factory import DatabaseConfigFactory, PartitionConfigFactory
from src.configs.metric_name import MetricName
from src.exceptions.exceptions import (
    PartitionErrorDropException,
    PartitionErrorCreateException,
    PartitionMaxValueErrorException
)
from src.interfaces.interfaces import IPartitionManager, IMetricPublisher
from src.utils.partition_name_generator import PartitionNameGenerator


class PartitionManager(IPartitionManager):
    def __init__(self, metric_publisher: IMetricPublisher):
        self.metric_publisher = metric_publisher
        self.database_env_config = DatabaseConfigFactory.create()
        self.database_connection = Database(self.database_env_config).get_connection()
        self.query_executor = PartitionQueryExecutor(self.database_connection)
        self.partition_env_config = PartitionConfigFactory.create()

    def drop_partition_by_month(self, current_date):
        drop_date = current_date - relativedelta(months=self.partition_env_config.drop_months_back)
        partition_name = PartitionNameGenerator.generate_partition_name(drop_date)

        query = f"ALTER TABLE {self.database_env_config.database_name}.{self.database_env_config.table_name} DROP PARTITION {partition_name};"
        self.query_executor.execute(query, PartitionErrorDropException, f"Error dropping partition: {partition_name}")

    def create_partitions_for_future_months(self, current_date: datetime):
        if self._is_maxvalue_partition_populated():
            raise PartitionMaxValueErrorException(f"Maxvalue partition '{self.partition_env_config.maxvalue_partition_name}' contains data.")

        self._drop_maxvalue_partition()

        for month in range(1, self.partition_env_config.future_partition_months + 1):
            partition_date = current_date + relativedelta(months=month)
            partition_name = PartitionNameGenerator.generate_partition_name(partition_date)

            if self._check_partition_exist(partition_name):
                print(f"Partition '{partition_name}' exists. Skipping creation.")
                continue

            end_date = (partition_date + relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0,
                                                                          microsecond=0)
            query = f"""
                ALTER TABLE {self.database_env_config.database_name}.{self.database_env_config.table_name}
                ADD PARTITION (
                    PARTITION {partition_name} 
                    VALUES LESS THAN ('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
                );
            """
            self.query_executor.execute(query, PartitionErrorCreateException, f"Error creating partition: {partition_name}")

        self._add_maxvalue_partition()

    def _check_partition_exist(self, new_partition_name: str):
        query = f"""
            SELECT PARTITION_NAME
            FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = '{self.database_env_config.database_name}' AND TABLE_NAME = '{self.database_env_config.table_name}' AND PARTITION_NAME = '{new_partition_name}';
        """
        return self.query_executor.partition_exist(query)

    def _add_maxvalue_partition(self):
        query = f"""
            ALTER TABLE {self.database_env_config.database_name}.{self.database_env_config.table_name}
            ADD PARTITION (
                PARTITION {self.partition_env_config.maxvalue_partition_name}
                VALUES LESS THAN (MAXVALUE)
            );
        """
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error recreating maxvalue partition '{self.partition_env_config.maxvalue_partition_name}'")

    def _drop_maxvalue_partition(self):
        query = f"ALTER TABLE {self.database_env_config.database_name}.{self.database_env_config.table_name} DROP PARTITION {self.partition_env_config.maxvalue_partition_name};"
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error removing maxvalue partition '{self.partition_env_config.maxvalue_partition_name}'")

    def _is_maxvalue_partition_populated(self) -> bool:
        query = f"""SELECT COUNT(*) AS count FROM {self.database_env_config.database_name}.{self.database_env_config.table_name} PARTITION ({self.partition_env_config.maxvalue_partition_name})"""
        count = self.query_executor.execute_count(query, PartitionMaxValueErrorException)

        if count > 0:
            self.metric_publisher.publish(MetricName.PartitionMaxValueItemCount, count)
            return True
        return False
