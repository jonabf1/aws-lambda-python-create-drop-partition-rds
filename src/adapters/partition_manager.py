from datetime import datetime

from dateutil.relativedelta import relativedelta

from src.adapters.database_connection import DatabaseConnection
from src.adapters.partition_query_executor import PartitionQueryExecutor
from src.configs.metric_name import MetricName
from src.exceptions.exceptions import (
    PartitionErrorDropException,
    PartitionErrorCreateException,
    PartitionMaxValueErrorException
)
from typing import List
from src.interfaces.interfaces import IPartitionManager, IMetricPublisher, IPartitionQueryExecutor
from src.utils.partition_name_generator import PartitionNameGenerator


class PartitionManager(IPartitionManager):
    def __init__(self, db_connection: DatabaseConnection, metric_publisher: IMetricPublisher):
        self.query_executor = PartitionQueryExecutor(db_connection)
        self.metric_publisher = metric_publisher

    def drop_partition_by_month(self, table_name: str, months_back: int, current_date):
        drop_date = current_date - relativedelta(months=months_back)
        partition_name = PartitionNameGenerator.generate_partition_name(drop_date)
        query = f"ALTER TABLE {table_name} DROP PARTITION {partition_name};"
        self.query_executor.execute(query, PartitionErrorDropException, f"Error dropping partition: {partition_name}")

    def create_partitions_for_future_months(self, table_name: str, months_in_future: int, current_date: datetime, maxvalue_partition_name: str):
        if self._is_maxvalue_partition_populated(table_name, maxvalue_partition_name):
            raise PartitionMaxValueErrorException(f"Maxvalue partition '{maxvalue_partition_name}' contains data.")

        self._drop_maxvalue_partition(table_name, maxvalue_partition_name)

        existing_partitions = self._get_existing_partitions(table_name)

        for month in range(1, months_in_future + 1):
            partition_date = current_date + relativedelta(months=month)
            partition_name = PartitionNameGenerator.generate_partition_name(partition_date)

            if partition_name in existing_partitions:
                print(f"Partition '{partition_name}' exists. Skipping creation.")
                continue

            end_date = (partition_date + relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0,
                                                                          microsecond=0)
            query = f"""
                ALTER TABLE {table_name} 
                ADD PARTITION (
                    PARTITION {partition_name} 
                    VALUES LESS THAN ('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
                );
            """
            self.query_executor.execute(query, PartitionErrorCreateException, f"Error creating partition: {partition_name}")

            self._add_maxvalue_partition(table_name, maxvalue_partition_name)

    def _get_existing_partitions(self, table_name: str) -> List[str]:
        query = f"""
            SELECT PARTITION_NAME
            FROM information_schema.PARTITIONS
            WHERE TABLE_NAME = '{table_name}' AND PARTITION_NAME IS NOT NULL;
        """
        return self.query_executor.fetch_partition_names(query)

    def _add_maxvalue_partition(self, table_name: str, max_partition_name: str):
        query = f"ALTER TABLE {table_name} ADD PARTITION (PARTITION {max_partition_name} VALUES LESS THAN (MAXVALUE));"
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error recreating maxvalue partition '{max_partition_name}'")

    def _drop_maxvalue_partition(self, table_name: str, max_partition_name: str):
        query = f"ALTER TABLE {table_name} DROP PARTITION {max_partition_name};"
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error removing maxvalue partition '{max_partition_name}'")

    def _is_maxvalue_partition_populated(self, table_name: str, max_partition_name: str) -> bool:
        query = f"SELECT COUNT(*) AS count FROM {table_name} PARTITION ({max_partition_name})"
        count = self.query_executor.execute_count(query, PartitionMaxValueErrorException)

        if count > 0:
            self.metric_publisher.publish(MetricName.PartitionMaxValueItemCount, count)
            return True
        return False
