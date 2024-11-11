import logging
from datetime import datetime
from typing import List

from dateutil.relativedelta import relativedelta
from dateutil.tz import tz

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
        self.current_date = datetime.now(tz.gettz('America/Sao_Paulo'))

    def drop_older_partitions(self):
        existing_partitions = self._get_existing_partitions()
        previous_existing_months = self._get_previous_months(existing_partitions)

        partitions_to_drop = []
        partitions_to_keep = []

        for i in range(1, self.partition_env_config.months_to_keep + 1):
            partition_date = self.current_date - relativedelta(months=i)
            partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
            partitions_to_keep.append(partition_name)

        for partition_name in previous_existing_months:
            if partition_name not in partitions_to_keep:
                partitions_to_drop.append(partition_name)

        for partition_name in partitions_to_drop:
            query = f"ALTER TABLE `{self.database_env_config.database_name}`.`{self.database_env_config.table_name}` DROP PARTITION {partition_name};"
            logging.info(f"Dropando particao '{partition_name}'")
            self.query_executor.execute(query, PartitionErrorDropException,
                                            f"Error dropping partition: {partition_name}")

    def create_partitions_for_future_months(self):
        if self._is_maxvalue_partition_populated():
            raise PartitionMaxValueErrorException(f"Maxvalue partition '{self.partition_env_config.maxvalue_partition_name}' contains data.")

        self._drop_maxvalue_partition()

        existing_partitions = self._get_existing_partitions()

        for month in range(1, self.partition_env_config.future_partition_months + 1):
            partition_date = self.current_date + relativedelta(months=month)
            partition_name = PartitionNameGenerator.generate_partition_name(partition_date)

            if partition_name in existing_partitions:
                print(f"Partition '{partition_name}' exists. Skipping creation.")
                continue

            end_date = (partition_date + relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0,
                                                                          microsecond=0)
            query = f"""
                ALTER TABLE `{self.database_env_config.database_name}`.`{self.database_env_config.table_name}`
                ADD PARTITION (
                    PARTITION {partition_name} 
                    VALUES LESS THAN ('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
                );
            """

            logging.info(f"Criando particao '{partition_name}' com o range de referencia menor que '{end_date}")
            self.query_executor.execute(query, PartitionErrorCreateException, f"Error creating partition: {partition_name}")

        self._add_maxvalue_partition()

    def _get_existing_partitions(self) -> List[str]:
        query = f"""
            SELECT PARTITION_NAME
            FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = '{self.database_env_config.database_name}' AND TABLE_NAME = '{self.database_env_config.table_name}' AND PARTITION_NAME IS NOT NULL;
        """
        logging.info("Buscando lista de particoes existentes")
        return self.query_executor.fetch_partition_names(query)

    def _add_maxvalue_partition(self):
        query = f"""
            ALTER TABLE `{self.database_env_config.database_name}`.`{self.database_env_config.table_name}`
            ADD PARTITION (
                PARTITION {self.partition_env_config.maxvalue_partition_name}
                VALUES LESS THAN (MAXVALUE)
            );
        """

        logging.info(f"Criando particao MaxValue '{self.partition_env_config.maxvalue_partition_name}'")
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error recreating maxvalue partition '{self.partition_env_config.maxvalue_partition_name}'")

    def _drop_maxvalue_partition(self):
        query = f"ALTER TABLE `{self.database_env_config.database_name}`.`{self.database_env_config.table_name}` DROP PARTITION `{self.partition_env_config.maxvalue_partition_name}`;"

        logging.info(f"Dropando particao MaxValue '{self.partition_env_config.maxvalue_partition_name}'")
        self.query_executor.execute(query, PartitionMaxValueErrorException,
                                f"Error removing maxvalue partition '{self.partition_env_config.maxvalue_partition_name}'")

    def _is_maxvalue_partition_populated(self) -> bool:
        query = f"""SELECT COUNT(*) AS count FROM `{self.database_env_config.database_name}`.`{self.database_env_config.table_name}` PARTITION ({self.partition_env_config.maxvalue_partition_name})"""
        count = self.query_executor.execute_count(query, PartitionMaxValueErrorException)

        if count > 0:
            self.metric_publisher.publish(MetricName.PartitionMaxValueItemCount, count)
            logging.error(f"Particao MaxValue '{self.partition_env_config.maxvalue_partition_name}' possue itens!")
            return True
        return False

    def _get_previous_months(self, month_list) -> List[str]:
        previous_months = []

        for item in month_list:
            # Ignora a particao atual e a MaxValue
            if item == self.partition_env_config.maxvalue_partition_name or item ==  PartitionNameGenerator.generate_partition_name(self.current_date):
                continue

            item_date = datetime.strptime(item, '%b%Y').replace(tzinfo=tz.gettz('America/Sao_Paulo'))
            if item_date < self.current_date:
                previous_months.append(item)

        return previous_months