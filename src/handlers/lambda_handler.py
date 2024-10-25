from src.adapters.partition_manager import PartitionManager
from src.configs.metric_name import MetricName
from src.utils.enviroment_util import EnvironmentUtil
from src.exceptions.exceptions import (
    PartitionErrorDropException,
    PartitionMaxValueErrorException,
    PartitionErrorCreateException
)
from src.adapters.metric_publisher import MetricPublisher
from src.services.partition_service import PartitionService
from src.adapters.database_connection import DatabaseConnection
from datetime import datetime
from dateutil import tz

from src.configs.config_env_factory import ServiceConfigFactory, DatabaseConfigFactory


def lambda_handler(event, context):
    service_config = ServiceConfigFactory.create()
    db_config = DatabaseConfigFactory.create()

    metric_publisher = MetricPublisher(service_config.application_name)
    db_connection = DatabaseConnection(db_config).get_connection()
    partition_manager = PartitionManager(db_connection, metric_publisher)

    partition_service = PartitionService(
        partition_manager,
        EnvironmentUtil(),
        datetime.now(tz.gettz('America/Sao_Paulo'))
    )

    if service_config.enable_drop:
        try:
            partition_service.drop_month_partition()
        except PartitionErrorDropException as e:
            metric_publisher.publish(MetricName.PartitionDropError, 1)
            raise e

    if service_config.enable_create:
        try:
            partition_service.create_month_partition()
        except PartitionErrorCreateException as e:
            metric_publisher.publish(MetricName.PartitionCreateError, 1)
            raise e
        except PartitionMaxValueErrorException as e:
            metric_publisher.publish(MetricName.PartitionMaxValueError, 1)
            raise e
