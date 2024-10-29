from src.configs.metric_name import MetricName
from src.exceptions.exceptions import (
    PartitionErrorDropException,
    PartitionMaxValueErrorException,
    PartitionErrorCreateException
)
from src.adapters.metric_publisher import MetricPublisher
from src.services.partition_service import PartitionService
from datetime import datetime
from dateutil import tz

from src.configs.config_env_factory import ServiceConfigFactory, DatabaseConfigFactory


def lambda_handler(event, context):
    service_env_config = ServiceConfigFactory.create()
    metric_publisher = MetricPublisher(service_env_config.application_name)
    partition_service = PartitionService(
        datetime.now(tz.gettz('America/Sao_Paulo')),
        metric_publisher
    )

    if service_env_config.enable_drop:
        try:
            partition_service.drop_month_partition()
        except PartitionErrorDropException as e:
            metric_publisher.publish(MetricName.PartitionDropError, 1)
            raise e

    if service_env_config.enable_create:
        try:
            partition_service.create_month_partition()
        except PartitionErrorCreateException as e:
            metric_publisher.publish(MetricName.PartitionCreateError, 1)
            raise e
        except PartitionMaxValueErrorException as e:
            metric_publisher.publish(MetricName.PartitionMaxValueError, 1)
            raise e
