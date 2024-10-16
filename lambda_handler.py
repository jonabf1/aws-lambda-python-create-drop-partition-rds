
from datetime import datetime

from dateutil import tz

from src.dependency_injector import DependencyInjector


def lambda_handler(event, context):
    sao_paulo_tz = tz.gettz('America/Sao_Paulo')
    current_date = datetime.now(sao_paulo_tz)

    injector = DependencyInjector(current_date)
    partition_manager = injector.create_partition_manager()

    table_name = injector.env_util.get_enviroment("table_name")

    try:
        partition_manager.drop_month_partition(table_name)
    except Exception as e:
        injector.metric_publisher.publish_metric("PartitionDropError", 1)
        raise e

    try:
        partition_manager.create_month_partition(table_name)
    except Exception as e:
        injector.metric_publisher.publish_metric("PartitionCreateError", 1)
        raise e
