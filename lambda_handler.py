from src.dependency_provider import DependencyProvider


def lambda_handler(event, context):
    factory = DependencyProvider()
    partition_manager = factory.create_partition_manager()
    metric_publisher = factory.create_metric_publisher()

    try:
        partition_manager.drop_month_partition()
    except Exception as e:
        metric_publisher.publish("PartitionDropError", 1)
        raise e

    try:
        partition_manager.create_month_partition()
    except Exception as e:
        metric_publisher.publish("PartitionCreateError", 1)
        raise e
