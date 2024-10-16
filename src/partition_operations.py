from src.interfaces import IDatabaseClient
from dateutil.relativedelta import relativedelta

class PartitionOperations(IDatabaseClient):
    def __init__(self, connection):
        self.connection = connection

    def drop_partition_by_month(self, table_name, partition_name):
        query = f"ALTER TABLE {table_name} DROP PARTITION {partition_name};"
        with self.connection.cursor() as cursor:
            cursor.execute(query)

    def create_partition_for_month(self, table_name, partition_name, reference_date, current_date):
        next_month_first_day = current_date + relativedelta(months=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date_previous_period = next_month_first_day.strftime('%Y-%m-%d')
        query = f"""
        ALTER TABLE {table_name} 
        PARTITION BY RANGE (TO_DAYS('{reference_date}')) (
            PARTITION {partition_name} VALUES LESS THAN (TO_DAYS('{end_date_previous_period}'))
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
