from src.exceptions.exceptions import PartitionErrorCreateException
from src.interfaces.interfaces import IPartitionQueryExecutor


class PartitionQueryExecutor(IPartitionQueryExecutor):
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query: str, exception_class, error_message: str):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                print(f"Successfully executed: {query}")
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise exception_class(f"{error_message}: {e}")

    def partition_exist(self, query: str):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone() is not None
        except Exception as e:
            raise PartitionErrorCreateException(f"Error checking if partition exists: {e}")

    def execute_count(self, query: str, exception_class) -> int:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result['count']
        except Exception as e:
            raise exception_class(f"Error counting records: {e}")
