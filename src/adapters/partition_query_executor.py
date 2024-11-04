import logging
from typing import List

from src.exceptions.exceptions import PartitionErrorCreateException
from src.interfaces.interfaces import IPartitionQueryExecutor


class PartitionQueryExecutor(IPartitionQueryExecutor):
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query: str, exception_class, error_message: str):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                logging.info(f"Query executada (criação ou drop de partição) com sucesso: {query}")
        except Exception as e:
            logging.error(f'Erro durante a insercao ou exclusao de particao: {e}')
            self.connection.rollback()
            raise exception_class(f"{error_message}: {e}")

    def fetch_partition_names(self, query: str) -> List[str]:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                logging.info(f"Query de listagem executada com sucesso: {query}")
                return [row['PARTITION_NAME'] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f'Erro ao consultar a lista de particoes existentes: {e}')
            raise PartitionErrorCreateException(f"Error listing partitions: {e}")

    def execute_count(self, query: str, exception_class) -> int:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                logging.info(f"Query de contagem executada com sucesso: {query}")
                return result['count']
        except Exception as e:
            logging.error(f'Erro ao contabilizar a quantidade de itens existentes em uma particao: {e}')
            raise exception_class(f"Error counting records: {e}")
