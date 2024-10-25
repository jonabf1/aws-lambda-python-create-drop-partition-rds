import subprocess
import time
import unittest
from dotenv import load_dotenv
import os

from src.handlers.lambda_handler import lambda_handler

# Carregar as variáveis do arquivo .env
load_dotenv()

# Carregar variáveis de ambiente
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')
database_name = os.getenv('database_name')
enable_drop = os.getenv('enable_drop') == 'true'
enable_create = os.getenv('enable_create') == 'true'
application_name = os.getenv('application_name')
table_name = os.getenv('table_name')
drop_month_back = int(os.getenv('drop_month_back'))
future_partition_months = int(os.getenv('future_partition_months'))
maxvalue_partition_name = os.getenv('maxvalue_partition_name')


def run_docker_compose_up():
    """Sobe o ambiente Docker."""
    try:
        print("Subindo o ambiente Docker...")
        subprocess.run(["docker-compose", "up", "--build", "-d"], check=True)
        time.sleep(15)  # Aguarda alguns segundos para garantir que o MySQL esteja totalmente disponível
    except subprocess.CalledProcessError as e:
        print("Erro ao subir o Docker:", e)
        run_docker_compose_down()  # Se der erro, derruba o Docker
        raise e


def run_docker_compose_down():
    """Derruba o ambiente Docker."""
    print("Derrubando o ambiente Docker...")
    subprocess.run(["docker-compose", "down"], check=True)


class TestPartitionOperations(unittest.TestCase):

    def setUp(self):
        run_docker_compose_up()  # Sobe o Docker

    def tearDown(self):
        run_docker_compose_down()  # Derruba o Docker

    def test_create_month_partition(self):
        lambda_handler("event", "context")

    def test_drop_month_partition(self):
        lambda_handler("event", "context")



if __name__ == "__main__":
    unittest.main()
