import pymysql
import subprocess
import time

# Configurações de conexão com o banco de dados local MySQL
db_host = 'localhost'
db_user = 'testuser'
db_password = 'testpassword'
db_name = 'database_test_1'

def run_docker_compose():
    """Inicia o ambiente Docker com MySQL."""
    subprocess.run(['docker-compose', 'up', '-d'])
    time.sleep(10)  # Aguardar o banco de dados inicializar

def stop_docker_compose():
    """Derruba o ambiente Docker com MySQL."""
    subprocess.run(['docker-compose', 'down'])
    time.sleep(10)  # Aguardar o banco de dados parar

def get_connection():
    """Conecta ao banco de dados MySQL local."""
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=3306,  # Porta padrão do MySQL
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

def run_partition_test():
    """Teste de integração para verificar as partições."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # Testar criação de partição para o mês anterior
            cursor.execute("ALTER TABLE `table_test_1` ADD PARTITION (PARTITION p202312 VALUES LESS THAN ('2024-01-01'));")
            connection.commit()
            print("Partição de dezembro criada com sucesso.")

            # Testar remoção de partição para um mês anterior
            cursor.execute("ALTER TABLE `table_test_1` DROP PARTITION p202307;")
            connection.commit()
            print("Partição de julho removida com sucesso.")
    finally:
        connection.close()

if __name__ == "__main__":
    run_docker_compose()  # Inicializar o ambiente Docker
    try:
        run_partition_test()  # Executar o teste de partições
    except Exception as e:
        stop_docker_compose()
        print(f"Erro: {str(e)}")
