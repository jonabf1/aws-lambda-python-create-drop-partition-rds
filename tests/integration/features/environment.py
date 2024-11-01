import os
import subprocess
import time

def _wait_for_mysql_healthcheck(container_name="mysql_container", timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run(
            ["docker", "inspect", "--format='{{json .State.Health.Status}}'", container_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        health_status = result.stdout.strip().replace("'", "")

        if health_status == '"healthy"':
            return True
        elif health_status == '"unhealthy"':
            print("MySQL está com falha de healthcheck.")
            break

        time.sleep(1)

    raise TimeoutError(f"O healthcheck do MySQL não ficou 'healthy' no tempo esperado ({timeout}s).")


def _is_container_removed(container_name="my_mysql_container", timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if container_name not in result.stdout:
            return True

        time.sleep(1)

    raise TimeoutError(f"O container '{container_name}' ainda está em execução após {timeout} segundos.")

def before_scenario(context, scenario):
    path_to_compose = os.path.abspath(os.path.join(os.path.dirname(__file__), "../docker-compose.yml"))
    subprocess.run(['docker-compose', '-f', path_to_compose, 'up', '--build', '-d'], check=True)
    _wait_for_mysql_healthcheck()
    context.start_time = time.time()

def after_scenario(context, scenario):
    print(f"Derrubando o Docker após o cenário: {scenario.name}")
    path_to_compose = os.path.abspath(os.path.join(os.path.dirname(__file__), "../docker-compose.yml"))
    subprocess.run(["docker-compose", "-f", path_to_compose, "down"], check=True)
    _is_container_removed()
    duration = time.time() - context.start_time
    print(f"Duração do cenário '{scenario.name}': {duration:.2f} segundos")
