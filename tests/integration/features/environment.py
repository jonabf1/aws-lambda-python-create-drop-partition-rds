import os
import subprocess
import time


def _is_container_removed(container_name="mysql_container", timeout=60):
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
    context.start_time = time.time()

def after_scenario(context, scenario):
    print(f"Derrubando o Docker após o cenário: {scenario.name}")
    path_to_compose = os.path.abspath(os.path.join(os.path.dirname(__file__), "../docker-compose.yml"))
    subprocess.run(["docker-compose", "-f", path_to_compose, "down", "-v"], check=True)
    _is_container_removed()
    duration = time.time() - context.start_time
    print(f"Duração do cenário '{scenario.name}': {duration:.2f} segundos")
