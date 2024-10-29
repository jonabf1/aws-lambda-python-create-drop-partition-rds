import subprocess
import time

def before_scenario(context, scenario):
    print(f"Subindo o Docker para o cenário: {scenario.name}")
    subprocess.run(['docker-compose', 'up', '-d'], check=True)
    time.sleep(13)  # Espera para garantir que o banco esteja disponível

def after_scenario(context, scenario):
    print(f"Derrubando o Docker após o cenário: {scenario.name}")
    subprocess.run(["docker-compose", "down"], check=True)
    time.sleep(8)  # Espera para garantir que o banco esteja disponível
