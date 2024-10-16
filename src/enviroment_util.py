import os
from src.interfaces import IEnviromentUtil

class EnviromentUtil(IEnviromentUtil):
    def get_enviroment(self, env_name: str):
        env = os.getenv(env_name)
        if env is None:
            raise OSError(f"Environment variable '{env_name}' is missing")
        return env
