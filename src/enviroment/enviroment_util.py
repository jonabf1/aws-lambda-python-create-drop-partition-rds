import os
from src.interface.interfaces import IEnviromentUtil

class EnvironmentUtil(IEnviromentUtil):

    def get_enviroment(self, env_name: str):
        try:
            env = os.getenv(env_name)
        except Exception as e:
            raise e

        if env is None:
            raise OSError(f"Environment variable '{env_name}' is missing")
        return env
