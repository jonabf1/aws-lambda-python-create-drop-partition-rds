import os

from src.interfaces.interfaces import IEnvironmentUtil


class EnvironmentUtil(IEnvironmentUtil):

    def get_environment(self, env_name: str):
        try:
            env = os.getenv(env_name)
            if env is None:
                raise OSError(f"Environment variable '{env_name}' is missing")
        except Exception as e:
            raise e
        return env
