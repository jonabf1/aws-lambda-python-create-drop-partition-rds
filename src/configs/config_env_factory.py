from src.configs.enviroment_config import ServiceEnvConfig, DatabaseEnvConfig, PartitionEnvConfig
from src.utils.enviroment_util import EnvironmentUtil

class ServiceConfigFactory:
    @staticmethod
    def create() -> ServiceEnvConfig:
        env_util = EnvironmentUtil()
        return ServiceEnvConfig(env_util)

class DatabaseConfigFactory:
    @staticmethod
    def create() -> DatabaseEnvConfig:
        env_util = EnvironmentUtil()
        return DatabaseEnvConfig(env_util)

class PartitionConfigFactory:
    @staticmethod
    def create() -> PartitionEnvConfig:
        env_util = EnvironmentUtil()
        return PartitionEnvConfig(env_util)
