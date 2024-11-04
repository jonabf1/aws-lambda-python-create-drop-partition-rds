from src.interfaces.interfaces import IEnvironmentUtil


class ServiceEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.application_name = env_util.get_environment('application_name')
        self.enable_drop = self.str_to_bool(env_util.get_environment('enable_drop'))
        self.enable_create = self.str_to_bool(env_util.get_environment('enable_create'))

    @staticmethod
    def str_to_bool(value: str) -> bool:
        return value.strip().lower() in ["true", "1"]

class DatabaseEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.host = env_util.get_environment('database_host')
        self.user = env_util.get_environment('database_user')
        self.password = env_util.get_environment('database_password')
        self.database_name = env_util.get_environment('database_name')
        self.table_name = env_util.get_environment('table_name')

class PartitionEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.months_to_keep = self.convert_value_to_int(env_util.get_environment('months_to_keep'))
        self.future_partition_months = self.convert_value_to_int(env_util.get_environment('future_partition_months'))
        self.maxvalue_partition_name = env_util.get_environment('maxvalue_partition_name')

    @staticmethod
    def convert_value_to_int(value: str) -> int:
        try:
            value_to_int = int(value)
            if value_to_int == 0:
                raise ValueError("Value cannot be zero")
            return value_to_int
        except ValueError:
            raise ValueError(f"Invalid value for {value}")
