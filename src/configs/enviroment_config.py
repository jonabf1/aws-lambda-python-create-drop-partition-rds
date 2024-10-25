from src.interfaces.interfaces import IEnvironmentUtil


class ServiceEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.application_name = env_util.get_environment('application_name')
        self.enable_drop = env_util.get_environment('enable_drop')
        self.enable_create = env_util.get_environment('enable_create')

class DatabaseEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.host = env_util.get_environment('host')
        self.user = env_util.get_environment('user')
        self.password = env_util.get_environment('password')
        self.database_name = env_util.get_environment('database_name')

class TableEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.table_name = env_util.get_environment('table_name')
        self.maxvalue_partition_name = env_util.get_environment('maxvalue_partition_name')

class PartitionEnvConfig:
    def __init__(self, env_util: IEnvironmentUtil):
        self.drop_months_back = env_util.get_environment('drop_month_back')
        self.future_partition_months = env_util.get_environment('future_partition_months')

    @staticmethod
    def convert_value_to_int(value: str):
        try:
            value_to_int = int(value)
            if value_to_int == 0:
                raise ValueError("Value cannot be zero")
            return value_to_int
        except ValueError:
            raise ValueError(f"Invalid value for {value}")

