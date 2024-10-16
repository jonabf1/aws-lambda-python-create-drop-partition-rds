from src.interface.interfaces import IEnviromentUtil

class ServiceEnvConfig:
    def __init__(self, env_util: IEnviromentUtil):
        self.application_name = env_util.get_enviroment('application_name')

class DatabaseEnvConfig:
    def __init__(self, env_util: IEnviromentUtil):
        self.host = env_util.get_enviroment('host')
        self.user = env_util.get_enviroment('user')
        self.password = env_util.get_enviroment('password')
        self.database_name = env_util.get_enviroment('database_name')

class TableEnvConfig:
    def __init__(self, env_util: IEnviromentUtil):
        self.table_name = env_util.get_enviroment('table_name')

class PartitionEnvConfig:
    def __init__(self, env_util: IEnviromentUtil):
        self.drop_months_back = env_util.get_enviroment('DROP_MONTHS_BACK')
        self.create_months_back = env_util.get_enviroment('CREATE_MONTHS_BACK')
