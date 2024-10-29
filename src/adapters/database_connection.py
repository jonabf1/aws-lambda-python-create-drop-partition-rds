import pymysql

from src.configs.enviroment_config import DatabaseEnvConfig

class Database:
    def __init__(self, config: DatabaseEnvConfig):
        self.host = config.host
        self.user = config.user
        self.password = config.password
        self.database_name = config.database_name

    def get_connection(self):
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database_name,
            cursorclass=pymysql.cursors.DictCursor
        )
