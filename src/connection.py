import logging
import mysql.connector
from pymongo import MongoClient

from .config import Config


logger = logging.getLogger(__name__)


class ConnectionFactory:
    """Create database clients using values from Config.

    Connections are created on demand. This factory does not log secret values.
    """

    def __init__(self, config: Config):
        self.config = config
        self._mongo_client = None

    def get_mysql_connection(self):
        mysql_config = self.config.get_mysql_config()
        return mysql.connector.connect(**mysql_config)

    def get_mongo_client(self):
        if self._mongo_client:
            return self._mongo_client
        mongo_uri = self.config.get_mongo_uri()
        # Avoid logging the URI or credentials
        self._mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        return self._mongo_client

    def close(self):
        if self._mongo_client:
            try:
                self._mongo_client.close()
            finally:
                self._mongo_client = None
