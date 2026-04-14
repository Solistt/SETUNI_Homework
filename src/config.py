import os
from dotenv import load_dotenv


load_dotenv()


class ConfigError(Exception):
    pass


class Config:
    """Centralized configuration loader that validates required environment variables.

    This class intentionally avoids printing or logging secret values.
    """

    # Core variables required by all services (API, ETL, etc.)
    CORE_REQUIRED_VARS = [
        'MYSQL_USER',
        'MYSQL_PASSWORD',
        'MYSQL_HOST',
        'MYSQL_PORT',
        'MYSQL_DATABASE',
        'MONGO_USER',
        'MONGO_PASSWORD',
        'MONGO_HOST',
        'MONGO_PORT',
        'MONGO_DB',
        'REDIS_HOST',
        'REDIS_PORT',
    ]

    # Variables required only by ETL processes
    ETL_REQUIRED_VARS = [
        'DATA_USERS_PATH',
        'DATA_CAMPAIGNS_PATH',
        'DATA_EVENTS_PATH',
    ]

    def __init__(self, env: dict):
        self._env = env
        self._validate_core()

        # Core DB configuration
        self.mysql_user = env['MYSQL_USER']
        self.mysql_password = env['MYSQL_PASSWORD']
        self.mysql_host = env['MYSQL_HOST']
        self.mysql_port = int(env.get('MYSQL_PORT') or 3306)
        self.mysql_database = env['MYSQL_DATABASE']

        self.mongo_user = env['MONGO_USER']
        self.mongo_password = env['MONGO_PASSWORD']
        self.mongo_host = env['MONGO_HOST']
        self.mongo_port = int(env.get('MONGO_PORT') or 27017)
        self.mongo_database = env['MONGO_DB']

        self.redis_host = env['REDIS_HOST']
        self.redis_port = int(env.get('REDIS_PORT') or 6379)

        # Tunables with safe defaults (coalesce None/empty to defaults)
        self.session_timeout_seconds = int(env.get('SESSION_TIMEOUT_SECONDS') or 1800)
        self.user_batch_size = int(env.get('USER_BATCH_SIZE') or 500)
        self.bulk_write_batch_size = int(env.get('BULK_WRITE_BATCH_SIZE') or 1000)

        # Optional: allow user to provide a full MONGO_URI directly
        self._mongo_uri_override = env.get('MONGO_URI')

        # Data file paths are now optional at init
        self.data_users_path = env.get('DATA_USERS_PATH')
        self.data_campaigns_path = env.get('DATA_CAMPAIGNS_PATH')
        self.data_events_path = env.get('DATA_EVENTS_PATH')

    @classmethod
    def load_from_env(cls):
        # Load all possible variables
        all_vars = cls.CORE_REQUIRED_VARS + cls.ETL_REQUIRED_VARS
        env = {k: os.getenv(k) for k in all_vars}
        # include optional overrides and tunables
        env.update({
            'MONGO_URI': os.getenv('MONGO_URI'),
            'SESSION_TIMEOUT_SECONDS': os.getenv('SESSION_TIMEOUT_SECONDS'),
            'USER_BATCH_SIZE': os.getenv('USER_BATCH_SIZE'),
            'BULK_WRITE_BATCH_SIZE': os.getenv('BULK_WRITE_BATCH_SIZE'),
        })
        return cls(env)

    def _validate_core(self):
        missing = [k for k in self.CORE_REQUIRED_VARS if not self._env.get(k)]
        if missing:
            # Fail fast but do not reveal secret values
            raise ConfigError(f"Missing required core environment variables: {', '.join(missing)}. Please add them to the root .env file.")

    def get_mysql_config(self):
        return {
            'user': self.mysql_user,
            'password': self.mysql_password,
            'host': self.mysql_host,
            'port': self.mysql_port,
            'database': self.mysql_database,
            'autocommit': True,
        }

    def get_mongo_uri(self):
        if self._mongo_uri_override:
            return self._mongo_uri_override
        return f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/?authSource=admin"

    def get_data_paths(self) -> dict:
        # Validate ETL-specific variables only when they are requested
        missing = [k for k in self.ETL_REQUIRED_VARS if not self._env.get(k)]
        if missing:
            raise ConfigError(f"Missing ETL-specific data path variables: {', '.join(missing)}. Please add them to the root .env file.")

        return {
            'users': self.data_users_path,
            'campaigns': self.data_campaigns_path,
            'events': self.data_events_path,
        }

    # convenience property for the mongo database name
    @property
    def mongo_db(self):
        return self.mongo_database
